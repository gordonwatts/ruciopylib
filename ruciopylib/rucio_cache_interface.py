# Higher level object to help manage a group of datasets on disk.
from src.grid.rucio import RucioFile, rucio, RucioException
from src.utils.logging_mgr import logging_mgr
from src.utils.dataset_cache_mgr import dataset_cache_mgr, dataset_listing_info
from typing import List, Optional, Tuple
import datetime
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, wait
from time import sleep

DatasetQueryStatus = Enum('DatasetQueryStatus', 'does_not_exist, query_queued, results_valid')

def ds_age_too_old(age:datetime.datetime, time_valid:Optional[datetime.timedelta]):
    '''If current time is older than datetime plus timedelta.

    Arguments
    age             When the dataset was created
    time_valid      Delta time that the dataset is valid
                        None - dataset is always valid
                        timedelta of 0 seconds - Always invalid
                        timedelta - how long till the present time it is allowed.

    Returns
    valid           True if still valid
    '''
    if time_valid is None:
        return False
    if time_valid.seconds == 0:
        return True
    return (datetime.datetime.now() + time_valid) <= age

class rucio_cache_interface:
    r'''
    Manages getting rucio data into a local cache of data.
        - Everything is run synchronously.
        - Do not attempt to have two processes download to same place. That will
          cause undefined behavior. That rule must be enforced at a higher level than
          this code.
    '''
    def __init__(self, data_mgr:dataset_cache_mgr, 
            rucio_mgr: Optional[rucio] = None,
            seconds_between_retries:float=60.0*5,
            logger:logging_mgr = None):
        '''
        Setup a dataset_mgr

        Arguments:

            rucio_mgr           Interface to query rucio directly to get back dataset file results.
        '''
        # We want to query rucio one dataset at a time.
        self._rucio = rucio_mgr if rucio_mgr is not None else rucio()
        self._cache_mgr = data_mgr
        self._seconds_between_retries = seconds_between_retries
        self._log = logger if logger is not None else logging_mgr()

    def get_ds_contents(self, ds_name: str, maxAge: Optional[datetime.timedelta] = None, maxAgeIfNotSeen: Optional[datetime.timedelta] = datetime.timedelta(minutes=60)) -> Tuple[DatasetQueryStatus, Optional[List[RucioFile]]]:
        '''
        Return the list of files that are in a dataset if they are in our local cache. If not, then queue a query to
        rucio to get the actual file contents. The results have age checks, which will trigger a re-query.

        If the dataset does not exist on the grid, it will be placed into our local cache. If you try to requery it,
        the not-existing result will be given for up to one hour (by default), and after that a re-query will be triggered.

        Arguments
        name              The rucio fully qualified name of the dataset
        maxAge            If None - any local results are returned, and if no results are present a query is started
                          if a `timedelta` then:
                              If no results present a query is queued.
                              If there are local results, then a query is queued if they are older than timedelta.
        maxAgeIfNotSeen   maxAge takes precedence. If maxAgeIfNotSeen is not null, and there are cached local results:
                              If the results point to a dataset that is valid, then the file list is returned.
                              If last time the cached dataset was queried it wasn't found, and it was queired a more recently
                                  than maxAgeIfNotSeen, then return does not exist.
                              Otherwise re-query rucio to see if the dataset has now appeared.

        Returns
        status        Status of the returned results (see DatasetQueryStatus) and below:
        files         Depends on the status:
                          does_not_exist - files will be None, and the dataset was not found on the last query to rucio.
                          query_queued - files will be None, and a query is pending to update the results
                          results_valid - files will be a list of all files in the dataset. 
                              Empty Dataset: The dataset is empty if the list has len()==0.
                              Dataset with files: The list will have an entry per file
        '''
        # See if the listing exists, if so, return it.
        listing = self._cache_mgr.get_listing(ds_name)
        if listing is not None:
            status = DatasetQueryStatus.results_valid if listing.FileList is not None else DatasetQueryStatus.does_not_exist
            if (status is not DatasetQueryStatus.does_not_exist or not ds_age_too_old(listing.Created, maxAgeIfNotSeen)) \
                and (status is not DatasetQueryStatus.results_valid or not ds_age_too_old(listing.Created, maxAge)):
                return (status, listing.FileList)

        # Need to write something into the cache that indicates we are working on getting this query back.
        # TODO: Fix race condition - if two threads are looking at the same thing, both might generate a rucio query
        # unless this code above and here is protected.
        if self._cache_mgr.query_in_progress(ds_name):
            return (DatasetQueryStatus.query_queued, None)
        self._cache_mgr.mark_query(ds_name)
            
        # Queue up run against rucio to find out what is in this dataset.
        self._exe.submit(dataset_mgr._query_rucio, self, ds_name)

        # Return the fact that the query has been queued.
        return (DatasetQueryStatus.query_queued, None)

    def _query_rucio(self, ds_name:str, seconds_to_wait:float=None) -> None:
        '''
        Run a query against rucio and then save the results.

        Arguments
        ds_name         The name of the dataset we should fetch
        seconds_to_wait How many seconds to wait before we re-try this guy
        '''
        # If we should wait before we re-try
        if seconds_to_wait is not None:
            sleep(seconds_to_wait)

        try:
            r = self._rucio.get_file_listing(ds_name, log_func=lambda l: self._log.log('rucio_file_listing', l))
            # Cache the result.
            self._cache_mgr.save_listing(dataset_listing_info(ds_name, r))
        except RucioException:
            self._exe.submit(dataset_mgr._query_rucio, self, ds_name, seconds_to_wait=self._seconds_between_retries)

    def download_ds(self, ds_name: str, do_download:bool=True, prefix:str=None) -> Tuple[DatasetQueryStatus, Optional[List[str]]]:
        '''
        Return the list of files that are in a dataset if they have been downloaded. If not, then queue a query to
        rucio to get the actual file contents.

        Arguments
        ds_name         The rucio fully qualified name of the dataset
        do_download     If true, then do the download if the file isn't local. If the dataset isn't local, then
                        return does_not_exist for the status.
        prefix          A string to be pre-pended to the files before they are returned.

        Returns:
        status        Status of the returned results (see DatasetQueryStatus) and below:
        files         Depends on the status:
                          does_not_exist - files will be None, and the dataset was not found on the last query to rucio.
                          query_queued - files will be None, and a query is pending to update the results
                          results_valid - files will be a list of all files in the dataset. 
                              Empty Dataset: The dataset is empty if the list has len()==0.
                              Dataset with files: The list will have an entry per file. The files will be relative to
                                cache directory, unless prefix is not none - then they will have the prefix added.
        '''
        # Do we know if the dataset already exists or not locally? If so, take advantage of that info.
        status, _ = self.get_ds_contents(ds_name)
        if status == DatasetQueryStatus.does_not_exist:
            return (DatasetQueryStatus.does_not_exist, None)

        # Is the download already in progress? Then bail out.
        if self._cache_mgr.download_in_progress(ds_name):
            return (DatasetQueryStatus.query_queued, None)

        # Check to see if we've downloaded all the files. If so, return them. Otherwise, queue
        # up a fetch.
        f_list = self._cache_mgr.get_ds_contents(ds_name)
        if f_list is None:
            if not do_download:
                return (DatasetQueryStatus.does_not_exist, None)

            self._cache_mgr.mark_downloading(ds_name)
            self._downloader.submit(dataset_mgr._rucio_download, self, ds_name)
            return (DatasetQueryStatus.query_queued, None)
        return (DatasetQueryStatus.results_valid, f_list if prefix is None else [f'{prefix}{fl}' for fl in f_list])

    def _rucio_download(self, ds_name:str, seconds_to_wait:int = None) -> None:
        'Download the files synchronously - this could take a long time'
        # If we should wait before we re-try
        if seconds_to_wait is not None:
            sleep(seconds_to_wait)

        # Try again.
        try:
            self._rucio.download_files(ds_name, self._cache_mgr.get_download_directory(), log_func=lambda l: self._log.log('rucio_downloader', l))
        except RucioException:
            self._downloader.submit(dataset_mgr._rucio_download, self, ds_name, seconds_to_wait=self._seconds_between_retries)
            return

        # Done, mark this as "done"
        self._cache_mgr.mark_download_done(ds_name)
