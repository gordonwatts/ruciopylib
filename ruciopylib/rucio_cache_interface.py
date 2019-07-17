# Higher level object to help manage a group of datasets on disk.
from ruciopylib.rucio import RucioFile, rucio, RucioException
from ruciopylib.dataset_local_cache import dataset_local_cache, dataset_listing_info
from typing import List, Optional, Tuple
import datetime
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, wait
from time import sleep
from retry.api import retry_call
import filelock

DatasetQueryStatus = Enum('DatasetQueryStatus', 'does_not_exist, query_queued, results_valid')

class RucioAlreadyBeingDownloaded(BaseException):
    'Thrown if you try to download a dataset that is already being downloaded by someone else'
    def __init__ (self, msg):
        BaseException.__init__(self, msg)

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
    def __init__(self, data_mgr:dataset_local_cache, 
            rucio_mgr: Optional[rucio] = None,
            seconds_between_retries:float=60.0*5):
        '''
        Setup a dataset_mgr

        Arguments:

            rucio_mgr           Interface to query rucio directly to get back dataset file results.
        '''
        # We want to query rucio one dataset at a time.
        self._rucio = rucio_mgr if rucio_mgr is not None else rucio()
        self._cache_mgr = data_mgr
        self._seconds_between_retries = seconds_between_retries

    def get_ds_contents(self, ds_name: str,
            maxAge: Optional[datetime.timedelta] = None,
            maxAgeIfNotSeen: Optional[datetime.timedelta] = datetime.timedelta(minutes=60),
            log_func=None) -> Tuple[DatasetQueryStatus, Optional[List[RucioFile]]]:
        '''
        Return the list of files that are in a dataset. Use the local cache if possible, and if not,
        then we run rucio to get the listing (so this can take a while!!).
        
        Arguments
        name              The rucio fully qualified name of the dataset
        maxAge            If None - any local results are returned, and if no results are present a query is started
                          if a `timedelta` then:
                              If cached results are older than timedelta, rucio is called to refresh the cache.
        maxAgeIfNotSeen   maxAge takes precedence. If maxAgeIfNotSeen is not None, and there are cached local results:
                              If the results point to a dataset that is valid, then the file list is returned.
                              If last time the cached dataset was queried it wasn't found, and it was queired a more recently
                                  than maxAgeIfNotSeen, then return does not exist.
                              Otherwise re-query rucio to see if the dataset has now appeared.
        log_func            Function called with any logging information.

        Returns
        status        Status of the returned results (see DatasetQueryStatus) and below:
        files         Depends on the status:
                          does_not_exist - files will be None, and the dataset was not found on the last query to rucio.
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

        # If we are here, we need to run the query against rucio for whatever reason.
        # We might be disconnected, or similar, so let this go.
        retry_call(self._query_rucio, [ds_name, log_func], exceptions=RucioException, delay=self._seconds_between_retries)
        listing = self._cache_mgr.get_listing(ds_name)
        status = DatasetQueryStatus.results_valid if listing.FileList is not None else DatasetQueryStatus.does_not_exist
        return (status, listing.FileList)

    def _query_rucio(self, ds_name:str, log_func=None) -> None:
        '''
        Run a query against rucio and then save the results.

        Arguments
        ds_name         The name of the dataset we should fetch
        '''

        try:
            with self._cache_mgr.get_dataset_downloading_lock(ds_name):
                # Run the fetch of the result
                r = self._rucio.get_file_listing(ds_name, log_func=log_func)
                # Cache the result.
                self._cache_mgr.save_listing(dataset_listing_info(ds_name, r))
        except filelock.Timeout:
            raise RucioAlreadyBeingDownloaded(f'Cannot query rucio about contents of dataset as someone elase already has the lock for {ds_name}.')

    def download_ds(self, ds_name: str,
            do_download:bool=True,
            log_func = None) -> Tuple[DatasetQueryStatus, Optional[List[str]]]:
        '''
        Return the list of files that are in a dataset if they have been downloaded.
        If not, then a download is started.

        Arguments
            ds_name         The rucio fully qualified name of the dataset
            do_download     If true, then do the download if the file isn't local. If the dataset isn't local, then
                            return does_not_exist for the status.
            log_func        Function called to log any output that occurs

        Returns:
            status        Status of the returned results (see DatasetQueryStatus) and below:
            files         Depends on the status:
                            does_not_exist - files will be None, and the dataset was not found on the last query to rucio.
                            results_valid - files will be a list of all files in the dataset. 
                                Empty Dataset: The dataset is empty if the list has len()==0.
                                Dataset with files: The list will have an entry per file. The files will be relative to
                                    cache directory, unless prefix is not none - then they will have the prefix added.
        '''
        # Do we know if the dataset already exists or not locally? If so, take advantage of that info.
        status, _ = self.get_ds_contents(ds_name, log_func=log_func)
        if status == DatasetQueryStatus.does_not_exist:
            return (DatasetQueryStatus.does_not_exist, None)

        # Check to see if we've downloaded all the files. If so, return them. Otherwise, queue
        # up a fetch.
        f_list = self._cache_mgr.get_ds_contents(ds_name)
        if f_list is None:
            if not do_download:
                return (DatasetQueryStatus.does_not_exist, None)

            retry_call(self._rucio_download, [ds_name, log_func], exceptions=RucioException, delay=self._seconds_between_retries)
            f_list = self._cache_mgr.get_ds_contents(ds_name)

        return (DatasetQueryStatus.results_valid, f_list)

    def _rucio_download(self, ds_name:str, log_func) -> None:
        'Download the files synchronously - this could take a long time'
        # Make sure we are the only ones
        try:
            with self._cache_mgr.get_dataset_downloading_lock(ds_name):
                self._rucio.download_files(ds_name, self._cache_mgr.get_download_directory(), log_func=log_func)
                # If we make it through here, then we are really done!
                self._cache_mgr.mark_dataset_done(ds_name)
        except filelock.Timeout:
            raise RucioAlreadyBeingDownloaded(f'Someone else has the lock file we need to download for {ds_name}.')