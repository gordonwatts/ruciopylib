# Manage the cache on disk
# - Contents of datasets
# - What datasets we have
#

from datetime import datetime
from ruciopylib.rucio import RucioFile
from typing import List, Optional
import filelock
import os
import tempfile
import pickle


class dataset_listing_info:
    '''
    Simple object that contains a list of files in the dataset
    '''
    def __init__(self, name: str, files: List[RucioFile], created_time: Optional[datetime] = None):
        '''
        Initialize a dataset file listing.

        Arguments
        created_time:   When this listing was created. Used to calculate age
        flies:          Listing of files. None means the dataset does not exist. Empty list means an empty dataset.
        '''
        self.Name = name
        self.Created = created_time if created_time is not None else datetime.now()
        self.FileList = files


class dataset_local_cache:
    r'''
    Manage the cache that contains the datasets we are going to be storing, along with some
    cached information to speed up subsequent rucio queries.

    This code does not talk to rucio - code that does talks to this code.
    '''

    def __init__(self, location=None):
        '''
        Initialize the dataset cache.

        Arguments:
            location            If given, use that as the proper location of the cache.
                                Defaults to a temp directory /tmp/rucio-cache.
        '''
        self._loc = location if location is not None else "{0}/rucio-cache".format(tempfile.gettempdir())
        if not os.path.exists(self._loc):
            os.mkdir(self._loc)

    def get_download_directory(self):
        'Return the directory where all data should be downloaded'
        return self._loc

    def _get_directory(self, dirname):
        d = "{self._loc}/{dirname}".format(**locals())
        if not os.path.exists(d):
            os.mkdir(d)
        return d

    def _get_filename(self, dirname, fname_stub, ext="pickle"):
        d = self._get_directory(dirname)
        return "{d}/{fname_stub}.{ext}".format(**locals())

    def save_listing(self, ds_listing: dataset_listing_info) -> None:
        'Save a listing to the cache'
        with open(self._get_filename("cache", ds_listing.Name), 'wb') as f:
            pickle.dump(ds_listing, f)

    def get_listing(self, name) -> Optional[dataset_listing_info]:
        'Return the listing. None if the listing does not exist'
        f_name = self._get_filename("cache", name)
        if not os.path.exists(f_name):
            return None
        with open(f_name, 'rb') as f:
            return pickle.load(f)

    def mark_dataset_done(self, name: str) -> None:
        '''
        Marks a dataset as having been completely downloaded.

        Arguments:
            name:       Name of the dataset
        '''
        f_done = self._get_filename("done_downloading", name, ext="txt")
        if not os.path.exists(os.path.basename(f_done)):
            os.mkdir(os.path.basename(f_done))
        if not os.path.exists(f_done):
            with open(f_done, 'w') as f:
                f.write("Done\n")

    def get_dataset_downloading_lock(self, ds_name: str) -> None:
        'Returns a lock. Use in a with statement'
        f_lock = self._get_filename('download_lock', ds_name, ext='lock')
        return filelock.SoftFileLock(f_lock, 0)

    def _check_dataset_done(self, name: str) -> bool:
        '''
        See if the dataset done mark exists

        Arguments:
            name:       Name of the dataset

        Returns
            True        The mark file is present
            False       The mark file is not present.
        '''
        f_done = self._get_filename("done_downloading", name, ext="txt")
        return os.path.exists(f_done)

    def get_ds_contents(self, name: str) -> Optional[List[str]]:
        '''
        Return the list of files in the current dataset

        Args:
            name:       Name fo the dataset

        Returns:
            [files]:    List of files.
                        None if this dataset has not been downloaded successfully, locally.
                        None if a download is currently in progress for this
                            dataset.
                        [] - Empty list if this dataset is known locally, but has no files.
        '''
        # If the download isn't done.
        if not self._check_dataset_done(name):
            return None

        f_name = "{0}/{1}".format(self._loc, name)
        if not os.path.isdir(f_name):
            return None

        # Find all the non-part files in the directory.
        result = []
        for f in os.listdir(f_name):
            if not f.endswith(".part"):
                result.append("{name}/{f}".format(**locals()))
        return result
