# Test out everything with datasets.

from ruciopylib.rucio_cache_interface import rucio_cache_interface, DatasetQueryStatus
from ruciopylib.rucio import RucioException
from tests.utils_for_tests import simple_dataset
from time import sleep
import datetime
import os

import pytest

@pytest.fixture()
def rucio_2file_dataset(simple_dataset):
    class rucio_dummy:
        def __init__(self, ds):
            self._ds = ds
            self.CountCalled = 0
            self.CountCalledDL = 0
            self._cache_mgr = None

        def get_file_listing(self, ds_name, log_func = None):
            self.CountCalled += 1
            if ds_name == self._ds.Name:
                if log_func is not None:
                    log_func("Got the file")
                return self._ds.FileList
            if log_func is not None:
                log_func("Bummer dude")
            return None

        def download_files(self, ds_name, data_dir, log_func = None):
            if self._cache_mgr is not None:
                self._cache_mgr.add_ds(self._ds)
            if log_func is not None:
                log_func('downloading ' + ds_name)
            self.CountCalledDL += 1


    return rucio_dummy(simple_dataset)

@pytest.fixture()
def rucio_do_nothing():
    class rucio_dummy:
        def __init__(self):
            self.CountCalled = 0
            self.CountCalledDL = 0
            
        def get_file_listing(self, ds_name, log_func = None):
            self.CountCalled += 1
            sleep(1)
            return None

        def download_files(self, ds_name, data_dir, log_func = None):
            self.CountCalledDL += 1
            sleep(1)

    return rucio_dummy()

@pytest.fixture()
def rucio_2file_dataset_take_time(simple_dataset):
    class rucio_dummy:
        def __init__(self, ds):
            self._ds = ds
            self.CountCalled = 0
            self.CountCalledDL = 0
            self._cache_mgr = None
            self.DLCalled = False

        def get_file_listing(self, ds_name, log_func = None):
            sleep(0.005)
            self.CountCalled += 1
            if ds_name == self._ds.Name:
                return self._ds.FileList
            return None

        def download_files(self, ds_name, data_dir, log_func = None):
            self.DLCalled = True
            sleep(0.005)
            if self._cache_mgr is not None:
                self._cache_mgr.add_ds(self._ds)
            self.CountCalledDL += 1

    return rucio_dummy(simple_dataset)

@pytest.fixture()
def rucio_2file_dataset_with_fails(simple_dataset):
    class rucio_dummy:
        def __init__(self, ds):
            self._ds = ds
            self.CountCalled = 0
            self.CountCalledDL = 0
            self._cache_mgr = None
            self.DLSleep = None

        def get_file_listing(self, ds_name, log_func = None):
            self.CountCalled += 1
            if self.CountCalled < 5:
                raise RucioException("Please Try again Due To Internet Being Out")
            if ds_name == self._ds.Name:
                return self._ds.FileList
            return None

        def download_files(self, ds_name, data_dir, log_func = None):
            self.CountCalledDL += 1
            if self.DLSleep is not None:
                sleep(self.DLSleep)
            if self.CountCalledDL < 5:
                raise RucioException("Please try again due to internet being out")
            if self._cache_mgr is not None:
                self._cache_mgr.add_ds(self._ds)

    return rucio_dummy(simple_dataset)

@pytest.fixture()
def rucio_2file_dataset_shows_up_later(simple_dataset):
    class rucio_dummy:
        def __init__(self, ds):
            self._ds = ds
            self.CountCalled = 0

        def get_file_listing(self, ds_name, log_func = None):
            self.CountCalled += 1
            if self.CountCalled < 2:
                return None
            if ds_name == self._ds.Name:
                return self._ds.FileList
            return None

    return rucio_dummy(simple_dataset)

@pytest.fixture()
def cache_empty():
    'Create an empty cache that will save anything saved in it.'
    class cache_good_dummy():
        def __init__(self):
            self._ds_list = {}
            self._downloaded_ds = {}
        
        def get_download_directory(self):
            return 'totally-bogus'

        def add_ds(self, ds_info):
            self._downloaded_ds[ds_info.Name] = ds_info

        def get_listing(self, ds_name):
            if ds_name in self._ds_list:
                return self._ds_list[ds_name]
            return None

        def save_listing(self, ds_info):
            self._ds_list[ds_info.Name] = ds_info

        def get_ds_contents(self, ds_name):
            if ds_name in self._downloaded_ds:
                return [f.filename for f in self._downloaded_ds[ds_name].FileList]
            return None

    return cache_good_dummy()

@pytest.fixture()
def cache_with_ds(cache_empty, simple_dataset):
    'Create a cache with a dataset called dataset1'
    cache_empty.add_ds(simple_dataset)
    return cache_empty

def wait_some_time(check):
    'Simple method to wait until check returns false. Will wait up to about a second so as not to delay things before throwing an assert.'
    counter = 0
    while check():
        sleep(0.01)
        counter += 1
        assert counter < 100

def test_dataset_query_resolved(rucio_2file_dataset, cache_empty, simple_dataset):
    'Queue and look for a dataset query result'
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset)

    # Now, make sure that we get back what we want here.
    status, files = dm.get_ds_contents(simple_dataset.Name)
    assert DatasetQueryStatus.results_valid == status
    assert len(simple_dataset.FileList) == len(files)

    # Make sure we didn't re-query for this.
    assert 1 == rucio_2file_dataset.CountCalled == 1
    _ = cache_empty.get_listing(simple_dataset.Name)

def test_dataset_query_resolved_logs(rucio_2file_dataset, cache_empty, simple_dataset):
    'Make sure the dataset is logged correctly'
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset)
    ln = []
    _ = dm.get_ds_contents(simple_dataset.Name, log_func=lambda l: ln.append(l))
    assert len(ln) > 0

def test_query_for_bad_dataset(rucio_2file_dataset, cache_empty, simple_dataset):
    'Ask for a bad dataset, and get back a null'
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset)
    _ = dm.get_ds_contents('bogus_ds')
    wait_some_time(lambda: rucio_2file_dataset.CountCalled == 0)

    # Make sure it comes back as bad.
    status, files = dm.get_ds_contents('bogus_ds')
    assert DatasetQueryStatus.does_not_exist == status
    assert None is files

    # Make sure that a timeout of an hour has been set on the dataset.
    info = cache_empty.get_listing('bogus_ds')
    assert datetime.datetime.now() == info.Created

def test_look_for_good_dataset_that_fails_a_bunch(rucio_2file_dataset_with_fails, cache_empty, simple_dataset):
    'Queue and look for a good dataset that takes a few queries to show up with results'
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset_with_fails, seconds_between_retries=0.01)
    _ = dm.get_ds_contents(simple_dataset.Name)

    # Wait for the dataset query to run
    wait_some_time(lambda: rucio_2file_dataset_with_fails.CountCalled < 5)

    # Now, make sure that we get back what we want and that the number of tries matches what we think
    # it should have.
    status, files = dm.get_ds_contents(simple_dataset.Name)
    assert DatasetQueryStatus.results_valid == status
    assert 5 == rucio_2file_dataset_with_fails.CountCalled

def test_two_queries_for_good_dataset(rucio_2file_dataset_take_time, cache_empty, simple_dataset):
    'Make sure second query does not trigger second web download'
    # Query twice, make sure we don't forget as we are doing this!
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset_take_time)
    _ = dm.get_ds_contents(simple_dataset.Name)
    status, _ = dm.get_ds_contents(simple_dataset.Name)
    assert DatasetQueryStatus.results_valid == status

    # Now, make sure that we get back what we want here.
    status, _ = dm.get_ds_contents(simple_dataset.Name)
    assert DatasetQueryStatus.results_valid == status

    # Make sure we didn't re-query for this, and the expiration date is not set.
    # Make sure to wait long enough for other timing stuff above to fall apart.
    assert 1 == rucio_2file_dataset_take_time.CountCalled

def test_dataset_appears(rucio_2file_dataset_shows_up_later, cache_empty, simple_dataset):
    'After a bad dataset has aged, automatically queue a new query'
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset_shows_up_later)
    _ = dm.get_ds_contents(simple_dataset.Name)
    wait_some_time(lambda: rucio_2file_dataset_shows_up_later.CountCalled == 0)
    status, _ = dm.get_ds_contents(simple_dataset.Name)
    assert DatasetQueryStatus.does_not_exist == status

    # Query, but demand a quick re-check
    status, _ = dm.get_ds_contents(simple_dataset.Name, maxAgeIfNotSeen=datetime.timedelta(seconds=0))
    assert DatasetQueryStatus.results_valid == status

def test_dataset_always_missing_noretry(rucio_2file_dataset_shows_up_later, cache_empty, simple_dataset):
    'Do not requery for the dataset'
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset_shows_up_later)
    _ = dm.get_ds_contents(simple_dataset.Name)
    wait_some_time(lambda: rucio_2file_dataset_shows_up_later.CountCalled == 0)
    status, _ = dm.get_ds_contents(simple_dataset.Name)
    assert DatasetQueryStatus.does_not_exist == status

    # Query, but demand a quick re-check
    status, _ = dm.get_ds_contents(simple_dataset.Name, maxAgeIfNotSeen=None)
    assert DatasetQueryStatus.does_not_exist == status
    assert 1 == rucio_2file_dataset_shows_up_later.CountCalled

def test_dataset_always_missing_longretry(rucio_2file_dataset_shows_up_later, cache_empty, simple_dataset):
    'Do not requery for the dataset'
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset_shows_up_later)
    _ = dm.get_ds_contents(simple_dataset.Name)
    wait_some_time(lambda: rucio_2file_dataset_shows_up_later.CountCalled == 0)
    status, _ = dm.get_ds_contents(simple_dataset.Name)
    assert DatasetQueryStatus.does_not_exist == status

    # Query, but demand a quick re-check
    status, _ = dm.get_ds_contents(simple_dataset.Name, maxAgeIfNotSeen=datetime.timedelta(seconds=1000))
    assert DatasetQueryStatus.does_not_exist == status
    assert 1 == rucio_2file_dataset_shows_up_later.CountCalled

def test_good_dataset_retry(rucio_2file_dataset, cache_empty, simple_dataset):
    'Do a requery for the dataset'
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset)
    _ = dm.get_ds_contents(simple_dataset.Name)
    wait_some_time(lambda: rucio_2file_dataset.CountCalled == 0)
    status, _ = dm.get_ds_contents(simple_dataset.Name)
    assert DatasetQueryStatus.results_valid == status

    # Query, but demand a quick re-check
    status, _ = dm.get_ds_contents(simple_dataset.Name, maxAge=datetime.timedelta(seconds=0))
    assert DatasetQueryStatus.results_valid == status

    assert 2 == rucio_2file_dataset.CountCalled

def test_good_dataset_longretry(rucio_2file_dataset, cache_empty, simple_dataset):
    'Do not requery for the dataset'
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset)
    _ = dm.get_ds_contents(simple_dataset.Name)
    wait_some_time(lambda: rucio_2file_dataset.CountCalled == 0)
    status, _ = dm.get_ds_contents(simple_dataset.Name)
    assert DatasetQueryStatus.results_valid == status

    # Query, but demand a quick re-check
    status, _ = dm.get_ds_contents(simple_dataset.Name, maxAge=datetime.timedelta(seconds=1000))
    assert DatasetQueryStatus.results_valid == status
    assert 1 == rucio_2file_dataset.CountCalled

def test_good_dataset_maxAgeIfNotSeenNoEffect(rucio_2file_dataset, cache_empty, simple_dataset):
    'Do not requery for the dataset'
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset)
    _ = dm.get_ds_contents(simple_dataset.Name)
    wait_some_time(lambda: rucio_2file_dataset.CountCalled == 0)
    status, _ = dm.get_ds_contents(simple_dataset.Name)
    assert DatasetQueryStatus.results_valid == status

    # Query, but demand a quick re-check
    status, _ = dm.get_ds_contents(simple_dataset.Name, maxAgeIfNotSeen=datetime.timedelta(seconds=0))
    assert DatasetQueryStatus.results_valid == status
    assert 1 == rucio_2file_dataset.CountCalled

def test_dataset_download_good(rucio_2file_dataset, cache_empty, simple_dataset):
    'Queue a download and look for it to show up'
    rucio_2file_dataset._cache_mgr = cache_empty
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset)

    # Now, make sure that we get back what we want here.
    status, files = dm.download_ds(simple_dataset.Name)
    assert DatasetQueryStatus.results_valid == status
    assert len(simple_dataset.FileList) == len(files)

    # Make sure we didn't re-query for this.
    assert 1 == rucio_2file_dataset.CountCalledDL

def test_dataset_download_log(rucio_2file_dataset, cache_empty, simple_dataset):
    'Queue a download and look for it to show up'
    rucio_2file_dataset._cache_mgr = cache_empty
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset)
    ln = []
    _ = dm.download_ds(simple_dataset.Name, log_func=lambda l: ln.append(l))
    assert len(ln) > 0

def test_dataset_download_good_nodownload(rucio_2file_dataset, cache_empty, simple_dataset):
    'Queue a download and look for it to show up'
    rucio_2file_dataset._cache_mgr = cache_empty
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset)
    status, files = dm.download_ds(simple_dataset.Name, do_download=False)
    assert None is files
    assert DatasetQueryStatus.does_not_exist == status

def test_dataset_download_no_exist(rucio_2file_dataset, cache_empty):
    'Queue a download and look for it to show up'
    rucio_2file_dataset._cache_mgr = cache_empty
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset)

    # Now, make sure that we get back what we want here.
    status, files = dm.download_ds('bogus')
    assert DatasetQueryStatus.does_not_exist == status
    assert None is files

def test_dataset_download_with_failures(rucio_2file_dataset_with_fails, cache_empty, simple_dataset):
    'Queue a download, it fails, but then gets there'
    rucio_2file_dataset_with_fails._cache_mgr = cache_empty
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset_with_fails, seconds_between_retries=0.01)
    _ = dm.download_ds(simple_dataset.Name)

    # Wait for the dataset query to run
    wait_some_time(lambda: rucio_2file_dataset_with_fails.CountCalledDL < 5)

    # Now, make sure that we get back what we want here.
    status, files = dm.download_ds(simple_dataset.Name)
    assert DatasetQueryStatus.results_valid == status
    assert len(simple_dataset.FileList) == len(files)

    # 2 failures, so make sure we re-try the right number of times
    assert 5 == rucio_2file_dataset_with_fails.CountCalledDL

def test_dataset_download_restart(rucio_do_nothing, rucio_2file_dataset, cache_empty, simple_dataset):
    rucio_2file_dataset._cache_mgr = cache_empty

    # Trigger the download on one.
    dm0 = rucio_cache_interface(cache_empty, rucio_mgr=rucio_do_nothing)

    # Next, create a second one with the same cache.
    dm = rucio_cache_interface(cache_empty, rucio_mgr=rucio_2file_dataset)
    status, _ = dm.download_ds(simple_dataset.Name)

    assert DatasetQueryStatus.results_valid == status
