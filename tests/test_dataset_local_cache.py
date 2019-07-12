# Tests for the dataset manager

from ruciopylib.dataset_local_cache import dataset_local_cache, dataset_listing_info
from ruciopylib.rucio import RucioFile
from tests.utils_for_tests import simple_dataset, nonexistant_dataset
import pytest
import tempfile
import shutil
import os

@pytest.fixture()
def local_cache():
    'Setup cache temp directory, and then remove it when done'
    loc = "{0}/desktop-rucio-cache-testing".format(tempfile.gettempdir())
    if os.path.exists(loc):
        shutil.rmtree(loc)
    os.mkdir(loc)
    yield dataset_local_cache(location=loc)
    shutil.rmtree(loc)

def create_ds(ds: dataset_listing_info, cache:dataset_local_cache, write_done_file:bool=True, create_files:bool=True, write_as_parts=False):
    'Create the files for a particular dataset'
    ds_dir = "{0}/{1}".format(cache._loc, ds.Name)
    os.mkdir(ds_dir)
    extra = ".part" if write_as_parts else ""
    if create_files:
        for f in ds.FileList:
            with open("{ds_dir}/{f.filename}{extra}".format(**locals()), 'w') as f_h:
                f_h.write("hi")

    # Write out the mark file
    if write_done_file:
        done_file = f'{cache._loc}/done_downloading/{ds.Name}.txt'
        os.mkdir(f'{cache._loc}/done_downloading')
        with open(done_file, 'w') as f:
            f.write("hi")

def test_dataset_cache_mgr_ctor():
    dataset_local_cache()

def test_ds_listing_cache_hit(local_cache, simple_dataset):
    local_cache.save_listing(simple_dataset)
    assert None is not local_cache.get_listing(simple_dataset.Name)

def test_ds_listing_roundtrip(local_cache, simple_dataset):
    local_cache.save_listing(simple_dataset)
    ds_back = local_cache.get_listing(simple_dataset.Name)
    assert simple_dataset.Name == ds_back.Name
    assert len(simple_dataset.FileList) == len(ds_back.FileList)
    for f in zip(simple_dataset.FileList, ds_back.FileList):
        assert f[0].filename == f[1].filename
        assert f[0].size == f[1].size
        assert f[0].events == f[1].events 

def test_ds_listing_cache_miss(local_cache):
    assert None is local_cache.get_listing("bogus_name")

def test_ds_empty(local_cache, nonexistant_dataset):
    local_cache.save_listing(nonexistant_dataset)
    ds_back = local_cache.get_listing(nonexistant_dataset.Name)
    assert None is ds_back.FileList

def test_ds_local_files(local_cache, simple_dataset):
    create_ds(simple_dataset, local_cache)
    r = local_cache.get_ds_contents(simple_dataset.Name)
    assert r is not None
    assert len(simple_dataset.FileList) == len(r)

    print (r)
    for f in simple_dataset.FileList:
        assert '{simple_dataset.Name}/{f.filename}'.format(**locals()) in r

def test_ds_local_files_weird_parts(local_cache, simple_dataset):
    create_ds(simple_dataset, local_cache, write_as_parts=True)
    r = local_cache.get_ds_contents(simple_dataset.Name)
    assert r is not None
    assert 0 == len(r)

def test_ds_not_local(local_cache, simple_dataset):
    r = local_cache.get_ds_contents(simple_dataset.Name)
    assert r is None

def test_ds_no_downloaded_files(local_cache, simple_dataset):
    create_ds(simple_dataset, local_cache, create_files=False)
    r = local_cache.get_ds_contents(simple_dataset.Name)
    assert r is not None
    assert 0 == len(r)

def test_ds_query_when_downloading(local_cache, simple_dataset):
    create_ds(simple_dataset, local_cache, write_done_file=False)
    r = local_cache.get_ds_contents(simple_dataset.Name)
    assert r is None
