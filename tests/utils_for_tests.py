# Some common utilities for using in tess.

from ruciopylib.cert import cert
from ruciopylib.runner import exe_result
from ruciopylib.rucio import RucioFile
from ruciopylib.dataset_local_cache import dataset_listing_info
import pytest
from datetime import timedelta
from time import sleep

@pytest.fixture()
def gcert():
    yield cert()

# TODO: We really do not need this
class run_dummy_single:
    def __init__(self, success, lines):
        self._result = exe_result(success, success == 0, lines)

    def shell_execute(self, cmd, log_callback):
        if log_callback is not None:
            for l in self._result.shell_output:
                log_callback(l)
        return self._result

class run_dummy_multiple:
    '''Enable a conversation band and forth'''
    def __init__(self, responses):
        '''
        Build a runner

        Args:
            responses:      Dictionary of responses keyed by command names.
                            The response are dictionaries that contain the following keys:
                                shell_output: Array of lines. Single entries with multiple lines are split.
                                shell_result: The exit_code
                                delay: Option, number of seconds it should take to run this command.

        '''
        self._responses = responses
        self.Running = False
        self.ExecutionCount = 0

    def shell_execute(self, cmd, log_func = None):
        # Make sure we have the command.
        assert cmd in self._responses

        # Got it, now feed back the info in there.
        lines = [l for l_grp in self._responses[cmd]['shell_output'] for l in str.splitlines(l_grp)]
        #lines = l for l in str.splitlines(l_grp): for l_grp in self._responses[cmd]['shell_output']
        if 'delay' in self._responses[cmd]:
            self.Running = True
            sleep(self._responses[cmd]['delay'])
            self.Running = False
        if log_func is not None:
            for l in lines:
                log_func(l)
        self.ExecutionCount += 1
        return exe_result(self._responses[cmd]['shell_result'], self._responses[cmd]['shell_result']==0, lines)

### For help with certificate grabbing
@pytest.fixture()
def cert_good_runner():
    'We will successfully register the thing'
    lines = ['Enter GRID pass phrase for this identity:Contacting lcg-voms2.cern.ch:15001 [/DC=ch/DC=cern/OU=computers/CN=lcg-voms2.cern.ch] "atlas"...',
              'Error contacting lcg-voms2.cern.ch:15001 for VO atlas: Remote host closed connection during handshake',
              'Remote VOMS server contacted succesfully.',
              '',
              '',
              'Created proxy in /usr/usercertfile.',
              '',
              'Your proxy is valid until Mon Apr 22 10:20:03 UTC 2019]']
    yield run_dummy_single(0, lines)

# A simple dataset for use in testing.
@pytest.fixture()
def simple_dataset():
    'Create a simple dataset with 2 files in it'
    f1 = RucioFile('f1.root', 100, 1)
    f2 = RucioFile('f2.root', 200, 2)
    return dataset_listing_info('dataset1', [f1, f2])

@pytest.fixture()
def empty_dataset():
    'Create a simple dataset with 2 files in it'
    return dataset_listing_info('dataset1', [])

@pytest.fixture()
def nonexistant_dataset():
    'Create a simple dataset with 2 files in it'
    return dataset_listing_info('dataset1', None)
