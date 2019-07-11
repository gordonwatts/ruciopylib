# Test the grid certificate methods.
import sys
sys.path.append(".")

import pytest
from tests.utils_for_tests import run_dummy_single, cert_good_runner, gcert
from ruciopylib.cert import cert

@pytest.fixture()
def cert_bad_runner_password():
    'We will fail to re register.'
    lines = ["Enter GRID pass phrase for this identity:Credentials couldn't be loaded [/root/.globus/userkey.pem, /root/.globus/usercert.pem]: Error decrypting private key: the password is incorrect or the PEM data is corrupted.",
             "No credentials found!"]
    yield run_dummy_single(1, lines)

@pytest.fixture()
def cert_bad_runner_voms():
    'We will fail to re register.'
    lines = ["VOMS server for VO atlassss is not known! Check your vomses configuration."]
    yield run_dummy_single(1, lines)

@pytest.fixture()
def cert_bad_runner_bad_internet():
    'We will fail to re register.'
    lines = ['Enter GRID pass phrase for this identity:Contacting voms2.cern.ch:15001 [/DC=ch/DC=cern/OU=computers/CN=voms2.cern.ch] "atlas"...',
            'Error contacting voms2.cern.ch:15001 for VO atlas: voms2.cern.ch',
            'Error contacting voms2.cern.ch:15001 for VO atlas: voms2.cern.ch',
            'Error contacting voms2.cern.ch:15001 for VO atlas: REST and legacy VOMS endpoints failed.',
            'Contacting lcg-voms2.cern.ch:15001 [/DC=ch/DC=cern/OU=computers/CN=lcg-voms2.cern.ch] "atlas"...',
            'Error contacting lcg-voms2.cern.ch:15001 for VO atlas: lcg-voms2.cern.ch',
            'Error contacting lcg-voms2.cern.ch:15001 for VO atlas: lcg-voms2.cern.ch',
            'Error contacting lcg-voms2.cern.ch:15001 for VO atlas: REST and legacy VOMS endpoints failed.',
            'None of the contacted servers for atlas were capable of returning a valid AC for the user.',
            "User's request for VOMS attributes could not be fulfilled."]
    yield run_dummy_single(1, lines)

def test_ctor():
    _ = cert()

def test_register(gcert, cert_good_runner):
    assert True is gcert.register(executor=cert_good_runner)

def test_fail_register(gcert, cert_bad_runner_password):
    assert False is gcert.register(executor=cert_bad_runner_password)
