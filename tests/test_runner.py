# Test the runner

from ruciopylib.runner import runner
import pytest

def test_good_command():
    run = runner()
    result = run.shell_execute("dir")
    assert True == result.shell_status
    assert len(result.shell_output) > 10
    assert not result.shell_output[0].endswith('\n')

def test_log_as_we_go():
    run = runner()
    lines = []
    _ = run.shell_execute("dir", log_func = lambda l: lines.append(l))
    for l in lines:
        print(l)
    assert len(lines) > 10
    assert not (lines[0].endswith('\n'))

def test_bad_command():
    run = runner()
    result = run.shell_execute("dudewhereismysoda")
    assert False == result.shell_status
