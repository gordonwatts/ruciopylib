# Provides the interface to rucio. THis is a pretty raw level interface, and can be used
# to download data files to various places.
from ruciopylib.runner import runner
import re
from collections import namedtuple
from typing import Optional, List

# Info for a single file. Contains the name, the size (in bytes), and the number of events
RucioFile = namedtuple('RucioFile', 'filename size events')

class RucioException(BaseException):
    def __init__ (self, message):
        BaseException.__init__(self, message)

unit_index = {'B': 0, 'KB': 1, 'MB': 2, 'GB': 3, 'TB': 4}
def calc_size(size_str):
    'Given a rucio size string, calculate the number of bytes'
    number, units = size_str.strip().split(' ')
    number = float(number)
    for _ in range(unit_index[units]):
        number = number * 1024
    return int(number)

class rucio:
    r'''
    Provides synchronos access to rucio commands. The methods here will run the `rucio` command
    and parse the returned data.
    '''
    def __init__(self, executor:runner = None):
        '''
        Initialize a rucio controller.

        Arguments:
            executor        Dependency injection for the code that will execute against the command shell.
        '''
        self._runner = executor if executor is not None else runner()

    def get_file_listing(self, ds_name, log_func = None) -> Optional[List[RucioFile]]:
        '''
        Return a list of files in the data set name by querying `rucio`.

        Arguments:
            ds_name     Name, including scope, of the rucio dataset
            log_fund    If set, will get called with each line of loging information.

        Returns:
            None         Dataset doesn't exist according to `rucio`
            [f1, f2,...] Listing of all files that are in the dataset. Each entry contains the name, the size and # of events in the file.
        '''
        # run the command to get the list of files back.
        r = self._runner.shell_execute("rucio list-files {ds_name}".format(**locals()), log_func=log_func)
        
        # See if it failed. If so, figure out what to do next.
        if r.shell_result == 12 and any("" in l for l in r.shell_output):
            # This is an actual bad dataset. Nothing we do will fix this!
            return None
        elif r.shell_result != 0:
            # Something went wrong. Best option: retry at some point.
            raise RucioException("Unable to get rucio to list files - died with a status code of {r.shell_result}. Try again.".format(**locals()))

        # Now, parse the lines for the data that we return.
        # Example output:
        # | mc16_13TeV:DAOD_EXOT15.17545540._000013.pool.root.1 | 1DCBECFA-EDC0-5840-A25A-8277CA9A31D4 | ad:c14ee390 | 5.956 GB   |    30000 |
        finder = re.compile(r"\|\s+(?P<file_name>[^|]+)\s+\|\s+(?P<guid>[^|]+)\s+\|\s+(?P<hash>[^|]+)\s+\|\s+(?P<size>[^|]+)\s+\|\s+(?P<events>[^|]+)\s+\|")

        return [RucioFile(m.group('file_name'), calc_size(m.group('size')), int(m.group('events'))) for m in [finder.match(l) for l in r.shell_output] if m is not None and (m.group('events') != 'EVENTS')]

    def _status_in_progress(self, ds_name):
        '''
        Mark the status as in progress, and when it is done that it is no longer in progress.
        '''
        class status_adder:
            def __init__(self, ds_name:str, status:status_mgr):
                self._status = status
                self._ds = ds_name

            def __enter__(self):
                old = self._status.status_value('rucio_download', 'downloading')
                old = old if old is not None else []
                old.append(self._ds)
                self._status.save_status('rucio_download', {'downloading': old})

            def __exit__(self, type, value, traceback):
                old = self._status.status_value('rucio_download', 'downloading')
                old.remove(self._ds)
                self._status.save_status('rucio_download', {'downloading': old})
        return status_adder(ds_name, self._status)

    def download_files(self, ds_name:str, data_dir:str, log_func = None) -> Optional[List[RucioFile]]:
        '''
        Download files in a dataset.

        Arguments:
            ds_name:            The name of the dataset
            data_dir:           Root directory where the files should be downloaded to
            log_func:           Called with each line of output from the shell executing
                                the download command.

        Returns:
            file_list           None if the dataset does not exist
                                List of RucioFiles for each file that was downloaded

        Raises:
            Exception           If something went wrong that isn't either of the above  
                                two (generally means this command needs to be retried).
        '''
        with self._status_in_progress(ds_name):
            r = self._runner.shell_execute("cd {data_dir}; rucio download {ds_name}".format(**locals()), log_func=log_func)
            if r.shell_status:
                pat = re.compile(r".*File (?P<file_name>\S+) successfully downloaded.*")
                files = []
                for l in r.shell_output:
                    m = pat.match(l)
                    if m:
                        files.append(m.group("file_name"))
                return files
        
            # We failed. Time to figure out why and return the proper type of error.
            for l in r.shell_output:
                print ("->" + l)
            if any("Using main thread to download 0 file" in l for l in r.shell_output):
                return None
            raise RucioException("Rucio failed with exit code {0}.".format(r.shell_result))