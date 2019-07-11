# Runs commands in a subprocess
from collections import namedtuple
from subprocess import Popen, PIPE, STDOUT

exe_result = namedtuple('ExeResult','shell_result shell_status shell_output')

class runner:
    def __init__(self):
        pass
    
    def shell_execute(self, shell_command, log_func = None) -> exe_result:
        '''
        Run in the default command shell, synchronously.

        Args:
            shell_command       The shell command to run
            log_func            Log the lines in real time.

        Returns:
            exe_result:         (shell_result,shell_status,shell_output)
                                shell_result is the exit code
                                shell_status is True if the exit code is 0
                                shell_output are the stdout/stderr lines
        '''
        lines = []
        with Popen(shell_command, shell=True, stdout=PIPE, stderr=STDOUT, bufsize=1, universal_newlines=True) as p:
            for line in p.stdout:
                l_trim = line.rstrip()
                lines.append(l_trim)
                if log_func is not None:
                    log_func(l_trim)
            p.wait()
            return exe_result(p.returncode, p.returncode==0, lines)
