# This code attempts to package up keeping the grid CERT validated. It needs to be
# invoked periodically.
from ruciopylib.runner import runner

import time

class cert:
    '''
    Drives registration of a grid certificate
    '''
    def register(self, executor=None, log_func = None):
        '''
        Attempt a single registration. This is done syncronsously, and might take a while
        to complete!

        In order to run:
            1. GRID_PASSWORD environment variable must be defined in the process
            2. Your grid proxy must be in the default locally (usually ~/.globus)
            3. GRID_VOMS env variable must be defined ('atlas' for example)
            4. X509_CERTIFICATE must be set to the file you'd like the proxy written to.

        Args:
            executor    If None use default, otherwise use something else to run the command
            log_func    A function that takes a string as an argument - which is the output from the reg process.

        Returns:
            success - True if it happened, false otherwise.

        '''
        run = executor if executor is not None else runner()

        result = run.shell_execute('echo $GRID_PASSWORD | voms-proxy-init -voms $GRID_VOMS',
                    lambda l: log_func(l) if log_func is not None else None)

        # Let the calling guy know how we did.
        return result.shell_status

    def run_registration_loop(self, executor=None, sleep_func=None, quit_func=None):
        '''
        Re-run the registration in a loop - ever 11 hours by default. Please see the documentation
        for the function 'cert.register' for requirements on environment, etc.

        If the registration fails, it will be retried every 5 minutes. If you are on a portable
        moving from one WiFi to another this loop can kick in!

        Arguments:
            executor        Where to run the shell
            sleep_fun       Function to do sleeping for 11 hours
            quit_func       Return true to gracefully exit from loop.
                            Checked just before registration is attempted.

        '''
        # Allow injection for sleep so we can dummy this out in a test.
        sleep_me = time.sleep if sleep_func is None else sleep_func

        # Now a loop
        while True:
            # Terminate?
            if quit_func is not None:
                if quit_func():
                    return

            # Try the registration.
            sleep=11*60*60
            if not self.register():
                sleep = 5*60
            
            # Now, sleep.
            sleep_me(sleep)
