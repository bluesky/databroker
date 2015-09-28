from __future__ import (unicode_literals, absolute_import, division,
                        print_function)
import subprocess, threading


class Command(object):
    """Thanks SO! http://stackoverflow.com/a/4825933

    Example
    -------
    >>> command = Command("echo 'Process started'; sleep 2; echo 'Process finished'")
    >>> command.run(timeout=3)
        Thread started
        Process started
        Process finished
        Thread finished
        0
    >>> command.run(timeout=1)
        Thread started
        Process started
        Terminating process
        Thread finished
        -15
    """
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout):
        def target():
            print('Thread started')
            self.process = subprocess.Popen(self.cmd, shell=True)
            self.process.communicate()
            print('Thread finished')

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            print('Terminating process')
            self.process.terminate()
            thread.join()
        print(self.process.returncode)
