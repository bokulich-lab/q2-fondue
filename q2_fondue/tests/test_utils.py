# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import os
import signal
import threading
import unittest
from threading import Thread
from unittest.mock import patch

from q2_fondue.utils import handle_threaded_exception


class TestUtils(unittest.TestCase):
    package = 'q2_fondue.tests'

    def do_something_with_error(self, msg):
        raise Exception(msg)

    @patch('os.kill')
    def test_handle_threaded_exception_gaierror(self, patch_kill):
        with self.assertLogs(
                level='DEBUG', logger='ThreadedErrorsManager') as cm:
            threading.excepthook = handle_threaded_exception
            error_msg = 'Something went wrong: gaierror is ' \
                        'not JSON serializable.'
            t = Thread(target=self.do_something_with_error, args=(error_msg,))
            t.start()
            t.join()

            self.assertIn('EntrezPy failed to connect to NCBI', cm.output[0])

            pid = os.getpid()
            patch_kill.assert_called_once_with(pid, signal.SIGINT)

    @patch('os.kill')
    def test_handle_threaded_exception_other_errors(self, patch_kill):
        with self.assertLogs(
                level='DEBUG', logger='ThreadedErrorsManager') as cm:
            threading.excepthook = handle_threaded_exception
            error_msg = 'Some unknown exception.'
            t = Thread(target=self.do_something_with_error, args=(error_msg,))
            t.start()
            t.join()

            self.assertIn(
                'Caught <class \'Exception\'> with value '
                '"Some unknown exception."', cm.output[0]
            )

            pid = os.getpid()
            patch_kill.assert_called_once_with(pid, signal.SIGINT)


if __name__ == "__main__":
    unittest.main()
