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
from unittest.mock import patch, MagicMock

from qiime2.plugin.testing import TestPluginBase

from q2_fondue.utils import handle_threaded_exception, _has_enough_space


class TestExceptHooks(unittest.TestCase):
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


class TestSRAUtils(TestPluginBase):
    package = 'q2_fondue.tests'

    @patch('subprocess.run')
    def test_has_enough_space(self, patched_run):
        with open(self.get_data_path('vdb-dump-response.txt')) as f:
            response = ''.join(f.readlines())
        patched_run.return_value = MagicMock(stdout=response, returncode=0)

        acc, test_dir = 'ABC123', 'some/where'
        obs = _has_enough_space(acc, test_dir, 24 * 10**10)
        self.assertTrue(obs)

    @patch('subprocess.run')
    def test_has_enough_space_not(self, patched_run):
        with open(self.get_data_path('vdb-dump-response.txt')) as f:
            response = ''.join(f.readlines())
        patched_run.return_value = MagicMock(stdout=response, returncode=0)

        acc, test_dir = 'ABC123', 'some/where'
        obs = _has_enough_space(acc, test_dir, 23 * 10**10)
        self.assertFalse(obs)

    @patch('subprocess.run')
    def test_has_enough_space_error(self, patched_run):
        patched_run.return_value = MagicMock(stderr='errorX', returncode=8)

        acc, test_dir = 'ABC123', 'some/where'
        with self.assertLogs('q2_fondue.utils', level='ERROR') as cm:
            obs = _has_enough_space(acc, test_dir, 23 * 10**10)
        self.assertEqual(
            cm.output,
            ['ERROR:q2_fondue.utils:vdb-dump exited with a "8" error code '
             '(the message was: "errorX"). We will try to fetch the next '
             'accession ID.']
        )
        self.assertTrue(obs)


if __name__ == "__main__":
    unittest.main()
