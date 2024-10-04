# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import gzip
import os
import signal
import tempfile
import threading
import unittest
from threading import Thread
from unittest.mock import patch, MagicMock

from qiime2.plugin.testing import TestPluginBase
from tqdm import tqdm

from q2_fondue.utils import (handle_threaded_exception, _has_enough_space,
                             _find_next_id, _chunker, _rewrite_fastq)


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
        patched_run.return_value = MagicMock(returncode=0)

        acc, test_dir = 'ABC123', 'some/where'
        obs = _has_enough_space(acc, test_dir)
        self.assertTrue(obs)
        patched_run.assert_called_once_with(
            ['fasterq-dump', '--size-check', 'only', '-x', acc],
            text=True, capture_output=True, cwd=test_dir
        )

    @patch('subprocess.run')
    def test_has_enough_space_not(self, patched_run):
        with open(self.get_data_path('fasterq-dump-response.txt')) as f:
            response = ''.join(f.readlines())
        patched_run.return_value = MagicMock(stderr=response, returncode=3)

        acc, test_dir = 'ABC123', 'some/where'
        obs = _has_enough_space(acc, test_dir)
        self.assertFalse(obs)
        patched_run.assert_called_once_with(
            ['fasterq-dump', '--size-check', 'only', '-x', acc],
            text=True, capture_output=True, cwd=test_dir
        )

    @patch('subprocess.run')
    def test_has_enough_space_error(self, patched_run):
        patched_run.return_value = MagicMock(stderr='errorX', returncode=8)

        acc, test_dir = 'ABC123', 'some/where'
        with self.assertLogs('q2_fondue.utils', level='ERROR') as cm:
            obs = _has_enough_space(acc, test_dir)
        self.assertEqual(
            cm.output,
            ['ERROR:q2_fondue.utils:fasterq-dump exited with a "8" error code '
             '(the message was: "errorX"). We will try to fetch the next '
             'accession ID.']
        )
        self.assertTrue(obs)
        patched_run.assert_called_once_with(
            ['fasterq-dump', '--size-check', 'only', '-x', acc],
            text=True, capture_output=True, cwd=test_dir
        )

    def test_find_next_id(self):
        pbar = tqdm(['A', 'B', 'C'])
        obs = _find_next_id('B', pbar)
        self.assertEqual(obs, 'C')

    def test_find_next_id_last(self):
        pbar = tqdm(['A', 'B', 'C'])
        obs = _find_next_id('C', pbar)
        self.assertIsNone(obs)

    def test_chunker(self):
        obs_out = _chunker(['A', 'B', 'C'], 2)
        exp_out_1 = ['A', 'B']
        exp_out_2 = ['C']
        self.assertEqual(next(obs_out), exp_out_1)
        self.assertEqual(next(obs_out), exp_out_2)

    def test_chunker_no_chunks(self):
        obs_out = _chunker(['A', 'B', 'C'], 4)
        exp_out = ['A', 'B', 'C']
        self.assertEqual(next(obs_out), exp_out)

    def test_rewrite_fastq(self):
        file_in = self.get_data_path('SRR123456.fastq')
        file_out = tempfile.NamedTemporaryFile()

        _rewrite_fastq(file_in, file_out.name)

        with open(file_in, 'rb') as fin:
            with gzip.open(file_out.name, 'r') as fout:
                for lin, lout in zip(fin.readlines(), fout.readlines()):
                    self.assertEqual(lin, lout)

        # clean up
        file_out.close()


if __name__ == "__main__":
    unittest.main()
