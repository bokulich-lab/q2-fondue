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

from q2_types.per_sample_sequences import CasavaOneEightSingleLanePerSampleDirFmt
from qiime2 import Artifact
from qiime2.plugin.testing import TestPluginBase
from tqdm import tqdm

from q2_fondue.utils import (
    handle_threaded_exception, _has_enough_space, _find_next_id, _chunker,
    _rewrite_fastq, _is_empty, _remove_empty, _make_empty_artifact
)


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


class TestSequenceUtils(TestPluginBase):
    package = 'q2_fondue.tests'

    def test_is_empty_with_empty_artifact(self):
        casava_out = CasavaOneEightSingleLanePerSampleDirFmt()
        filenames = ['xxx_01_L001_R1_001.fastq.gz']
        for filename in filenames:
            with gzip.open(str(casava_out.path / filename), mode="w"):
                pass

        artifact = Artifact.import_data(
            'SampleData[SequencesWithQuality]',
            casava_out
        )

        self.assertTrue(_is_empty(artifact))

    def test_is_empty_with_nonempty_artifact(self):
        artifact = Artifact.import_data(
            'SampleData[SequencesWithQuality]',
            self.get_data_path('single1'),
            CasavaOneEightSingleLanePerSampleDirFmt
        )

        self.assertFalse(_is_empty(artifact))

    def test_remove_empty(self):
        empty_casava = CasavaOneEightSingleLanePerSampleDirFmt()
        with gzip.open(
                str(empty_casava.path / 'xxx_01_L001_R1_001.fastq.gz'), mode="w"
        ):
            pass
        empty_artifact_single = Artifact.import_data(
            'SampleData[SequencesWithQuality]',
            empty_casava
        )
        with gzip.open(
                str(empty_casava.path / 'xxx_01_L001_R2_001.fastq.gz'), mode="w"
        ):
            pass
        empty_artifact_paired = Artifact.import_data(
            'SampleData[PairedEndSequencesWithQuality]',
            empty_casava
        )

        non_empty_artifact_single = Artifact.import_data(
            'SampleData[SequencesWithQuality]',
            self.get_data_path('single1'),
            CasavaOneEightSingleLanePerSampleDirFmt
        )
        non_empty_artifact_paired = Artifact.import_data(
            'SampleData[PairedEndSequencesWithQuality]',
            self.get_data_path('paired1'),
            CasavaOneEightSingleLanePerSampleDirFmt
        )

        singles = [empty_artifact_single, non_empty_artifact_single]
        paired = [empty_artifact_paired, non_empty_artifact_paired]

        filtered_singles, filtered_paired = _remove_empty(singles, paired)

        self.assertEqual(len(filtered_singles), 1)
        self.assertEqual(len(filtered_paired), 1)
        self.assertIs(filtered_singles[0], non_empty_artifact_single)
        self.assertIs(filtered_paired[0], non_empty_artifact_paired)

    def test_make_empty_artifact_single(self):
        ctx = MagicMock()
        ctx.make_artifact.return_value = "single_artifact"

        result = _make_empty_artifact(ctx, False)

        self.assertEqual(result, "single_artifact")
        ctx.make_artifact.assert_called_once()

        args, kwargs = ctx.make_artifact.call_args

        self.assertEqual(args[0], "SampleData[SequencesWithQuality]")

        casava_output = args[1]
        self.assertTrue(
            os.path.exists(casava_output.path / 'xxx_01_L001_R1_001.fastq.gz')
        )

    def test_make_empty_artifact_paired(self):
        ctx = MagicMock()
        ctx.make_artifact.return_value = "paired_artifact"

        result = _make_empty_artifact(ctx, True)

        self.assertEqual(result, "paired_artifact")
        ctx.make_artifact.assert_called_once()

        args, kwargs = ctx.make_artifact.call_args

        self.assertEqual(args[0], "SampleData[PairedEndSequencesWithQuality]")

        casava_output = args[1]
        self.assertTrue(
            os.path.exists(casava_output.path / 'xxx_00_L001_R1_001.fastq.gz')
        )
        self.assertTrue(
            os.path.exists(casava_output.path / 'xxx_00_L001_R2_001.fastq.gz')
        )


if __name__ == "__main__":
    unittest.main()
