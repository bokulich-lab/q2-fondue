# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
from multiprocessing import Queue, Manager

import gzip
import itertools
import logging
import filecmp
import os
import pandas as pd
import shutil
import tempfile
from q2_types.per_sample_sequences import (
    FastqGzFormat, CasavaOneEightSingleLanePerSampleDirFmt
)
from qiime2 import Artifact
from qiime2.metadata import Metadata
from qiime2.plugin.testing import TestPluginBase
from unittest.mock import patch, call, MagicMock, ANY
from parameterized import parameterized

from q2_fondue.sequences import (
    get_sequences, _run_fasterq_dump_for_all, _process_downloaded_sequences,
    _write_empty_casava, combine_seqs, _write2casava_dir, _announce_completion
)
from q2_fondue.utils import DownloadError


class MockTempDir(tempfile.TemporaryDirectory):
    pass


class SequenceTests(TestPluginBase):
    # class is inspired by class SubsampleTest in
    # q2_demux.tests.test_subsample
    package = 'q2_fondue.tests'

    @classmethod
    def setUpClass(cls) -> None:
        cls.fake_logger = logging.getLogger('test_log')

    def setUp(self):
        super().setUp()
        self.fetched_q = Queue()
        self.manager = Manager()
        self.renamed_q = self.manager.Queue()
        self.processed_q = self.manager.Queue()

    def move_files_2_tmp_dir(self, ls_files):
        test_temp_dir = MockTempDir()

        for file in ls_files:
            path_seq_single = self.get_data_path(file)

            shutil.copy(
                path_seq_single,
                os.path.join(test_temp_dir.name, file)
            )

        return test_temp_dir

    def _validate_sequences_in_samples(self, read_output):
        nb_obs_samples = 0
        ls_seq_length = []
        samples = read_output.sequences.iter_views(FastqGzFormat)

        # iterate over each sample
        for (_, file_loc) in samples:
            # assemble sequences
            nb_obs_samples += 1
            file_fh = gzip.open(str(file_loc), 'rt')

            # Assemble expected sequences, per-sample
            file_seqs = [r for r in itertools.zip_longest(*[file_fh] * 4)]

            ls_seq_length.append(len(file_seqs))

        return nb_obs_samples, ls_seq_length

    def validate_counts(self, single_output, paired_output,
                        ls_exp_lengths_single, ls_exp_lengths_paired):
        nb_samples_single, ls_seq_length_single = \
            self._validate_sequences_in_samples(
                single_output)
        self.assertTrue(nb_samples_single == 1)
        self.assertTrue(ls_seq_length_single == ls_exp_lengths_single)

        # test paired sequences
        nb_samples_paired, ls_seq_length_paired = \
            self._validate_sequences_in_samples(
                paired_output)
        self.assertTrue(nb_samples_paired == 2)
        self.assertTrue(ls_seq_length_paired == ls_exp_lengths_paired)


class TestUtils4SequenceFetching(SequenceTests):

    @patch('os.remove')
    @patch('subprocess.run', return_value=MagicMock(returncode=0))
    @patch('q2_fondue.sequences._has_enough_space', return_value=True)
    def test_run_cmd_fasterq_sra_file(
            self, mock_space_check, mock_subprocess, mock_rm
    ):
        test_temp_dir = self.move_files_2_tmp_dir(['testaccA.fastq',
                                                   'testaccA.sra'])

        ls_acc_ids = ['testaccA']
        exp_prefetch = [
            'prefetch', '-X', 'u', '-O', ls_acc_ids[0], ls_acc_ids[0]
        ]
        exp_fasterq = [
            'fasterq-dump', '-e', str(6), '--size-check', 'on', '-x',
            ls_acc_ids[0]
        ]

        _run_fasterq_dump_for_all(
            ls_acc_ids, test_temp_dir.name, threads=6, key_file='',
            retries=0, fetched_queue=self.fetched_q,
            done_queue=self.processed_q
        )
        mock_subprocess.assert_has_calls([
            call(exp_prefetch, text=True,
                 capture_output=True, cwd=test_temp_dir.name),
            call(exp_fasterq, text=True,
                 capture_output=True, cwd=test_temp_dir.name)
        ])
        mock_rm.assert_called_with(
            os.path.join(test_temp_dir.name, ls_acc_ids[0] + '.sra')
        )
        mock_space_check.assert_not_called()

    @patch('shutil.rmtree')
    @patch('subprocess.run', return_value=MagicMock(returncode=0))
    @patch('q2_fondue.sequences._has_enough_space', return_value=True)
    def test_run_cmd_fasterq_sra_directory(
            self, mock_space_check, mock_subprocess, mock_rm
    ):
        test_temp_dir = self.move_files_2_tmp_dir(['testaccA.fastq'])
        os.makedirs(f'{test_temp_dir.name}/testaccA')

        ls_acc_ids = ['testaccA']
        exp_prefetch = [
            'prefetch', '-X', 'u', '-O', ls_acc_ids[0], ls_acc_ids[0]
        ]
        exp_fasterq = [
            'fasterq-dump', '-e', str(6), '--size-check', 'on', '-x',
            ls_acc_ids[0]
        ]

        _run_fasterq_dump_for_all(
            ls_acc_ids, test_temp_dir.name, threads=6, key_file='',
            retries=0, fetched_queue=self.fetched_q,
            done_queue=self.processed_q
        )
        mock_subprocess.assert_has_calls([
            call(exp_prefetch, text=True,
                 capture_output=True, cwd=test_temp_dir.name),
            call(exp_fasterq, text=True,
                 capture_output=True, cwd=test_temp_dir.name)
        ])
        mock_rm.assert_called_with(
            os.path.join(test_temp_dir.name, ls_acc_ids[0])
        )
        mock_space_check.assert_not_called()

    @patch('shutil.rmtree')
    @patch('subprocess.run', return_value=MagicMock(returncode=0))
    @patch('q2_fondue.sequences._has_enough_space', return_value=True)
    def test_run_cmd_fasterq_with_restricted_key(
            self, mock_space_check, mock_subprocess, mock_rm
    ):
        test_temp_dir = self.move_files_2_tmp_dir(['testaccA.fastq'])
        os.makedirs(f'{test_temp_dir.name}/testaccA')

        ls_acc_ids = ['testaccA']
        key = 'mykey.ngc'
        exp_prefetch = [
            'prefetch', '-X', 'u', '-O', ls_acc_ids[0], '--ngc', key,
            ls_acc_ids[0]
        ]
        exp_fasterq = [
            'fasterq-dump', '-e', str(6), '--size-check', 'on', '-x',
            '--ngc', key, ls_acc_ids[0]
        ]

        _run_fasterq_dump_for_all(
            ls_acc_ids, test_temp_dir.name, threads=6, key_file=key,
            retries=0, fetched_queue=self.fetched_q,
            done_queue=self.processed_q
        )
        mock_subprocess.assert_has_calls([
            call(exp_prefetch, text=True,
                 capture_output=True, cwd=test_temp_dir.name),
            call(exp_fasterq, text=True,
                 capture_output=True, cwd=test_temp_dir.name)
        ])
        mock_rm.assert_called_with(
            os.path.join(test_temp_dir.name, ls_acc_ids[0])
        )
        mock_space_check.assert_not_called()

    @patch('os.remove')
    @patch('subprocess.run', return_value=MagicMock(returncode=0))
    @patch('q2_fondue.sequences._has_enough_space', return_value=True)
    def test_run_fasterq_dump_for_all(
            self, mock_space_check, mock_subprocess, mock_rm
    ):
        test_temp_dir = self.move_files_2_tmp_dir(['testaccA.fastq',
                                                   'testaccA.sra'])
        ls_acc_ids = ['testaccA']
        exp_prefetch = [
            'prefetch', '-X', 'u', '-O', ls_acc_ids[0], ls_acc_ids[0]
        ]
        exp_fasterq = [
            'fasterq-dump', '-e', str(6), '--size-check', 'on', '-x',
            ls_acc_ids[0]
        ]

        with self.assertLogs('q2_fondue.sequences', level='INFO') as cm:
            _run_fasterq_dump_for_all(
                ls_acc_ids, test_temp_dir.name, threads=6, key_file='',
                retries=0, fetched_queue=self.fetched_q,
                done_queue=self.processed_q
            )
            mock_subprocess.assert_has_calls([
                call(exp_prefetch, text=True,
                     capture_output=True, cwd=test_temp_dir.name),
                call(exp_fasterq, text=True,
                     capture_output=True, cwd=test_temp_dir.name)
            ])
            mock_rm.assert_called_with(
                os.path.join(test_temp_dir.name, ls_acc_ids[0] + '.sra')
            )
            mock_space_check.assert_not_called()
            self.assertIn(
                'INFO:q2_fondue.sequences:Download finished.', cm.output
            )
            obs_failed = self.processed_q.get()
            self.assertDictEqual(obs_failed, {'failed_ids': {}})

    @patch('time.sleep')
    @patch('subprocess.run',
           return_value=MagicMock(stderr='Some error', returncode=1))
    @patch('q2_fondue.sequences._has_enough_space', return_value=True)
    def test_run_fasterq_dump_for_all_error(
            self, mock_space_check, mock_subprocess, mock_sleep
    ):
        test_temp_dir = MockTempDir()
        ls_acc_ids = ['test_accERROR']

        with self.assertLogs('q2_fondue.sequences', level='INFO') as cm:
            _run_fasterq_dump_for_all(
                ls_acc_ids, test_temp_dir.name, threads=6, key_file='',
                retries=1, fetched_queue=self.fetched_q,
                done_queue=self.processed_q
            )
            # check retry procedure:
            self.assertEqual(mock_subprocess.call_count, 2)
            mock_space_check.assert_not_called()
            self.assertIn(
                'INFO:q2_fondue.sequences:Download finished. 1 out of 1 '
                'runs failed to fetch. Below are the error messages of the '
                'first 5 failed runs:\nID=test_accERROR, Error=Some error',
                cm.output
            )
            obs_failed = self.processed_q.get()
            self.assertDictEqual(
                obs_failed, {'failed_ids': {'test_accERROR': 'Some error'}}
            )

    @patch('os.remove')
    @patch('time.sleep')
    @patch('subprocess.run')
    @patch('q2_fondue.sequences._has_enough_space', return_value=True)
    def test_run_fasterq_dump_for_all_error_twoids(
            self, mock_space_check, mock_subprocess, mock_sleep, mock_rm
    ):
        test_temp_dir = self.move_files_2_tmp_dir(['testaccA.fastq',
                                                   'testaccA.sra'])
        ls_acc_ids = ['testaccA', 'testaccERROR']
        mock_subprocess.side_effect = [
            MagicMock(returncode=0), MagicMock(returncode=0),
            MagicMock(returncode=1, stderr='Error 1'),
            MagicMock(returncode=1, stderr='Error 2')
        ]

        with self.assertLogs('q2_fondue.sequences', level='INFO') as cm:
            _run_fasterq_dump_for_all(
                ls_acc_ids, test_temp_dir.name, threads=6, key_file='',
                retries=1, fetched_queue=self.fetched_q,
                done_queue=self.processed_q
            )
            # check retry procedure:
            self.assertEqual(mock_subprocess.call_count, 4)
            self.assertIn(
                'INFO:q2_fondue.sequences:Download finished. 1 out of 2 runs '
                'failed to fetch. Below are the error messages of the first '
                '5 failed runs:\nID=testaccERROR, Error=Error 2',
                cm.output
            )
            mock_rm.assert_called_with(
                os.path.join(test_temp_dir.name, ls_acc_ids[0] + '.sra')
            )
            mock_space_check.assert_not_called()
            obs_failed = self.processed_q.get()
            self.assertDictEqual(
                obs_failed, {'failed_ids': {'testaccERROR': 'Error 2'}}
            )

    @patch('shutil.rmtree')
    @patch('shutil.disk_usage', side_effect=[(0, 0, 10), (0, 0, 2)])
    @patch('subprocess.run', side_effect=[MagicMock(returncode=0)] * 2)
    @patch('q2_fondue.sequences._has_enough_space', return_value=False)
    def test_run_fasterq_dump_for_all_space_error(
            self, mock_space_check, mock_subprocess, mock_disk_usage, mock_rm
    ):
        # test checking that space availability break procedure works
        test_temp_dir = MockTempDir()
        os.makedirs(f'{test_temp_dir.name}/testaccA')
        ls_acc_ids = ['testaccA', 'testaccERROR']

        with self.assertLogs('q2_fondue.sequences', level='INFO') as cm:
            _run_fasterq_dump_for_all(
                ls_acc_ids, test_temp_dir.name, threads=6, key_file='',
                retries=2, fetched_queue=self.fetched_q,
                done_queue=self.processed_q
            )
            self.assertEqual(mock_subprocess.call_count, 2)
            self.assertEqual(mock_disk_usage.call_count, 2)
            self.assertIn(
                'INFO:q2_fondue.sequences:Download finished. 1 out of 2 runs '
                'failed to fetch. Below are the error messages of the first '
                '5 failed runs:\nID=testaccERROR, Error=Storage exhausted.',
                cm.output
            )
            mock_rm.assert_called_with(
                os.path.join(test_temp_dir.name, ls_acc_ids[0])
            )
            mock_space_check.assert_called_once_with(
                ls_acc_ids[1], test_temp_dir.name
            )
            obs_failed = self.processed_q.get()
            self.assertDictEqual(
                obs_failed,
                {'failed_ids': {'testaccERROR': 'Storage exhausted.'}}
            )

    @patch('shutil.rmtree')
    @patch('shutil.disk_usage', side_effect=[(0, 0, 10), (0, 0, 2)])
    @patch('subprocess.run', side_effect=[MagicMock(returncode=0)] * 2)
    @patch('q2_fondue.sequences._has_enough_space', return_value=False)
    def test_run_fasterq_dump_for_all_no_last_space_error(
            self, mock_space_check, mock_subprocess, mock_disk_usage, mock_rm
    ):
        # test checking that space availability break procedure does not cause
        # issues when triggered after last run ID
        test_temp_dir = MockTempDir()
        os.makedirs(f'{test_temp_dir.name}/testaccA')
        ls_acc_ids = ['testaccA']

        with self.assertLogs('q2_fondue.sequences', level='INFO') as cm:
            _run_fasterq_dump_for_all(
                ls_acc_ids, test_temp_dir.name, threads=6, key_file='',
                retries=2, fetched_queue=self.fetched_q,
                done_queue=self.processed_q
            )
            self.assertEqual(mock_subprocess.call_count, 2)
            self.assertEqual(mock_disk_usage.call_count, 2)
            self.assertIn(
                'INFO:q2_fondue.sequences:Download finished.', cm.output
            )
            mock_rm.assert_called_with(
                os.path.join(test_temp_dir.name, ls_acc_ids[0])
            )
            mock_space_check.assert_called_once_with(None, test_temp_dir.name)
            obs_failed = self.processed_q.get()
            self.assertDictEqual(obs_failed, {'failed_ids': {}})

    @patch('shutil.rmtree')
    @patch('os.remove')
    @patch('shutil.disk_usage')
    @patch('time.sleep')
    @patch('subprocess.run')
    @patch('q2_fondue.sequences._has_enough_space', return_value=False)
    def test_run_fasterq_dump_for_all_error_and_storage_exhausted(
            self, mock_space_check, mock_subprocess, mock_sleep,
            mock_disk_usage, mock_rm, mock_rmtree
    ):
        test_temp_dir = self.move_files_2_tmp_dir(['testaccA.fastq',
                                                   'testaccA.sra'])
        os.makedirs(f'{test_temp_dir.name}/testaccF')

        ls_acc_ids = ['testaccA', 'testaccERROR', 'testaccF', 'testaccNOSPACE']
        mock_subprocess.side_effect = [
            MagicMock(returncode=0), MagicMock(returncode=0),
            MagicMock(returncode=1, stderr='Error 1'),
            MagicMock(returncode=0), MagicMock(returncode=0)
        ]
        mock_disk_usage.side_effect = [
            (0, 0, 10), (0, 0, 10), (0, 0, 10), (0, 0, 2)
        ]

        with self.assertLogs('q2_fondue.sequences', level='INFO') as cm:
            _run_fasterq_dump_for_all(
                ls_acc_ids, test_temp_dir.name, threads=6, key_file='',
                retries=1, fetched_queue=self.fetched_q,
                done_queue=self.processed_q
            )
            # check retry procedure:
            self.assertEqual(mock_subprocess.call_count, 5)
            self.assertIn(
                'INFO:q2_fondue.sequences:Download finished. 2 out of 4 runs '
                'failed to fetch. Below are the error messages of the first '
                '5 failed runs:\nID=testaccERROR, Error=Error 1'
                '\nID=testaccNOSPACE, Error=Storage exhausted.',
                cm.output
            )
            mock_rm.assert_called_with(
                os.path.join(test_temp_dir.name, 'testaccA.sra')
            )
            mock_rmtree.assert_called_with(
                os.path.join(test_temp_dir.name, 'testaccF')
            )
            mock_space_check.assert_called_once_with(
                ls_acc_ids[-1], test_temp_dir.name
            )
            obs_failed = self.processed_q.get()
            self.assertDictEqual(
                obs_failed,
                {'failed_ids': {'testaccERROR': 'Error 1',
                                'testaccNOSPACE': 'Storage exhausted.'}}
            )

    def test_process_downloaded_sequences(self):
        ids = ['testaccA', 'testacc_1', 'testacc_2']
        test_temp_dir = self.move_files_2_tmp_dir([f'{x}.fastq' for x in ids])

        [self.fetched_q.put(_id) for _id in ids]
        self.fetched_q.put(None)

        _ = _process_downloaded_sequences(
            output_dir=test_temp_dir.name, fetched_queue=self.fetched_q,
            renaming_queue=self.renamed_q, n_workers=1
        )

        ls_act_single, ls_act_paired = [], []
        for _id in iter(self.renamed_q.get, None):
            ls_act_single.append(_id[0][0]) if not _id[0][1] else False
            ls_act_paired.append(_id[0][0]) if _id[0][1] else False

        ls_exp_single = [
            os.path.join(test_temp_dir.name, 'testaccA_01_L001_R1_001.fastq')
        ]
        ls_exp_paired = [
            os.path.join(test_temp_dir.name, 'testacc_00_L001_R1_001.fastq'),
            os.path.join(test_temp_dir.name, 'testacc_00_L001_R2_001.fastq')
        ]

        self.assertEqual(set(ls_act_single), set(ls_exp_single))
        self.assertEqual(set(ls_act_paired), set(ls_exp_paired))

    def test_process_downloaded_sequences_paired_n_single_content(self):
        ids = ['testaccHYB', 'testaccHYB_1', 'testaccHYB_2']
        test_temp_dir = self.move_files_2_tmp_dir([f'{x}.fastq' for x in ids])

        [self.fetched_q.put(_id) for _id in ids]
        self.fetched_q.put(None)

        _ = _process_downloaded_sequences(
            output_dir=test_temp_dir.name, fetched_queue=self.fetched_q,
            renaming_queue=self.renamed_q, n_workers=1
        )

        ls_act_single, ls_act_paired = [], []
        for _id in iter(self.renamed_q.get, None):
            for i in range(0, len(_id)):
                ls_act_single.append(_id[i][0]) if not _id[i][1] else False
                ls_act_paired.append(_id[i][0]) if _id[i][1] else False

        # test that file contents are the same
        self.assertTrue(
            filecmp.cmp(
                ls_act_single[0], self.get_data_path(f'{ids[0]}.fastq')))
        for i in [0, 1]:
            self.assertTrue(
                filecmp.cmp(
                    ls_act_paired[i], self.get_data_path(f'{ids[i+1]}.fastq')))

    def test_write_empty_casava_single(self):
        casava_out_single = CasavaOneEightSingleLanePerSampleDirFmt()
        with self.assertLogs('q2_fondue.sequences', level='INFO') as cm:
            _write_empty_casava('single', casava_out_single)
            exp_filename = 'xxx_01_L001_R1_001.fastq.gz'
            exp_casava_fpath = os.path.join(str(casava_out_single),
                                            exp_filename)
            self.assertTrue(os.path.isfile(exp_casava_fpath))
            self.assertIn(
                'WARNING:q2_fondue.sequences:No single-end sequences '
                'available for these accession IDs.', cm.output
            )

    def test_write_empty_casava_paired(self):
        casava_out_paired = CasavaOneEightSingleLanePerSampleDirFmt()
        with self.assertLogs('q2_fondue.sequences', level='INFO') as cm:
            _write_empty_casava('paired', casava_out_paired)

            for exp_filename in ['xxx_00_L001_R1_001.fastq.gz',
                                 'xxx_00_L001_R2_001.fastq.gz']:
                exp_casava_fpath = os.path.join(str(casava_out_paired),
                                                exp_filename)
                self.assertTrue(os.path.isfile(exp_casava_fpath))
            self.assertIn(
                'WARNING:q2_fondue.sequences:No paired-end sequences '
                'available for these accession IDs.', cm.output
            )

    def test_write2casava_dir_single(self):
        casava_out_single = CasavaOneEightSingleLanePerSampleDirFmt()
        casava_out_paired = CasavaOneEightSingleLanePerSampleDirFmt()
        ls_file_single = ['testaccA_01_L001_R1_001.fastq']
        test_temp_dir = self.move_files_2_tmp_dir(ls_file_single)

        self.renamed_q.put(
            [(os.path.join(test_temp_dir.name, ls_file_single[0]), False)]
        )
        self.renamed_q.put(None)

        _write2casava_dir(
            test_temp_dir.name, str(casava_out_single.path),
            str(casava_out_paired.path), self.renamed_q, self.processed_q
        )
        exp_casava_fpath = os.path.join(str(casava_out_single),
                                        ls_file_single[0] + '.gz')
        self.assertTrue(os.path.isfile(exp_casava_fpath))
        self.assertEqual(1, self.processed_q.qsize())
        self.assertTupleEqual(
            (1, [3]), self._validate_sequences_in_samples(casava_out_single)
        )
        self.assertTupleEqual(
            (0, []), self._validate_sequences_in_samples(casava_out_paired)
        )

    def test_write2casava_dir_paired(self):
        casava_out_single = CasavaOneEightSingleLanePerSampleDirFmt()
        casava_out_paired = CasavaOneEightSingleLanePerSampleDirFmt()
        ls_file_paired = ['testacc_00_L001_R1_001.fastq',
                          'testacc_00_L001_R2_001.fastq']
        test_temp_dir = self.move_files_2_tmp_dir(ls_file_paired)

        self.renamed_q.put([
            (os.path.join(test_temp_dir.name, ls_file_paired[0]), True),
            (os.path.join(test_temp_dir.name, ls_file_paired[1]), True)
        ])
        self.renamed_q.put(None)

        _write2casava_dir(
            test_temp_dir.name, str(casava_out_single.path),
            str(casava_out_paired.path), self.renamed_q, self.processed_q
        )
        exp_casava_fpath_fwd = os.path.join(str(casava_out_paired),
                                            ls_file_paired[0] + '.gz')
        self.assertTrue(os.path.isfile(exp_casava_fpath_fwd))

        exp_casava_fpath_rev = os.path.join(str(casava_out_paired),
                                            ls_file_paired[1] + '.gz')
        self.assertTrue(os.path.isfile(exp_casava_fpath_rev))
        self.assertTupleEqual(
            (0, []), self._validate_sequences_in_samples(casava_out_single)
        )
        self.assertTupleEqual(
            (2, [3, 3]), self._validate_sequences_in_samples(casava_out_paired)
        )

    def test_announce_completion_single(self):
        self.processed_q.put(['fileA.fastq'])
        self.processed_q.put(['fileB.fastq'])
        self.processed_q.put({'failed_ids': {}})

        obs_fail, obs_s, obs_p = _announce_completion(self.processed_q)

        self.assertEqual(self.processed_q.qsize(), 0)
        self.assertDictEqual(obs_fail, {})
        self.assertListEqual(obs_s, [['fileA.fastq'], ['fileB.fastq']])
        self.assertListEqual(obs_p, [])

    def test_announce_completion_paired(self):
        self.processed_q.put(['fileA_1.fastq', 'fileA_2.fastq'])
        self.processed_q.put(['fileB_1.fastq', 'fileB_2.fastq'])
        self.processed_q.put({'failed_ids': {}})

        obs_fail, obs_s, obs_p = _announce_completion(self.processed_q)

        self.assertEqual(self.processed_q.qsize(), 0)
        self.assertDictEqual(obs_fail, {})
        self.assertListEqual(obs_s, [])
        self.assertListEqual(
            obs_p,
            [['fileA_1.fastq', 'fileA_2.fastq'],
             ['fileB_1.fastq', 'fileB_2.fastq']]
        )

    def test_announce_completion_mixed(self):
        self.processed_q.put(['fileA.fastq'])
        self.processed_q.put(['fileB_1.fastq', 'fileB_2.fastq'])
        self.processed_q.put({'failed_ids': {}})

        obs_fail, obs_s, obs_p = _announce_completion(self.processed_q)

        self.assertEqual(self.processed_q.qsize(), 0)
        self.assertDictEqual(obs_fail, {})
        self.assertListEqual(obs_s, [['fileA.fastq']])
        self.assertListEqual(obs_p, [['fileB_1.fastq', 'fileB_2.fastq']])

    def test_announce_completion_with_failed(self):
        self.processed_q.put(['fileA.fastq'])
        self.processed_q.put({'failed_ids': {'fileB': 'some error'}})

        obs_fail, obs_s, obs_p = _announce_completion(self.processed_q)

        self.assertEqual(self.processed_q.qsize(), 0)
        self.assertDictEqual(obs_fail, {'fileB': 'some error'})
        self.assertListEqual(obs_s, [['fileA.fastq']])
        self.assertListEqual(obs_p, [])


class TestSequenceFetching(SequenceTests):

    def prepare_metadata(self, acc_id):
        acc_id_tsv = acc_id + '_md.tsv'
        _ = self.move_files_2_tmp_dir([acc_id_tsv])
        return Metadata.load(self.get_data_path(acc_id_tsv))

    @patch('q2_fondue.sequences.Process')
    @patch('q2_fondue.sequences.Pool')
    @patch('q2_fondue.sequences._announce_completion')
    @patch('tempfile.TemporaryDirectory')
    def test_get_sequences_single_only(
            self, mock_tmpdir, mock_announce, mock_pool, mock_proc
    ):
        acc_id = 'SRR123456'
        ls_file_names = [f'{acc_id}.fastq', f'{acc_id}.sra']
        mock_tmpdir.return_value = self.move_files_2_tmp_dir(ls_file_names)

        test_temp_md = self.prepare_metadata(acc_id)
        mock_announce.return_value = {}, [ls_file_names[0]], []

        with self.assertLogs('q2_fondue.sequences', level='INFO') as cm:
            casava_single, casava_paired, failed_ids = get_sequences(
                test_temp_md, email='some@where.com', retries=0)
            self.assertIsInstance(casava_single,
                                  CasavaOneEightSingleLanePerSampleDirFmt)
            self.assertIsInstance(casava_paired,
                                  CasavaOneEightSingleLanePerSampleDirFmt)
            pd.testing.assert_frame_equal(
                failed_ids, pd.DataFrame(
                    [], index=pd.Index([], name='ID'),
                    columns=['Error message']
                ), check_dtype=False
            )
            mock_proc.assert_has_calls([
                call(target=_run_fasterq_dump_for_all, args=(
                    [acc_id], mock_tmpdir.return_value.name, 1, '', 0,
                    ANY, ANY), daemon=True),
                call(target=_process_downloaded_sequences, args=(
                    mock_tmpdir.return_value.name, ANY, ANY, 1), daemon=True)
            ])
            mock_pool.assert_called_once_with(
                1, _write2casava_dir,
                (mock_tmpdir.return_value.name, ANY, ANY, ANY, ANY)
            )
            self.assertIn(
                'WARNING:q2_fondue.sequences:No paired-end sequences '
                'available for these accession IDs.', cm.output
            )

    @patch('q2_fondue.sequences.Process')
    @patch('q2_fondue.sequences.Pool')
    @patch('q2_fondue.sequences._announce_completion')
    @patch('tempfile.TemporaryDirectory')
    def test_get_sequences_paired_only(
            self, mock_tmpdir, mock_announce, mock_pool, mock_proc
    ):
        acc_id = 'SRR123457'
        ls_file_names = [
            f'{acc_id}_1.fastq', f'{acc_id}_2.fastq', f'{acc_id}.sra'
        ]
        mock_tmpdir.return_value = self.move_files_2_tmp_dir(ls_file_names)

        test_temp_md = self.prepare_metadata(acc_id)
        mock_announce.return_value = {}, [], ls_file_names[:2]

        with self.assertLogs('q2_fondue.sequences', level='INFO') as cm:
            casava_single, casava_paired, failed_ids = get_sequences(
                test_temp_md, email='some@where.com', retries=0)
            self.assertIsInstance(casava_single,
                                  CasavaOneEightSingleLanePerSampleDirFmt)
            self.assertIsInstance(casava_paired,
                                  CasavaOneEightSingleLanePerSampleDirFmt)
            pd.testing.assert_frame_equal(
                failed_ids, pd.DataFrame(
                    [], index=pd.Index([], name='ID'),
                    columns=['Error message']
                ), check_dtype=False
            )
            mock_proc.assert_has_calls([
                call(target=_run_fasterq_dump_for_all, args=(
                    [acc_id], mock_tmpdir.return_value.name, 1, '', 0,
                    ANY, ANY), daemon=True),
                call(target=_process_downloaded_sequences, args=(
                    mock_tmpdir.return_value.name, ANY, ANY, 1), daemon=True),
            ])
            mock_pool.assert_called_once_with(
                1, _write2casava_dir,
                (mock_tmpdir.return_value.name, ANY, ANY, ANY, ANY)
            )
            self.assertIn(
                'WARNING:q2_fondue.sequences:No single-end sequences '
                'available for these accession IDs.', cm.output
            )

    @patch('q2_fondue.sequences.Process')
    @patch('q2_fondue.sequences.Pool')
    @patch('q2_fondue.sequences._announce_completion')
    @patch('tempfile.TemporaryDirectory')
    def test_get_sequences_single_n_paired(
            self, mock_tmpdir, mock_announce, mock_pool, mock_proc
    ):
        ls_file_names = [
            'SRR123456.fastq', 'SRR123457_1.fastq', 'SRR123457_2.fastq',
            'SRR123456.sra', 'SRR123457.sra']
        mock_tmpdir.return_value = self.move_files_2_tmp_dir(ls_file_names)

        test_temp_md = self.prepare_metadata('testaccBC')
        mock_announce.return_value = {}, [ls_file_names[0]], ls_file_names[1:3]

        casava_single, casava_paired, failed_ids = get_sequences(
            test_temp_md, email='some@where.com', retries=0)
        self.assertIsInstance(casava_single,
                              CasavaOneEightSingleLanePerSampleDirFmt)
        self.assertIsInstance(casava_paired,
                              CasavaOneEightSingleLanePerSampleDirFmt)
        pd.testing.assert_frame_equal(
            failed_ids, pd.DataFrame(
                [], index=pd.Index([], name='ID'),
                columns=['Error message']
            ), check_dtype=False
        )
        mock_proc.assert_has_calls([
            call(target=_run_fasterq_dump_for_all, args=(
                ['SRR123456', 'SRR123457'], mock_tmpdir.return_value.name, 1,
                '', 0, ANY, ANY), daemon=True),
            call(target=_process_downloaded_sequences, args=(
                mock_tmpdir.return_value.name, ANY, ANY, 1), daemon=True),
        ])
        mock_pool.assert_called_once_with(
            1, _write2casava_dir,
            (mock_tmpdir.return_value.name, ANY, ANY, ANY, ANY)
        )

    @parameterized.expand([
        ("study", "SRP123456"),
        ("bioproject", "PRJNA734376"),
        ("experiment", "SRX123456"),
        ("sample", "SRS123456")
        ])
    @patch('q2_fondue.sequences.Process')
    @patch('q2_fondue.sequences.Pool')
    @patch('q2_fondue.sequences._announce_completion')
    @patch('q2_fondue.sequences._get_run_ids',
           return_value=['SRR123456'])
    @patch('tempfile.TemporaryDirectory')
    def test_get_sequences_other(
            self, id_type, acc_id, mock_tmpdir, mock_get,  mock_announce,
            mock_pool, mock_proc
    ):
        run_id = 'SRR123456'
        ls_file_names = [f'{run_id}.fastq', f'{run_id}.sra']
        mock_tmpdir.return_value = self.move_files_2_tmp_dir(ls_file_names)
        test_temp_md = self.prepare_metadata(acc_id)
        mock_announce.return_value = {}, [ls_file_names[0]], []

        _, _, _ = get_sequences(
            test_temp_md, email='some@where.com', retries=0)

        mock_get.assert_called_with(
            'some@where.com', 1, [acc_id], None, id_type, 'INFO'
        )
        mock_proc.assert_has_calls([
            call(target=_run_fasterq_dump_for_all, args=(
                [run_id], mock_tmpdir.return_value.name, 1, '',
                0, ANY, ANY), daemon=True),
            call(target=_process_downloaded_sequences, args=(
                mock_tmpdir.return_value.name, ANY, ANY, 1), daemon=True),
        ])
        mock_pool.assert_called_once_with(
            1, _write2casava_dir,
            (mock_tmpdir.return_value.name, ANY, ANY, ANY, ANY)
        )

    @patch('q2_fondue.sequences.Process')
    @patch('q2_fondue.sequences.Pool')
    @patch('q2_fondue.sequences._announce_completion')
    @patch('tempfile.TemporaryDirectory')
    def test_get_sequences_with_failed(
            self, mock_tmpdir, mock_announce, mock_pool, mock_proc
    ):
        ls_file_names = ['SRR123456.fastq']
        mock_tmpdir.return_value = self.move_files_2_tmp_dir(ls_file_names)
        test_temp_md = self.prepare_metadata('testaccBC')
        mock_announce.return_value = \
            {'SRR123457': 'Some error'}, ls_file_names[0], []

        casava_single, casava_paired, failed_ids = get_sequences(
            test_temp_md, email='some@where.com', retries=0)
        self.assertIsInstance(casava_single,
                              CasavaOneEightSingleLanePerSampleDirFmt)
        self.assertIsInstance(casava_paired,
                              CasavaOneEightSingleLanePerSampleDirFmt)
        pd.testing.assert_frame_equal(
            failed_ids, pd.DataFrame(
                ['Some error'], index=pd.Index(['SRR123457'], name='ID'),
                columns=['Error message']
            )
        )
        mock_proc.assert_has_calls([
            call(target=_run_fasterq_dump_for_all, args=(
                ['SRR123456', 'SRR123457'], mock_tmpdir.return_value.name, 1,
                '', 0, ANY, ANY), daemon=True),
            call(target=_process_downloaded_sequences, args=(
                mock_tmpdir.return_value.name, ANY, ANY, 1), daemon=True),
        ])
        mock_pool.assert_called_once_with(
            1, _write2casava_dir,
            (mock_tmpdir.return_value.name, ANY, ANY, ANY, ANY)
        )

    @patch('q2_fondue.sequences.Process')
    @patch('q2_fondue.sequences.Pool')
    @patch('q2_fondue.sequences._announce_completion')
    @patch('tempfile.TemporaryDirectory', return_value=MockTempDir())
    def test_get_sequences_nothing_downloaded(
            self, mock_tmpdir, mock_announce, mock_pool, mock_proc
    ):
        acc_id = 'SRR123456'
        test_temp_md = self.prepare_metadata(acc_id)
        mock_announce.return_value = {}, [], []

        with self.assertRaisesRegex(
                DownloadError,
                'Neither single- nor paired-end sequences could be downloaded'
        ):
            get_sequences(test_temp_md, email='some@where.com', retries=0)
            mock_proc.assert_has_calls([
                call(target=_run_fasterq_dump_for_all, args=(
                    ['SRR123456'], mock_tmpdir.return_value.name,
                    1,
                    0, ANY, ANY), daemon=True),
                call(target=_process_downloaded_sequences, args=(
                    mock_tmpdir.return_value.name, ANY, ANY, 1), daemon=True),
            ])
            mock_pool.assert_called_once_with(
                1, _write2casava_dir,
                (mock_tmpdir.return_value.name, ANY, ANY, ANY, ANY)
            )

    @patch.dict(os.environ, {"KEY_FILEPATH": "path/to/key.ngc"})
    @patch('q2_fondue.sequences.Process')
    @patch('q2_fondue.sequences.Pool')
    @patch('q2_fondue.sequences._announce_completion')
    @patch('tempfile.TemporaryDirectory')
    def test_get_sequences_restricted_access(
        self, mock_tmpdir, mock_announce, mock_pool, mock_proc
    ):
        acc_id = 'SRR123456'
        ls_file_names = [f'{acc_id}.fastq', f'{acc_id}.sra']
        mock_tmpdir.return_value = self.move_files_2_tmp_dir(ls_file_names)

        test_temp_md = self.prepare_metadata(acc_id)
        mock_announce.return_value = {}, [ls_file_names[0]], []

        _, _, _ = get_sequences(
            test_temp_md, email='some@where.com', retries=0,
            restricted_access=True
        )
        mock_proc.assert_has_calls([
            call(target=_run_fasterq_dump_for_all, args=(
                [acc_id], mock_tmpdir.return_value.name, 1,
                'path/to/key.ngc', 0, ANY, ANY), daemon=True),
            call(target=_process_downloaded_sequences, args=(
                mock_tmpdir.return_value.name, ANY, ANY, 1), daemon=True)
        ])


class TestSequenceCombining(SequenceTests):

    def load_seq_artifact(self, type='single', suffix=1):
        t = '' if type == 'single' else 'PairedEnd'
        return Artifact.import_data(
            f'SampleData[{t}SequencesWithQuality]',
            self.get_data_path(f'{type}{suffix}'),
            CasavaOneEightSingleLanePerSampleDirFmt
        ).view(CasavaOneEightSingleLanePerSampleDirFmt)

    def test_combine_samples_single(self):
        seqs = [
            self.load_seq_artifact('single', 1),
            self.load_seq_artifact('single', 2)
        ]
        obs_seqs = combine_seqs(seqs=seqs)
        exp_ids = pd.Index(
            ['SEQID1', 'SEQID2', 'SEQID3', 'SEQID4'], name='sample-id'
        )
        self.assertIsInstance(
            obs_seqs, CasavaOneEightSingleLanePerSampleDirFmt
        )
        pd.testing.assert_index_equal(obs_seqs.manifest.index, exp_ids)
        self.assertFalse(all(obs_seqs.manifest.reverse))

    def test_combine_samples_paired(self):
        seqs = [
            self.load_seq_artifact('paired', 1),
            self.load_seq_artifact('paired', 2)
        ]
        obs_seqs = combine_seqs(seqs=seqs)
        exp_ids = pd.Index(
            ['SEQID1', 'SEQID2', 'SEQID3', 'SEQID4'], name='sample-id'
        )
        self.assertIsInstance(
            obs_seqs, CasavaOneEightSingleLanePerSampleDirFmt
        )
        pd.testing.assert_index_equal(obs_seqs.manifest.index, exp_ids)
        self.assertTrue(all(obs_seqs.manifest.reverse))

    def test_combine_samples_single_duplicated_error(self):
        seqs = [self.load_seq_artifact('single', 1)] * 2

        with self.assertRaisesRegex(
                ValueError, 'Duplicate sequence files.*SEQID1, SEQID2.'
        ):
            combine_seqs(seqs=seqs, on_duplicates='error')

    def test_combine_samples_single_duplicated_warning(self):
        seqs = [self.load_seq_artifact('single', 1)] * 2

        with self.assertWarnsRegex(
                Warning,
                'Duplicate sequence files.*dropped.*SEQID1, SEQID2.'
        ):
            obs_seqs = combine_seqs(seqs=seqs, on_duplicates='warn')
            exp_ids = pd.Index(['SEQID1', 'SEQID2'], name='sample-id')

            self.assertIsInstance(
                obs_seqs, CasavaOneEightSingleLanePerSampleDirFmt
            )
            pd.testing.assert_index_equal(obs_seqs.manifest.index, exp_ids)
            self.assertFalse(all(obs_seqs.manifest.reverse))

    def test_combine_samples_paired_duplicated_error(self):
        seqs = [self.load_seq_artifact('paired', 1)] * 2

        with self.assertRaisesRegex(
                ValueError, 'Duplicate sequence files.*SEQID1, SEQID2.'
        ):
            combine_seqs(seqs=seqs, on_duplicates='error')

    def test_combine_samples_paired_duplicated_warning(self):
        seqs = [self.load_seq_artifact('paired', 1)] * 2

        with self.assertWarnsRegex(
                Warning,
                'Duplicate sequence files.*dropped.*SEQID1, SEQID2.'
        ):
            obs_seqs = combine_seqs(seqs=seqs, on_duplicates='warn')
            exp_ids = pd.Index(['SEQID1', 'SEQID2'], name='sample-id')

            self.assertIsInstance(
                obs_seqs, CasavaOneEightSingleLanePerSampleDirFmt
            )
            pd.testing.assert_index_equal(obs_seqs.manifest.index, exp_ids)
            self.assertTrue(all(obs_seqs.manifest.reverse))

    def test_combine_samples_paired_with_empty_warning(self):
        seqs = [
            self.load_seq_artifact('paired', 1),
            self.load_seq_artifact('empty', '')
        ]

        with self.assertWarnsRegex(
                Warning,
                '1 empty sequence files were found and excluded.'
        ):
            obs_seqs = combine_seqs(seqs=seqs, on_duplicates='warn')
            exp_ids = pd.Index(['SEQID1', 'SEQID2'], name='sample-id')

            self.assertIsInstance(
                obs_seqs, CasavaOneEightSingleLanePerSampleDirFmt
            )
            pd.testing.assert_index_equal(obs_seqs.manifest.index, exp_ids)
            self.assertTrue(all(obs_seqs.manifest.reverse))
