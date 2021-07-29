# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from unittest.mock import patch
import os
import gzip
import shutil
import itertools
import tempfile
from qiime2.plugin.testing import TestPluginBase
from q2_types.per_sample_sequences import (
    FastqGzFormat, CasavaOneEightSingleLanePerSampleDirFmt)
from q2_fondue.sequences import (get_sequences,
                                 _run_fasterq_dump_for_all,
                                 _process_downloaded_sequences,
                                 _write2casava_dir_single,
                                 _write2casava_dir_paired)


class MockTempDir(tempfile.TemporaryDirectory):
    pass


class SequenceTests(TestPluginBase):
    # class is inspired by class SubsampleTest in
    # q2_demux.tests.test_subsample
    package = 'q2_fondue.tests'

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

    @patch('subprocess.run')
    def test_run_fasterq_dump_for_all(self, mock_subprocess):
        test_temp_dir = self.move_files_2_tmp_dir(['testaccA.fastq'])

        ls_accIDs = ['testaccA']
        exp_comd = ["fasterq-dump",
                    "-O", test_temp_dir.name,
                    "-t", test_temp_dir.name,
                    "-e", "6",
                    ls_accIDs[0]]

        _run_fasterq_dump_for_all(
            ls_accIDs, test_temp_dir.name, threads=6, general_retries=0)
        mock_subprocess.assert_called_once_with(exp_comd, text=True,
                                                capture_output=True)

    @patch('subprocess.run')
    def test_run_fasterq_dump_for_all_error(self, mock_subprocess):
        test_temp_dir = MockTempDir()
        ls_accIDs = ['test_accERROR']

        with self.assertRaisesRegex(
                ValueError, 'could not be downloaded with'):
            _run_fasterq_dump_for_all(
                ls_accIDs, test_temp_dir.name, threads=6, general_retries=0)

    # # test _process_downloaded_sequences
    def test_process_downloaded_sequences(self):
        ls_fastq_files = ['testaccA.fastq',
                          'testacc_1.fastq', 'testacc_2.fastq']
        test_temp_dir = self.move_files_2_tmp_dir(ls_fastq_files)

        ls_act_single, ls_act_paired = _process_downloaded_sequences(
            test_temp_dir.name)

        ls_exp_single = ['testaccA_00_L001_R1_001.fastq']
        ls_exp_paired = ['testacc_00_L001_R1_001.fastq',
                         'testacc_00_L001_R2_001.fastq']

        self.assertEqual(ls_act_single, ls_exp_single)
        self.assertEqual(ls_act_paired, ls_exp_paired)

    # test write2casava_dir_single
    def test_write2casava_dir_single(self):
        casava_out_single = CasavaOneEightSingleLanePerSampleDirFmt()
        ls_file_single = ['testaccA_00_L001_R1_001.fastq']
        test_temp_dir = self.move_files_2_tmp_dir(ls_file_single)

        _write2casava_dir_single(test_temp_dir.name, casava_out_single,
                                 ls_file_single)
        exp_casava_fpath = os.path.join(str(casava_out_single),
                                        ls_file_single[0]+'.gz')
        self.assertTrue(os.path.isfile(exp_casava_fpath))

    # test write2casava_dir_paired
    def test_write2casava_dir_paired(self):
        casava_out_paired = CasavaOneEightSingleLanePerSampleDirFmt()
        ls_file_paired = ['testacc_00_L001_R1_001.fastq',
                          'testacc_00_L001_R2_001.fastq']
        test_temp_dir = self.move_files_2_tmp_dir(ls_file_paired)

        _write2casava_dir_paired(test_temp_dir.name, casava_out_paired,
                                 ls_file_paired)

        exp_casava_fpath_fwd = os.path.join(str(casava_out_paired),
                                            ls_file_paired[0]+'.gz')
        self.assertTrue(os.path.isfile(exp_casava_fpath_fwd))

        exp_casava_fpath_rev = os.path.join(str(casava_out_paired),
                                            ls_file_paired[1]+'.gz')
        self.assertTrue(os.path.isfile(exp_casava_fpath_rev))


class TestSequenceFetching(SequenceTests):

    @patch('subprocess.run')
    @patch('tempfile.TemporaryDirectory')
    def test_get_sequences_single_only(self, mock_tmpdir, mock_subprocess):
        accID = 'testaccB'
        test_temp_dir = self.move_files_2_tmp_dir([accID + '.fastq'])
        mock_tmpdir.return_value = test_temp_dir

        with self.assertWarnsRegex(Warning, "No paired-end sequences"):
            casava_single, casava_paired = get_sequences(
                [accID], general_retries=0)
            self.assertIsInstance(casava_single,
                                  CasavaOneEightSingleLanePerSampleDirFmt)
            self.assertIsInstance(casava_paired,
                                  CasavaOneEightSingleLanePerSampleDirFmt)

            self.validate_counts(casava_single, casava_paired,
                                 [3], [0, 0])

    @patch('subprocess.run')
    @patch('tempfile.TemporaryDirectory')
    def test_get_sequences_paired_only(self, mock_tmpdir, mock_subprocess):
        accID = 'testaccC'
        ls_file_names = [accID + '_1.fastq', accID + '_2.fastq']
        test_temp_dir = self.move_files_2_tmp_dir(ls_file_names)
        mock_tmpdir.return_value = test_temp_dir

        with self.assertWarnsRegex(Warning, "No single-read sequences"):
            casava_single, casava_paired = get_sequences(
                [accID], general_retries=0)
            self.assertIsInstance(casava_single,
                                  CasavaOneEightSingleLanePerSampleDirFmt)
            self.assertIsInstance(casava_paired,
                                  CasavaOneEightSingleLanePerSampleDirFmt)

            self.validate_counts(casava_single, casava_paired,
                                 [0], [3, 3])

    @patch('subprocess.run')
    @patch('tempfile.TemporaryDirectory')
    def test_get_sequences_single_n_paired(self, mock_tmpdir, mock_subprocess):
        accID_single = 'testaccB'
        ls_file_names = [accID_single + '.fastq']
        accID_paired = 'testaccC'
        ls_file_names += [accID_paired + '_1.fastq',
                          accID_paired + '_2.fastq']
        test_temp_dir = self.move_files_2_tmp_dir(ls_file_names)
        mock_tmpdir.return_value = test_temp_dir

        casava_single, casava_paired = get_sequences(
            [accID_single, accID_paired], general_retries=0)
        self.assertIsInstance(casava_single,
                              CasavaOneEightSingleLanePerSampleDirFmt)
        self.assertIsInstance(casava_paired,
                              CasavaOneEightSingleLanePerSampleDirFmt)

        self.validate_counts(casava_single, casava_paired,
                             [3], [3, 3])
