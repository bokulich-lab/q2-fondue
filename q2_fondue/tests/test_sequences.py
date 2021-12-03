# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import logging
from unittest.mock import patch, call, ANY
import os
import gzip
import shutil
import itertools
import tempfile

from qiime2.metadata import Metadata
from qiime2.plugin.testing import TestPluginBase
from q2_types.per_sample_sequences import (
    FastqGzFormat, CasavaOneEightSingleLanePerSampleDirFmt)
from q2_fondue.sequences import (get_sequences,
                                 _run_fasterq_dump_for_all,
                                 _process_downloaded_sequences,
                                 _write_empty_casava,
                                 _write2casava_dir_single,
                                 _write2casava_dir_paired)


class MockTempDir(tempfile.TemporaryDirectory):
    pass


class SequenceTests(TestPluginBase):
    # class is inspired by class SubsampleTest in
    # q2_demux.tests.test_subsample
    package = 'q2_fondue.tests'

    @classmethod
    def setUpClass(cls) -> None:
        cls.logger = logging.getLogger()

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
        test_temp_dir = self.move_files_2_tmp_dir(['testaccA.fastq',
                                                   'testaccA.sra'])
        ls_acc_ids = ['testaccA']

        exp_prefetch = ['prefetch',
                        '-O', test_temp_dir.name,
                        ls_acc_ids[0]]
        exp_fasterq = ['fasterq-dump',
                       '-O', test_temp_dir.name,
                       "-t", test_temp_dir.name,
                       "-e", str(6),
                       ls_acc_ids[0]]

        _run_fasterq_dump_for_all(
            ls_acc_ids, test_temp_dir.name, threads=6,
            retries=0, logger=self.logger
        )

        mock_subprocess.assert_has_calls([call(exp_prefetch, text=True,
                                               capture_output=True),
                                          call(exp_fasterq, text=True,
                                               capture_output=True)])

    @patch('subprocess.run')
    def test_run_fasterq_dump_for_all_error(self, mock_subprocess):
        test_temp_dir = MockTempDir()
        ls_acc_ids = ['test_accERROR']

        with self.assertRaisesRegex(
                ValueError, 'could not be downloaded with'):
            _run_fasterq_dump_for_all(
                ls_acc_ids, test_temp_dir.name, threads=6,
                retries=1, logger=self.logger
            )
            # check retry procedure:
            self.assertEqual(mock_subprocess.call_count, 2)

    @patch('subprocess.run')
    def test_run_fasterq_dump_for_all_error_twoids(self, mock_subprocess):
        test_temp_dir = self.move_files_2_tmp_dir(['testaccA.fastq'])
        ls_acc_ids = ['test_accERROR', 'testaccA']

        with self.assertRaisesRegex(
                ValueError, 'could not be downloaded with'):
            _run_fasterq_dump_for_all(
                ls_acc_ids, test_temp_dir.name, threads=6,
                retries=1, logger=self.logger
            )
            # check retry procedure:
            self.assertEqual(mock_subprocess.call_count, 3)

    def test_process_downloaded_sequences(self):
        ls_fastq_files = ['testaccA.fastq',
                          'testacc_1.fastq', 'testacc_2.fastq']
        test_temp_dir = self.move_files_2_tmp_dir(ls_fastq_files)

        ls_act_single, ls_act_paired = _process_downloaded_sequences(
            test_temp_dir.name)

        ls_exp_single = ['testaccA_00_L001_R1_001.fastq']
        ls_exp_paired = ['testacc_00_L001_R1_001.fastq',
                         'testacc_00_L001_R2_001.fastq']

        self.assertEqual(set(ls_act_single), set(ls_exp_single))
        self.assertEqual(set(ls_act_paired), set(ls_exp_paired))

    def test_write_empty_casava_single(self):
        casava_out_single = CasavaOneEightSingleLanePerSampleDirFmt()
        empty_seq_type = 'single'
        with self.assertWarnsRegex(Warning,
                                   'No {}-read sequences'.format(
                                       empty_seq_type)):
            _write_empty_casava(empty_seq_type, casava_out_single, self.logger)
            exp_filename = 'xxx_00_L001_R1_001.fastq.gz'
            exp_casava_fpath = os.path.join(str(casava_out_single),
                                            exp_filename)
            self.assertTrue(os.path.isfile(exp_casava_fpath))

    def test_write_empty_casava_paired(self):
        casava_out_paired = CasavaOneEightSingleLanePerSampleDirFmt()
        empty_seq_type = 'paired'
        with self.assertWarnsRegex(Warning,
                                   'No {}-read sequences'.format(
                                       empty_seq_type)):
            _write_empty_casava(empty_seq_type, casava_out_paired, self.logger)

            for exp_filename in ['xxx_00_L001_R1_001.fastq.gz',
                                 'xxx_00_L001_R2_001.fastq.gz']:
                exp_casava_fpath = os.path.join(str(casava_out_paired),
                                                exp_filename)
                self.assertTrue(os.path.isfile(exp_casava_fpath))

    def test_write2casava_dir_single(self):
        casava_out_single = CasavaOneEightSingleLanePerSampleDirFmt()
        ls_file_single = ['testaccA_00_L001_R1_001.fastq']
        test_temp_dir = self.move_files_2_tmp_dir(ls_file_single)

        _write2casava_dir_single(test_temp_dir.name, casava_out_single,
                                 ls_file_single)
        exp_casava_fpath = os.path.join(str(casava_out_single),
                                        ls_file_single[0]+'.gz')
        self.assertTrue(os.path.isfile(exp_casava_fpath))

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

    def prepare_metadata(self, acc_id):
        acc_id_tsv = acc_id + '_md.tsv'
        _ = self.move_files_2_tmp_dir([acc_id_tsv])
        return Metadata.load(self.get_data_path(acc_id_tsv))

    @patch('subprocess.run')
    @patch('tempfile.TemporaryDirectory')
    def test_get_sequences_single_only(self, mock_tmpdir, mock_subprocess):
        acc_id = 'SRR123456'
        test_temp_dir = self.move_files_2_tmp_dir([acc_id + '.fastq'])
        mock_tmpdir.return_value = test_temp_dir

        test_temp_md = self.prepare_metadata(acc_id)

        with self.assertWarnsRegex(Warning, 'No paired-read sequences'):
            casava_single, casava_paired = get_sequences(
                test_temp_md, email='some@where.com', retries=0)
            self.assertIsInstance(casava_single,
                                  CasavaOneEightSingleLanePerSampleDirFmt)
            self.assertIsInstance(casava_paired,
                                  CasavaOneEightSingleLanePerSampleDirFmt)
            self.validate_counts(casava_single, casava_paired, [3], [0, 0])

    @patch('subprocess.run')
    @patch('tempfile.TemporaryDirectory')
    def test_get_sequences_paired_only(self, mock_tmpdir, mock_subprocess):
        acc_id = 'SRR123457'
        ls_file_names = [acc_id + '_1.fastq', acc_id + '_2.fastq']
        test_temp_dir = self.move_files_2_tmp_dir(ls_file_names)
        mock_tmpdir.return_value = test_temp_dir

        test_temp_md = self.prepare_metadata(acc_id)

        with self.assertWarnsRegex(Warning, 'No single-read sequences'):
            casava_single, casava_paired = get_sequences(
                test_temp_md, email='some@where.com', retries=0)
            self.assertIsInstance(casava_single,
                                  CasavaOneEightSingleLanePerSampleDirFmt)
            self.assertIsInstance(casava_paired,
                                  CasavaOneEightSingleLanePerSampleDirFmt)
            self.validate_counts(casava_single, casava_paired, [0], [3, 3])

    @patch('subprocess.run')
    @patch('tempfile.TemporaryDirectory')
    def test_get_sequences_single_n_paired(self, mock_tmpdir, mock_subprocess):
        ls_file_names = [
            'SRR123456.fastq', 'SRR123457_1.fastq', 'SRR123457_2.fastq']
        mock_tmpdir.return_value = self.move_files_2_tmp_dir(ls_file_names)

        test_temp_md = self.prepare_metadata('testaccBC')

        casava_single, casava_paired = get_sequences(
            test_temp_md, email='some@where.com', retries=0)
        self.assertIsInstance(casava_single,
                              CasavaOneEightSingleLanePerSampleDirFmt)
        self.assertIsInstance(casava_paired,
                              CasavaOneEightSingleLanePerSampleDirFmt)
        self.validate_counts(casava_single, casava_paired, [3], [3, 3])

    @patch('q2_fondue.sequences._run_fasterq_dump_for_all')
    @patch('q2_fondue.sequences._get_run_ids_from_projects',
           return_value=['SRR123456'])
    @patch('tempfile.TemporaryDirectory')
    def test_get_sequences_bioproject(self, mock_tmpdir, mock_get, mock_run):
        acc_id = 'SRR123456'
        proj_acc_id = 'PRJNA734376'
        test_temp_dir = self.move_files_2_tmp_dir([acc_id + '.fastq'])
        mock_tmpdir.return_value = test_temp_dir

        test_temp_md = self.prepare_metadata(proj_acc_id)

        _, _, = get_sequences(test_temp_md, email='some@where.com', retries=0)

        mock_get.assert_called_with(
            'some@where.com', 1, ['PRJNA734376'], 'INFO'
        )
        mock_run.assert_called_with(['SRR123456'], ANY, 1, 0, ANY)
