# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import os

import pandas as pd
import unittest
from q2_types.per_sample_sequences import \
    CasavaOneEightSingleLanePerSampleDirFmt
from qiime2 import Artifact
from qiime2.plugins import fondue
from unittest.mock import (patch, ANY, call)

from q2_fondue.sequences import (_run_fasterq_dump_for_all,
                                 _process_downloaded_sequences,
                                 _write2casava_dir, _copy_single_to_casava)
from q2_fondue.tests.test_sequences import SequenceTests


class TestGetAll(SequenceTests):
    package = 'q2_fondue.tests'

    @patch('q2_fondue.metadata.es.Esearcher')
    @patch('q2_fondue.metadata._validate_esearch_result')
    @patch('q2_fondue.metadata.ef.Efetcher')
    @patch('q2_fondue.metadata._efetcher_inquire')
    @patch('time.sleep')
    @patch('q2_fondue.sequences.Process')
    @patch('q2_fondue.sequences.Pool')
    @patch('q2_fondue.sequences._announce_completion')
    @patch('q2_fondue.sequences.CasavaOneEightSingleLanePerSampleDirFmt')
    @patch('tempfile.TemporaryDirectory')
    def test_get_all_single(
            self, mock_tmpdir, mock_casava, mock_announce, mock_pool,
            mock_proc, mock_sleep, mock_inquire, mock_efetcher,
            mock_validation, mock_esearcher
    ):
        """
        Test verifying that pipeline get_all calls all expected actions,
        individual actions are tested in details in respective test classes
        """
        acc_id = 'SRR123456'
        test_md = Artifact.import_data(
            'NCBIAccessionIDs', self.get_data_path(f'{acc_id}_md.tsv')
        )

        # define mocked return values for get_metadata mocks
        mock_validation.return_value = {}

        path2df = self.get_data_path('sra-metadata-mock.tsv')
        mock_inquire.return_value = (
            pd.read_csv(path2df, sep='\t', index_col=0), []
        )

        # define mocked return values for get_sequences mocks
        mock_tmpdir.return_value = self.move_files_2_tmp_dir(
            [f'{acc_id}.fastq', f'{acc_id}.sra'])
        mock_announce.return_value = {}, [f'{acc_id}.fastq'], []
        casavas = [CasavaOneEightSingleLanePerSampleDirFmt(),
                   CasavaOneEightSingleLanePerSampleDirFmt()]
        mock_casava.side_effect = casavas
        _copy_single_to_casava(
            f'{acc_id}.fastq', mock_tmpdir.return_value.name,
            str(casavas[0].path)
        )
        os.rename(
            os.path.join(str(casavas[0].path),
                         f'{acc_id}.fastq.gz'),
            os.path.join(str(casavas[0].path),
                         f'{acc_id}_00_L001_R1_001.fastq.gz'),
        )

        # run pipeline
        fondue.actions.get_all(test_md, 'fake@email.com', retries=1)

        # function call assertions for get_metadata within
        mock_esearcher.assert_called_once_with(
            'esearcher', 'fake@email.com', apikey=None, apikey_var=None,
            threads=1, qid=None)
        mock_validation.assert_called_once_with(ANY, [acc_id], 'INFO')
        mock_efetcher.assert_called_once_with(
            'efetcher', 'fake@email.com', apikey=None, apikey_var=None,
            threads=1, qid=None)
        mock_inquire.assert_called_once_with(ANY, [acc_id], 'INFO')

        # function call assertions for get_sequences within
        mock_proc.assert_has_calls([
            call(target=_run_fasterq_dump_for_all, args=(
                [acc_id], mock_tmpdir.return_value.name, 1, 1,
                ANY, ANY), daemon=True),
            call(target=_process_downloaded_sequences, args=(
                mock_tmpdir.return_value.name, ANY, ANY, 1), daemon=True),
        ])
        mock_pool.assert_called_once_with(
            1, _write2casava_dir,
            (mock_tmpdir.return_value.name, ANY, ANY, ANY, ANY)
        )

    @patch('q2_fondue.metadata.es.Esearcher')
    @patch('q2_fondue.metadata._validate_esearch_result')
    @patch('q2_fondue.metadata.ef.Efetcher')
    @patch('q2_fondue.metadata._efetcher_inquire')
    @patch('time.sleep')
    @patch('q2_fondue.sequences.Process')
    @patch('q2_fondue.sequences.Pool')
    @patch('q2_fondue.sequences._announce_completion')
    @patch('q2_fondue.sequences.CasavaOneEightSingleLanePerSampleDirFmt')
    @patch('tempfile.TemporaryDirectory')
    def test_get_all_multi_with_errors(
            self, mock_tmpdir, mock_casava, mock_announce, mock_pool,
            mock_proc, mock_sleep, mock_inquire, mock_efetcher,
            mock_validation, mock_esearcher
    ):
        """
        Test verifying that pipeline get_all calls all expected actions,
        individual actions are tested in details in respective test classes
        """
        acc_ids = ['SRR123456', 'SRR123457']
        test_md = Artifact.import_data(
            'NCBIAccessionIDs', self.get_data_path('SRR1234567_md.tsv')
        )

        # define mocked return values for get_metadata mocks
        mock_validation.return_value = {'SRR123457': 'ID is invalid.'}

        path2df = self.get_data_path('sra-metadata-mock.tsv')
        mock_inquire.return_value = (
            pd.read_csv(path2df, sep='\t', index_col=0), []
        )

        # define mocked return values for get_sequences mocks
        mock_tmpdir.return_value = self.move_files_2_tmp_dir(
            [f'{acc_ids[0]}.fastq', f'{acc_ids[0]}.sra']
        )
        mock_announce.return_value = {}, [f'{acc_ids[0]}.fastq'], []
        casavas = [CasavaOneEightSingleLanePerSampleDirFmt(),
                   CasavaOneEightSingleLanePerSampleDirFmt()]
        mock_casava.side_effect = casavas
        _copy_single_to_casava(
            f'{acc_ids[0]}.fastq', mock_tmpdir.return_value.name,
            str(casavas[0].path)
        )
        os.rename(
            os.path.join(str(casavas[0].path),
                         f'{acc_ids[0]}.fastq.gz'),
            os.path.join(str(casavas[0].path),
                         f'{acc_ids[0]}_00_L001_R1_001.fastq.gz'),
        )

        # run pipeline
        fondue.actions.get_all(test_md, 'fake@email.com', retries=0)

        # function call assertions for get_metadata within
        mock_esearcher.assert_called_once_with(
            'esearcher', 'fake@email.com', apikey=None, apikey_var=None,
            threads=1, qid=None)
        mock_validation.assert_called_once_with(ANY, acc_ids, 'INFO')
        mock_efetcher.assert_called_once_with(
            'efetcher', 'fake@email.com', apikey=None, apikey_var=None,
            threads=1, qid=None)
        mock_inquire.assert_called_once_with(ANY, [acc_ids[0]], 'INFO')

        # function call assertions for get_sequences within
        mock_proc.assert_has_calls([
            call(target=_run_fasterq_dump_for_all, args=(
                [acc_ids[0]], mock_tmpdir.return_value.name, 1, 0,
                ANY, ANY), daemon=True),
            call(target=_process_downloaded_sequences, args=(
                mock_tmpdir.return_value.name, ANY, ANY, 1), daemon=True),
        ])
        mock_pool.assert_called_once_with(
            1, _write2casava_dir,
            (mock_tmpdir.return_value.name, ANY, ANY, ANY, ANY)
        )


if __name__ == "__main__":
    unittest.main()
