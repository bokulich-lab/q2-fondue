# ----------------------------------------------------------------------------
# Copyright (c) 2022, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest
import pandas as pd
from unittest.mock import (patch, call, ANY, MagicMock)
from q2_fondue.tests.test_sequences import SequenceTests
from qiime2.metadata import Metadata
from qiime2.plugins import fondue


class TestGetAll(SequenceTests):
    package = 'q2_fondue.tests'

    @patch('q2_fondue.metadata.es.Esearcher')
    @patch('q2_fondue.metadata._validate_esearch_result')
    @patch('q2_fondue.metadata.ef.Efetcher')
    @patch('q2_fondue.metadata._efetcher_inquire')
    @patch('time.sleep')
    @patch('subprocess.run')
    @patch('tempfile.TemporaryDirectory')
    def test_get_all_single(self, mock_tmpdir, mock_subprocess, mock_sleep,
                            mock_inquire, mock_efetcher,
                            mock_validation, mock_esearcher):
        """
        Test verifying that pipeline get_all calls all expected actions,
        individual actions are tested in details in respective test classes
        """
        acc_id = 'SRR123456'
        test_md = Metadata.load(self.get_data_path(f'{acc_id}_md.tsv'))

        # define mocked return values for get_metadata mocks
        mock_validation.return_value = True

        path2df = self.get_data_path('sra-metadata-mock.tsv')
        mock_inquire.return_value = (
            pd.read_csv(path2df, sep='\t', index_col=0), []
        )

        # define mocked return values for get_sequences mocks
        mock_tmpdir.return_value = self.move_files_2_tmp_dir(
            [f'{acc_id}.fastq', f'{acc_id}.sra'])
        mock_subprocess.return_value = MagicMock(returncode=0)

        # run pipeline
        fondue.actions.get_all(test_md, 'fake@email.com', retries=1)

        # function call assertions for get_metadata within
        mock_esearcher.assert_called_once_with(
            'esearcher', 'fake@email.com', apikey=None, apikey_var=None,
            threads=1, qid=None)
        mock_validation.assert_called_once_with(ANY, [acc_id])
        mock_efetcher.assert_called_once_with(
            'efetcher', 'fake@email.com', apikey=None, apikey_var=None,
            threads=1, qid=None)
        mock_inquire.assert_called_once_with(ANY, [acc_id], 'INFO')

        # function call assertions for get_sequences within
        mock_subprocess.assert_has_calls([
            call(['prefetch', '-O', acc_id,  acc_id],
                 text=True, capture_output=True, cwd=ANY),
            call(['fasterq-dump', '-e', '1', acc_id],
                 text=True, capture_output=True, cwd=ANY)
        ])


if __name__ == "__main__":
    unittest.main()
