# ----------------------------------------------------------------------------
# Copyright (c) 2016-2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest
import pandas as pd
from unittest.mock import patch
from q2_fondue.tests.test_sequences import SequenceTests
from qiime2.plugins import fondue


class TestGetAll(SequenceTests):
    package = 'q2_fondue.tests'

    @patch('q2_fondue.metadata.es.Esearcher')
    @patch('q2_fondue.metadata._validate_esearch_result')
    @patch('q2_fondue.metadata.ef.Efetcher')
    @patch('q2_fondue.metadata._efetcher_inquire')
    @patch('subprocess.run')
    @patch('tempfile.TemporaryDirectory')
    def test_get_all_single(self, mock_tmpdir, mock_subprocess,
                            mock_inquire, mock_efetcher,
                            mock_validation, mock_esearcher):
        """
        Test verifying that pipeline get_all calls all expected actions,
        individual actions are tested in details in respective test classes
        """
        ls_accIDs = ['testaccB']

        # define mocked return values for get_metadata mocks
        mock_validation.return_value = True

        path2df = self.get_data_path('sra-metadata-mock.tsv')
        mock_inquire.return_value = pd.read_csv(path2df, sep='\t')

        # define mocked return values for get_sequences mocks
        mock_tmpdir.return_value = self.move_files_2_tmp_dir(
            [ls_accIDs[0] + '.fastq'])

        # run pipeline
        fondue.actions.get_all(
            ls_accIDs, 'fake@email.com', retries=1)

        # function call assertions for get_metadata within
        mock_esearcher.assert_called_once()
        mock_validation.assert_called_once()
        mock_efetcher.assert_called_once()
        mock_inquire.assert_called_once()

        # function call assertions for get_sequences within
        mock_subprocess.assert_called_once()


if __name__ == "__main__":
    unittest.main()
