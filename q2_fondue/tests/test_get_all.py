# ----------------------------------------------------------------------------
# Copyright (c) 2016-2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest
import pandas as pd
from unittest.mock import (patch, ANY)
from q2_fondue.tests.test_sequences import SequenceTests
from qiime2.metadata import Metadata
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
        str_mocked_email = 'fake@email.com'
        accID_tsv = 'testaccB_md.tsv'
        test_temp_md = Metadata.load(self.get_data_path(accID_tsv))

        # define mocked return values for get_metadata mocks
        mock_validation.return_value = True

        path2df = self.get_data_path('sra-metadata-mock.tsv')
        mock_inquire.return_value = pd.read_csv(path2df, sep='\t')

        # define mocked return values for get_sequences mocks
        mock_tmpdir.return_value = self.move_files_2_tmp_dir(
            [ls_accIDs[0] + '.fastq'])

        # run pipeline
        fondue.actions.get_all(
            test_temp_md, str_mocked_email, retries=1)

        # function call assertions for get_metadata within
        mock_esearcher.assert_called_once_with('esearcher',
                                               str_mocked_email,
                                               apikey=None, apikey_var=None,
                                               threads=1, qid=None)
        mock_validation.assert_called_once_with(ANY, ls_accIDs)
        mock_efetcher.assert_called_once_with('efetcher',
                                              str_mocked_email,
                                              apikey=None, apikey_var=None,
                                              threads=1, qid=None)
        mock_inquire.assert_called_once_with(ANY, ls_accIDs)

        # function call assertions for get_sequences within
        mock_subprocess.assert_called_once_with(['fasterq-dump',
                                                 '-O', ANY,
                                                 '-t', ANY,
                                                 '-e', '6',
                                                 ls_accIDs[0]],
                                                text=True,
                                                capture_output=True)


if __name__ == "__main__":
    unittest.main()
