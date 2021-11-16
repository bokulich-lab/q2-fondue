# ----------------------------------------------------------------------------
# Copyright (c) 2016-2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest
import pandas as pd
from unittest.mock import (patch, call, ANY)
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
            [f'{acc_id}.fastq'])

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
        # todo: rethink below quick fix
        mock_subprocess.assert_has_calls([
            call(['prefetch', '-O', ANY, acc_id],
                 text=True, capture_output=True),
            call(['prefetch', '-O', ANY, acc_id],
                 text=True, capture_output=True)
        ])


if __name__ == "__main__":
    unittest.main()
