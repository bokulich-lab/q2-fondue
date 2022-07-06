# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import entrezpy.efetch.efetcher as ef
import entrezpy.esearch.esearcher as es
import pandas as pd
import numpy as np
import unittest
from parameterized import parameterized
from entrezpy import conduit
from entrezpy.esearch import esearcher
from entrezpy.efetch import efetcher
from entrezpy.requester.requester import Requester
from pandas._testing import assert_frame_equal, assert_series_equal
from numpy.testing import assert_array_equal
from qiime2.metadata import Metadata
from unittest.mock import patch, MagicMock, ANY, call

from q2_fondue.entrezpy_clients import _esearch
from q2_fondue.entrezpy_clients._efetch import EFetchAnalyzer
from q2_fondue.entrezpy_clients._utils import InvalidIDs
from q2_fondue.metadata import (
    _efetcher_inquire, _get_other_meta,
    get_metadata, _get_run_meta, merge_metadata,
    _find_doi_mapping_and_type, _execute_efetcher
)
from q2_fondue.tests._utils import _TestPluginWithEntrezFakeComponents
from q2_fondue.utils import (
    _validate_run_ids, _determine_id_type
)


class FakeConduit:
    def __init__(self, fake_efetch_result, fake_efetch_response):
        self.logger = MagicMock()
        self.fake_efetch_result = fake_efetch_result
        self.fake_efetch_response = fake_efetch_response
        self.pipeline = MagicMock()

    def new_pipeline(self):
        self.pipeline.add_search = MagicMock(return_value='fake_search')
        self.pipeline.add_link = MagicMock(return_value='fake_link')
        self.pipeline.add_fetch = MagicMock(return_value='fake_fetch')
        return self.pipeline

    def run(self, pipeline):
        analyzer = EFetchAnalyzer('INFO')
        analyzer.result = self.fake_efetch_result
        analyzer.result.extract_run_ids(self.fake_efetch_response)
        return analyzer


class FakeAnalyzerValidation():
    def __init__(self, ids, counts):
        self.log_level = 'INFO'
        self.result = MagicMock()
        self.result.validate_result = MagicMock()
        self.result.result = pd.Series(
            data=counts, index=ids)


class TestMetadataFetching(_TestPluginWithEntrezFakeComponents):
    package = 'q2_fondue.tests'

    def setUp(self):
        super().setUp()
        self.fake_efetcher = ef.Efetcher(
            'fake_efetcher', 'fake@email.com', apikey=None,
            apikey_var=None, threads=1, qid=None
        )
        self.fake_esearcher = es.Esearcher(
            'fake_esearcher', 'fake@email.com', apikey=None,
            apikey_var=None, threads=1, qid=None
        )
        self.fake_econduit = FakeConduit(
            self.generate_ef_result(kind='runs', prefix='efetch'),
            self.xml_to_response('runs', prefix='efetch'))

    def generate_meta_df(self, obs_suffices, exp_suffix):
        meta_dfs = []
        for s in obs_suffices:
            meta_dfs.append(pd.read_csv(
                self.get_data_path(f'sra-metadata-{s}.tsv'),
                sep='\t', index_col=0
            ))
        exp_df = pd.read_csv(
            self.get_data_path(f'sra-metadata-{exp_suffix}.tsv'),
            sep='\t', index_col=0
        )
        return meta_dfs, exp_df

    @parameterized.expand([
        ("run", ['SRR123', 'ERR123']),
        ("study", ['ERP12345', 'SRP23456']),
        ("bioproject", ['PRJNA123', 'PRJNA123']),
        ("experiment", ['ERX123', 'SRX123']),
        ("sample", ['ERS123', 'SRS123'])
    ])
    def test_determine_id_type(self, id_type, acc_ids):
        obs = _determine_id_type(acc_ids)
        self.assertEqual(id_type, obs)

    def test_determine_id_type_mixed(self):
        ids = ['SRS123', 'ERR123']

        with self.assertRaisesRegexp(
                InvalidIDs, 'type of provided IDs is either not supported'):
            _determine_id_type(ids)

    def test_determine_id_type_unknown(self):
        ids = ['ABC123', 'DEF123']

        with self.assertRaisesRegexp(
                InvalidIDs, 'type of provided IDs is either not supported'):
            _determine_id_type(ids)

    def test_efetcher_inquire_single(self):
        with patch.object(Requester, 'request') as mock_request:
            mock_request.return_value = self.xml_to_response('single')
            obs_df, _ = _efetcher_inquire(
                self.fake_efetcher, ['FAKEID1'], 'INFO'
            )
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_ef_request(['FAKEID1'])
        exp_df = self.generate_expected_df().iloc[[0]]

        for arg in self.efetch_request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        pd.testing.assert_frame_equal(
            exp_df.sort_index(axis=1), obs_df.sort_index(axis=1))

    def test_efetcher_inquire_multi(self):
        with patch.object(Requester, 'request') as mock_request:
            mock_request.return_value = self.xml_to_response('multi')
            obs_df, _ = _efetcher_inquire(
                self.fake_efetcher, ['FAKEID1', 'FAKEID2'], 'INFO'
            )
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_ef_request(
            ['FAKEID1', 'FAKEID2'], size=2)
        exp_df = self.generate_expected_df()

        for arg in self.efetch_request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        pd.testing.assert_frame_equal(
            exp_df.sort_index(axis=1), obs_df.sort_index(axis=1))

    @patch('q2_fondue.metadata.BATCH_SIZE', 2)
    @patch.object(efetcher, 'Efetcher')
    @patch('q2_fondue.metadata._efetcher_inquire')
    def test_execute_efetcher_batchsize(self, patch_efetch_iq, patch_ef):
        patch_efetch_iq.side_effect = [(pd.DataFrame(), {}),
                                       (pd.DataFrame(), {})]
        _, _ = _execute_efetcher('someone@somewhere.com', 1,
                                 ['Valid1', 'Valid2', 'Valid3'],
                                 'INFO', self.fake_logger)
        patch_efetch_iq.assert_has_calls([
            call(ANY, ['Valid1', 'Valid2'], 'INFO'),
            call(ANY, ['Valid3'], 'INFO')
        ])
        self.assertEqual(patch_efetch_iq.call_count, 2)

    def test_efetcher_inquire_error(self):
        with patch.object(Requester, 'request') as mock_response:
            mock_response.return_value = self.xml_to_response('error')
            obs_df, obs_missing = _efetcher_inquire(
                self.fake_efetcher, ['FAKEID1'], 'INFO'
            )
            exp_missing = {
                'FAKEID1': '<?xml version="1.0"  ?>\n<ERROR>\n</ERROR>\n'}
            self.assertDictEqual(obs_missing, exp_missing)

    @patch('entrezpy.esearch.esearcher.Esearcher')
    def test_esearcher_inquire_single(self, mock_search):
        with patch.object(Requester, 'request') as mock_request:
            mock_search.return_value = self.fake_esearcher
            mock_request.return_value = self.json_to_response(
                'single', '_correct', True)
            obs_result = _validate_run_ids(
                'someone@somewhere.com', 1, ['SRR000001'], 'INFO'
            )
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_es_request('SRR000001')

        for arg in self.esearch_request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        self.assertDictEqual(obs_result, {})

    @patch('entrezpy.esearch.esearcher.Esearcher')
    def test_esearcher_inquire_multi(self, mock_search):
        with patch.object(Requester, 'request') as mock_request:
            mock_search.return_value = self.fake_esearcher
            mock_request.return_value = self.json_to_response(
                'multi', '_correct', True)
            obs_result = _validate_run_ids(
                'someone@somewhere.com', 1,
                ['SRR000001', 'SRR000013', 'ERR3978173'],
                'INFO'
            )
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_es_request(
            'SRR000001 OR SRR000013 OR ERR3978173')

        for arg in self.esearch_request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        self.assertDictEqual(obs_result, {})

    @patch('entrezpy.esearch.esearcher.Esearcher')
    @patch.object(_esearch, 'ESearchAnalyzer')
    def test_validate_run_ids_one_batch(
            self, mock_analyzer, mock_search):
        ids = ['SRR000001', 'SRR000013', 'ERR3978173']
        mock_analyzer.return_value = FakeAnalyzerValidation(
            ids, [1, 1, 1])
        mock_search.return_value.inquire = mock_analyzer

        obs_invalid = _validate_run_ids(
            'someone@somewhere.com', 1, ids, 'INFO')

        self.assertEqual(
            mock_analyzer.return_value.result.validate_result.call_count,
            1)
        self.assertDictEqual(obs_invalid, {})

    @patch('entrezpy.esearch.esearcher.Esearcher')
    @patch.object(_esearch, 'ESearchAnalyzer')
    @patch('q2_fondue.utils._chunker')
    def test_validate_run_ids_multiple_batches(
            self, mock_chunker, mock_analyzer, mock_search):
        ids = ['SRR000001', 'SRR000013', 'ERR3978173']
        mock_chunker.return_value = (
            ['SRR000001', 'SRR000013'], ['ERR3978173'])

        first_analyzer = FakeAnalyzerValidation(
            ['SRR000001', 'SRR000013'], [1, 1])
        second_analyzer = FakeAnalyzerValidation(
            ['ERR3978173'], [1])
        mock_analyzer.side_effect = [first_analyzer, second_analyzer]
        mock_search.return_value.inquire = mock_analyzer

        obs_invalid = _validate_run_ids(
            'someone@somewhere.com', 1, ids, 'INFO')

        self.assertEqual(
            first_analyzer.result.validate_result.call_count, 1)
        self.assertEqual(
            second_analyzer.result.validate_result.call_count, 1)
        self.assertDictEqual(obs_invalid, {})

    def test_find_all_run_ids(self):
        fake_results = [
            {'RUN_SET': {'RUN': {'@accession': 'abc123', '@alias': 'run123'}}},
            {'RUN_SET': {'RUN': {'@accession': 'cde234', '@alias': 'run124'}}}
        ]
        obs_map = self.efetch_result_single._find_all_run_ids(fake_results)
        exp_map = {'abc123': 0, 'cde234': 1}
        self.assertDictEqual(exp_map, obs_map)

    def test_find_all_run_ids_big_runset(self):
        fake_results = [
            {'RUN_SET': [{'RUN': {'@accession': 'ab12', '@alias': 'run123'}},
                         {'RUN': {'@accession': 'bc23', '@alias': 'run124'}}]},
            {'RUN_SET': {'RUN': {'@accession': 'de34', '@alias': 'run125'}}}
        ]
        obs_map = self.efetch_result_single._find_all_run_ids(fake_results)
        exp_map = {'ab12': 0, 'bc23': 0, 'de34': 1}
        self.assertDictEqual(exp_map, obs_map)

    def test_find_all_run_ids_big_runset_big_run(self):
        fake_results = [
            {'RUN_SET': [
                {'RUN': [
                    {'@accession': 'ab12', '@alias': 'run123'},
                    {'@accession': 'bc23', '@alias': 'run124'}
                ]},
                {'RUN': [
                    {'@accession': 'cd34', '@alias': 'run125'},
                    {'@accession': 'de45', '@alias': 'run126'}
                ]}]},
            {'RUN_SET': {'RUN': {'@accession': 'ef56', '@alias': 'run127'}}}
        ]

        obs_map = self.efetch_result_single._find_all_run_ids(fake_results)
        exp_map = {'ab12': 0, 'bc23': 0, 'cd34': 0, 'de45': 0, 'ef56': 1}
        self.assertDictEqual(exp_map, obs_map)

    @patch('q2_fondue.metadata._validate_run_ids', return_value={})
    @patch('q2_fondue.metadata._execute_efetcher')
    def test_get_run_meta(self, patch_ef, patch_val):
        exp_df = pd.DataFrame(
            {'meta1': [1, 2, 3], 'meta2': ['a', 'b', 'c']},
            index=['AB', 'cd', 'Ef']
        )
        patch_ef.return_value = (exp_df, [])
        obs_df, obs_dict = _get_run_meta(
            'someone@somewhere.com', 1, ['AB', 'cd', 'Ef'],
            'INFO', self.fake_logger
        )

        assert_frame_equal(exp_df, obs_df)
        patch_val.assert_called_once_with(
            'someone@somewhere.com', 1, ['AB', 'cd', 'Ef'], 'INFO')
        patch_ef.assert_called_once_with(
            'someone@somewhere.com', 1, ['AB', 'Ef', 'cd'], 'INFO',
            self.fake_logger
        )

    @patch('q2_fondue.metadata._validate_run_ids', return_value={})
    @patch('q2_fondue.metadata._execute_efetcher')
    def test_get_run_meta_missing_ids(self, patch_ef, patch_val):
        exp_meta = pd.DataFrame(
            {'meta1': [1, 2], 'meta2': ['a', 'b']},
            index=['AB', 'cd']
        )
        exp_missing = {'Ef': 'Fake error'}
        patch_ef.return_value = (exp_meta.iloc[:2, :], exp_missing)

        with self.assertLogs('test_log', level='WARNING') as cm:
            obs_df, missing_dict = _get_run_meta(
                'someone@somewhere.com', 1, ['AB', 'cd', 'Ef'],
                'INFO', self.fake_logger
            )

            assert_frame_equal(exp_meta, obs_df)
            self.assertDictEqual(missing_dict, exp_missing)
            self.assertEqual(
                cm.output,
                ['WARNING:test_log:Metadata for the following run IDs '
                 'could not be fetched: Ef. Please try fetching those '
                 'independently.']
            )

            patch_val.assert_called_once_with(
                'someone@somewhere.com', 1, ['AB', 'cd', 'Ef'], 'INFO')
            patch_ef.assert_called_once_with(
                'someone@somewhere.com', 1, ['AB', 'Ef', 'cd'], 'INFO',
                self.fake_logger
            )

    @patch.object(esearcher, 'Esearcher')
    @patch('q2_fondue.metadata._validate_run_ids')
    @patch('q2_fondue.metadata._execute_efetcher')
    def test_get_run_meta_no_valid_ids(self, patch_ef, patch_val, patch_es):
        patch_val.return_value = {
            'AB': 'ID is invalid.', 'cd': 'ID is ambiguous.'
        }

        with self.assertRaisesRegexp(
                InvalidIDs, 'All provided IDs were invalid.'
        ):
            _ = _get_run_meta(
                'someone@somewhere.com', 1, ['AB', 'cd'],
                'INFO', self.fake_logger
            )

    @patch.object(esearcher, 'Esearcher')
    @patch('q2_fondue.metadata._validate_run_ids')
    @patch('q2_fondue.metadata._execute_efetcher')
    def test_get_run_meta_one_invalid_id(self, patch_ef, patch_val, patch_es):
        patch_val.return_value = {
            'AB': 'ID is invalid.'
        }
        exp_df = pd.DataFrame(
            {'meta1': [1], 'meta2': ['a']},
            index=['AA']
        )
        patch_ef.return_value = (exp_df, [])

        with self.assertLogs('test_log', level='WARNING') as cm:
            _ = _get_run_meta(
                'someone@somewhere.com', 1, ['AA', 'AB'],
                'INFO', self.fake_logger
            )
            self.assertIn(
                'WARNING:test_log:The following provided IDs are invalid: AB. '
                'Please correct them and try fetching those independently.',
                cm.output
            )

    @parameterized.expand([
        ("study", "sra"),
        ("bioproject", "bioproject"),
        ("experiment", "sra"),
        ("sample", "sra")
    ])
    @patch('q2_fondue.entrezpy_clients._pipelines.get_run_id_count')
    @patch('q2_fondue.metadata._get_run_meta')
    def test_get_other_meta_different_ids(
            self, id_type, db2search, mock_get, mock_count):
        exp_ids = [
            'SRR000007', 'SRR000018', 'SRR000020', 'SRR000038',
            'SRR000043', 'SRR000046', 'SRR000048', 'SRR000050',
            'SRR000057', 'SRR000058', 'SRR13961759', 'SRR13961771']
        with patch.object(conduit, 'Conduit') as mock_conduit:
            mock_count.return_value = 12
            mock_conduit.return_value = self.fake_econduit
            mock_get.return_value = exp_ids

            _ = _get_other_meta(
                'someone@somewhere.com', 1, ['AB', 'cd'], id_type,
                'INFO', MagicMock()
            )

            self.fake_econduit.pipeline.add_search.assert_called_once_with(
                {'db': db2search, 'term': "AB OR cd"}, analyzer=ANY
            )
            self.fake_econduit.pipeline.add_fetch.assert_called_once_with(
                {'rettype': 'docsum', 'retmode': 'xml', 'retmax': 10000},
                analyzer=ANY, dependency=ANY
            )
            mock_get.assert_called_once_with(
                'someone@somewhere.com', 1, exp_ids, 'INFO', ANY
            )

    @patch('q2_fondue.entrezpy_clients._pipelines.get_run_id_count')
    @patch('q2_fondue.metadata._get_run_meta')
    def test_get_other_meta_large_retmax(self, mock_get, mock_count):
        exp_ids = [
            'SRR000007', 'SRR000018', 'SRR000020', 'SRR000038',
            'SRR000043', 'SRR000046', 'SRR000048', 'SRR000050',
            'SRR000057', 'SRR000058', 'SRR13961759', 'SRR13961771']
        with patch.object(conduit, 'Conduit') as mock_conduit:
            mock_count.return_value = 234000
            mock_conduit.return_value = self.fake_econduit
            mock_get.return_value = exp_ids

            _ = _get_other_meta(
                'someone@somewhere.com', 1, ['AB', 'cd'], 'bioproject',
                'INFO', MagicMock()
            )

            self.fake_econduit.pipeline.add_fetch.assert_called_once_with(
                {'rettype': 'docsum', 'retmode': 'xml', 'retmax': 240000},
                analyzer=ANY, dependency=ANY
            )

    @patch('q2_fondue.metadata._get_run_meta')
    @patch('q2_fondue.metadata._get_other_meta')
    def test_get_metadata_run(
            self, patched_get_other, patched_get_run):
        patched_get_run.return_value = (pd.DataFrame(), {})
        ids_meta = Metadata.load(self.get_data_path('run_ids.tsv'))
        _ = get_metadata(ids_meta, 'abc@def.com', 2)

        patched_get_run.assert_called_once_with(
            'abc@def.com', 2, ['SRR123', 'SRR234', 'SRR345'],
            'INFO', ANY
        )
        patched_get_other.assert_not_called()

    @parameterized.expand([
        ("study", ['ERP12345', 'SRP23456']),
        ("bioproject", ['PRJNA123', 'PRJNA234']),
        ("experiment", ['ERX115020', 'SRX10331465']),
        ("sample", ['ERS147978', 'ERS3588233'])
    ])
    @patch('q2_fondue.metadata._get_run_meta')
    @patch('q2_fondue.metadata._get_other_meta')
    def test_get_metadata_other(
            self, id_type, acc_ids, patched_get_other, patched_get_run):
        patched_get_other.return_value = (pd.DataFrame(), {})
        ids_meta = Metadata.load(self.get_data_path(f'{id_type}_ids.tsv'))
        _ = get_metadata(ids_meta, 'abc@def.com', 2)

        patched_get_other.assert_called_once_with(
            'abc@def.com', 2, acc_ids, id_type,
            'INFO', ANY
        )
        patched_get_run.assert_not_called()

    @parameterized.expand([
        "run",
        "study",
        "bioproject",
        "experiment",
        "sample"
        ])
    def test_find_doi_mapping_and_type(self, id_type):
        map_ids_doi = Metadata.load(self.get_data_path(
            f'{id_type}_ids_w_doi.tsv'))

        obs_map, obs_type = _find_doi_mapping_and_type(map_ids_doi)
        self.assertEqual(obs_type, id_type)
        assert_series_equal(obs_map, map_ids_doi.to_dataframe()['DOI'])

    @patch('q2_fondue.metadata._get_run_meta')
    @patch('q2_fondue.metadata._get_other_meta')
    def test_get_metadata_run_w_doi(
            self, patched_get_other_study, patched_get_run):
        meta_df = pd.read_csv(self.get_data_path('sra-metadata-1.tsv'),
                              sep='\t', index_col=0)
        patched_get_run.return_value = (meta_df, {})
        ids_meta = Metadata.load(self.get_data_path('run_ids_w_doi.tsv'))

        obs_meta, _ = get_metadata(ids_meta, 'abc@def.com', 1)
        self.assertTrue('DOI' in obs_meta.columns)
        assert_frame_equal(obs_meta[['DOI']], ids_meta.to_dataframe())

    @parameterized.expand([
        ("study", "Study ID"),
        ("bioproject", "Bioproject ID"),
        ("experiment", "Experiment ID"),
        ("sample", "Sample Accession")
    ])
    @patch('q2_fondue.metadata._get_run_meta')
    @patch('q2_fondue.metadata._get_other_meta')
    def test_get_metadata_other_w_doi(
            self, id_type, match_id, patched_get_other_study, patched_get_run):
        meta_df = pd.read_csv(self.get_data_path('sra-metadata-1.tsv'),
                              sep='\t', index_col=0)
        patched_get_other_study.return_value = (meta_df, {})
        ids_meta = Metadata.load(self.get_data_path(
            f'{id_type}_ids_w_doi.tsv'))

        obs_meta, _ = get_metadata(ids_meta, 'abc@def.com', 1)
        self.assertTrue('DOI' in obs_meta.columns)
        for row in range(0, obs_meta.shape[0]):
            assert_array_equal(
                obs_meta[[match_id, 'DOI']].values[row],
                ids_meta.to_dataframe().reset_index().values[0])

    @patch('q2_fondue.metadata._get_run_meta')
    def test_get_metadata_missing_ids_refetch_w_doi_run(
            self, patched_get_run):
        meta_df = pd.read_csv(self.get_data_path(
            'sra-metadata-failed-ids.tsv'), sep='\t', index_col=0)
        patched_get_run.return_value = (meta_df, {})

        failed_ids = Metadata.load(self.get_data_path(
            'failed_ids_no_doi.tsv'))
        ids_doi = Metadata.load(self.get_data_path(
            'run_ids_w_doi_2.tsv'))

        obs_meta, _ = get_metadata(
            failed_ids, 'abc@def.com', linked_doi_names=ids_doi)
        self.assertTrue('DOI' in obs_meta.columns)
        assert_frame_equal(obs_meta[['DOI']], ids_doi.to_dataframe())

    @parameterized.expand([
        ("study", "Study ID"),
        ("bioproject", "Bioproject ID"),
        ("experiment", "Experiment ID"),
        ("sample", "Sample Accession")
    ])
    @patch('q2_fondue.metadata._get_run_meta')
    def test_get_metadata_missing_ids_refetch_w_doi_other(
            self, id_type, match_id, patched_get_run):
        meta_df = pd.read_csv(self.get_data_path(
            'sra-metadata-failed-ids.tsv'), sep='\t', index_col=0)
        patched_get_run.return_value = (meta_df, {})

        failed_ids = Metadata.load(self.get_data_path(
            'failed_ids_no_doi.tsv'))
        ids_doi = Metadata.load(self.get_data_path(
            f'{id_type}_ids_w_doi.tsv'))

        obs_meta, _ = get_metadata(
            failed_ids, 'abc@def.com', linked_doi_names=ids_doi)
        self.assertTrue('DOI' in obs_meta.columns)
        assert_array_equal(
            np.unique(obs_meta[[match_id, 'DOI']].values),
            ids_doi.to_dataframe().reset_index().values[0]
        )

    def test_merge_metadata_3dfs_nodupl(self):
        # Test merging metadata with no duplicated run IDs
        meta, exp_df = self.generate_meta_df(range(1, 4), 'exp-1')
        obs_df = merge_metadata(meta)
        assert_frame_equal(obs_df, exp_df)

    def test_merge_metadata_2dfs_dupl_ids(self):
        # Test merging metadata with duplicated run IDs
        meta, exp_df = self.generate_meta_df([1, 1], '1')
        with self.assertLogs('q2_fondue.metadata', level='INFO') as cm:
            obs_df = merge_metadata(meta)
            self.assertIn(
                'INFO:q2_fondue.metadata:2 duplicate record(s) found in '
                'the metadata were dropped.', cm.output
            )
            assert_frame_equal(obs_df, exp_df)

    def test_merge_metadata_2dfs_diff_cols(self):
        # Test merging metadata with different sets of columns
        meta, exp_df = self.generate_meta_df([1, 4], 'exp-2')
        obs_df = merge_metadata(meta)
        assert_frame_equal(obs_df, exp_df)

    def test_merge_metadata_2dfs_overlap_cols(self):
        # Test merging metadata with some overlapping columns
        meta, exp_df = self.generate_meta_df([5, 6], 'exp-3')
        obs_df = merge_metadata(meta)
        assert_frame_equal(obs_df, exp_df)

    def test_merge_metadata_2dfs_overlap_cols_rows(self):
        # Test merging metadata with some overlapping columns
        # and some overlapping run IDs
        meta, exp_df = self.generate_meta_df([5, 7], 'exp-4')
        with self.assertLogs('q2_fondue.metadata', level='INFO') as cm:
            obs_df = merge_metadata(meta)
            self.assertIn(
                'INFO:q2_fondue.metadata:1 duplicate record(s) found in '
                'the metadata were dropped.', cm.output
            )
            assert_frame_equal(obs_df, exp_df)

    def test_merge_metadata_2dfs_overlap_cols_rows_keep_dupl(self):
        # Test merging metadata with some overlapping columns
        # and some overlapping run IDs where duplicates have
        # differing values
        meta, exp_df = self.generate_meta_df([5, 8], 'exp-5')
        with self.assertLogs('q2_fondue.metadata', level='WARNING') as cm:
            obs_df = merge_metadata(meta)
            self.assertIn(
                'WARNING:q2_fondue.metadata:Records with same IDs '
                'but differing values were found in the metadata '
                'and will not be removed.', cm.output
            )
            assert_frame_equal(obs_df, exp_df)


if __name__ == "__main__":
    unittest.main()
