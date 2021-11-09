# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest
from unittest.mock import patch, MagicMock, ANY, call

import entrezpy.efetch.efetcher as ef
import entrezpy.esearch.esearcher as es
import pandas as pd
from entrezpy import conduit
from entrezpy.esearch import esearcher
from entrezpy.requester.requester import Requester
from pandas._testing import assert_frame_equal
from qiime2.metadata import Metadata

from q2_fondue.entrezpy_clients._efetch import EFetchAnalyzer
from q2_fondue.metadata import (
    _efetcher_inquire, _get_project_meta, get_metadata, _get_run_meta
)
from q2_fondue.utils import (_validate_esearch_result,
                             _determine_id_type)
from q2_fondue.entrezpy_clients._utils import InvalidIDs
from q2_fondue.tests._utils import _TestPluginWithEntrezFakeComponents


class FakeConduit:
    def __init__(self, fake_efetch_result, fake_efetch_response):
        self.logging = MagicMock()
        self.fake_efetch_result = fake_efetch_result
        self.fake_efetch_response = fake_efetch_response
        self.pipeline = MagicMock()

    def new_pipeline(self):
        self.pipeline.add_search = MagicMock(return_value='fake_search')
        self.pipeline.add_link = MagicMock(return_value='fake_link')
        self.pipeline.add_fetch = MagicMock(return_value='fake_fetch')
        return self.pipeline

    def run(self, pipeline):
        analyzer = EFetchAnalyzer()
        analyzer.result = self.fake_efetch_result
        analyzer.result.extract_run_ids(self.fake_efetch_response)
        return analyzer


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

    def test_determine_id_type_project(self):
        ids = ['PRJNA123', 'PRJNA123']

        obs = _determine_id_type(ids)
        exp = 'bioproject'
        self.assertEqual(exp, obs)

    def test_determine_id_type_run(self):
        ids = ['SRR123', 'ERR123']

        obs = _determine_id_type(ids)
        exp = 'run'
        self.assertEqual(exp, obs)

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
            obs_df, _ = _efetcher_inquire(self.fake_efetcher, ['FAKEID1'])
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
                self.fake_efetcher, ['FAKEID1', 'FAKEID2'])
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

    def test_esearcher_inquire_single(self):
        with patch.object(Requester, 'request') as mock_request:
            mock_request.return_value = self.json_to_response(
                'single', '_correct', True)
            obs_result = _validate_esearch_result(
                self.fake_esearcher, ['SRR000001'])
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_es_request('SRR000001')

        for arg in self.esearch_request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        self.assertTrue(obs_result)

    def test_esearcher_inquire_multi(self):
        with patch.object(Requester, 'request') as mock_request:
            mock_request.return_value = self.json_to_response(
                'multi', '_correct', True)
            obs_result = _validate_esearch_result(
                self.fake_esearcher, ['SRR000001', 'SRR000013', 'ERR3978173'])
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_es_request(
            'SRR000001 OR SRR000013 OR ERR3978173')

        for arg in self.esearch_request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        self.assertTrue(obs_result)

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

    @patch.object(esearcher, 'Esearcher')
    @patch('q2_fondue.metadata._validate_esearch_result')
    @patch('q2_fondue.metadata._execute_efetcher')
    def test_get_run_meta(self, patch_ef, patch_val, patch_es):
        exp_df = pd.DataFrame(
            {'meta1': [1, 2, 3], 'meta2': ['a', 'b', 'c']},
            index=['AB', 'cd', 'Ef']
        )
        patch_ef.return_value = (exp_df, [])
        obs_df = _get_run_meta('someone@somewhere.com', 1, ['AB', 'cd', 'Ef'])

        assert_frame_equal(exp_df, obs_df)
        patch_es.assert_called_once_with(
            'esearcher', 'someone@somewhere.com', apikey=None,
            apikey_var=None, threads=1, qid=None
        )
        patch_val.assert_called_once_with(ANY, ['AB', 'cd', 'Ef'])
        patch_ef.assert_called_once_with(
            'someone@somewhere.com', 1, ['AB', 'cd', 'Ef']
        )

    @patch.object(esearcher, 'Esearcher')
    @patch('q2_fondue.metadata._validate_esearch_result')
    @patch('q2_fondue.metadata._execute_efetcher')
    def test_get_run_meta_missing_ids(self, patch_ef, patch_val, patch_es):
        exp_df = pd.DataFrame(
            {'meta1': [1, 2, 3], 'meta2': ['a', 'b', 'c']},
            index=['AB', 'cd', 'Ef']
        )
        patch_ef.side_effect = [(exp_df.iloc[:2, :], ['Ef']),
                                (exp_df.iloc[2:, :], [])]
        obs_df = _get_run_meta('someone@somewhere.com', 1, ['AB', 'cd', 'Ef'])

        assert_frame_equal(exp_df, obs_df)
        patch_es.assert_called_once_with(
            'esearcher', 'someone@somewhere.com', apikey=None,
            apikey_var=None, threads=1, qid=None
        )
        patch_val.assert_called_once_with(ANY, ['AB', 'cd', 'Ef'])
        patch_ef.assert_has_calls(
            [call('someone@somewhere.com', 1, ['AB', 'cd', 'Ef']),
             call('someone@somewhere.com', 1, ['Ef'])], any_order=False
        )
        self.assertEqual(2, patch_ef.call_count)

    @patch.object(esearcher, 'Esearcher')
    @patch('q2_fondue.metadata._validate_esearch_result')
    @patch('q2_fondue.metadata._execute_efetcher')
    def test_get_run_meta_not_all_found(self, patch_ef, patch_val, patch_es):
        exp_df = pd.DataFrame(
            {'meta1': [1, 2, 3], 'meta2': ['a', 'b', 'c']},
            index=['AB', 'cd', 'Ef']
        )
        patch_ef.side_effect = [(exp_df, ['Ef']) for i in range(21)]

        with self.assertWarnsRegex(
                Warning, 'could not be fetched: Ef. Please try fetching'):
            _ = _get_run_meta('someone@somewhere.com', 1, ['AB', 'cd', 'Ef'])

    @patch('q2_fondue.metadata._get_run_meta')
    def test_get_project_meta(self, patched_get):
        with patch.object(conduit, 'Conduit') as mock_conduit:
            mock_conduit.return_value = self.fake_econduit
            _ = _get_project_meta('someone@somewhere.com', 1, ['AB', 'cd'])

            exp_ids = ['SRR13961771', 'SRR000007', 'SRR000018', 'SRR000020',
                       'SRR000038', 'SRR000043', 'SRR000046', 'SRR000048',
                       'SRR000050', 'SRR000057', 'SRR000058', 'SRR13961759']

            self.fake_econduit.pipeline.add_search.assert_called_once_with(
                {'db': 'bioproject', 'term': "AB OR cd"}, analyzer=ANY
            )
            patched_get.assert_called_once_with(
                'someone@somewhere.com', 1, exp_ids
            )

    @patch('q2_fondue.metadata._get_run_meta')
    @patch('q2_fondue.metadata._get_project_meta')
    def test_get_metadata_run(self, patched_get_project, patched_get_run):
        ids_meta = Metadata.load(self.get_data_path('run_ids.tsv'))
        _ = get_metadata(ids_meta, 'abc@def.com', 2)

        patched_get_run.assert_called_once_with(
            'abc@def.com', 2, ['SRR123', 'SRR234', 'SRR345']
        )
        patched_get_project.assert_not_called()

    @patch('q2_fondue.metadata._get_run_meta')
    @patch('q2_fondue.metadata._get_project_meta')
    def test_get_metadata_project(self, patched_get_project, patched_get_run):
        ids_meta = Metadata.load(self.get_data_path('project_ids.tsv'))
        _ = get_metadata(ids_meta, 'abc@def.com', 2)

        patched_get_project.assert_called_once_with(
            'abc@def.com', 2, ['PRJNA123', 'PRJNA234']
        )
        patched_get_run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
