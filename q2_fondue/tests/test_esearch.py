# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest
from unittest.mock import patch, MagicMock, ANY, call

import pandas as pd
from q2_fondue.entrezpy_clients import _esearch
from q2_fondue.entrezpy_clients._esearch import (
    ESearchResult, ESearchAnalyzer, get_run_id_count)
from q2_fondue.tests._utils import _TestPluginWithEntrezFakeComponents


class FakeESAnalyzer():
    def __init__(self, uids):
        self.uids = uids
        self.log_level = 'INFO'
        self.result = MagicMock()
        self.result.result = pd.Series(
            data=[6, 6], index=['ABC', '123'])


class TestEsearchClients(_TestPluginWithEntrezFakeComponents):
    package = 'q2_fondue.tests'

    def test_esresult_parse_search_results(self):
        esearch_result = self.generate_es_result('single', '_correct')
        esearch_result.parse_search_results(
            self.json_to_response('single', '_correct'), ["SRR000001"])

        obs = esearch_result.result
        exp = pd.Series(data=[1], index=["SRR000001"], name="count")
        pd.testing.assert_series_equal(exp, obs)

    def test_esresult_parse_search_results_ambiguous(self):
        esearch_result = self.generate_es_result('single', '_ambiguous')
        esearch_result.parse_search_results(
            self.json_to_response('single', '_ambiguous'), ["SR012"])

        obs = esearch_result.result
        exp = pd.Series(data=[7], index=["SR012"], name="count")
        pd.testing.assert_series_equal(exp, obs)

    def test_esresult_parse_search_results_multi(self):
        esearch_result = self.generate_es_result('multi', '_correct')
        esearch_result.parse_search_results(
            self.json_to_response('multi', '_correct'),
            ["SRR000001", "SRR000013", "ERR3978173"])

        obs = esearch_result.result
        exp = pd.Series(
            data=[1, 1, 1],
            index=["SRR000001", "SRR000013", "ERR3978173"],
            name="count"
        )
        pd.testing.assert_series_equal(exp, obs)

    def test_esresult_parse_search_results_multi_invalid(self):
        esearch_result = self.generate_es_result('multi', '_invalid')
        esearch_result.parse_search_results(
            self.json_to_response('multi', '_invalid'), ["ABCD123", "SRR001"])

        obs = esearch_result.result
        exp = pd.Series(
            data=[0, 0],
            index=["ABCD123", "SRR001"],
            name="count"
        )
        pd.testing.assert_series_equal(exp, obs)

    def test_esresult_parse_search_results_multi_mixed(self):
        esearch_result = self.generate_es_result('multi', '_mixed')
        esearch_result.parse_search_results(
            self.json_to_response('multi', '_mixed'),
            ["SRR000001", "SRR000013", "SR012", "ABCD123", "SRR001"])

        obs = esearch_result.result
        exp = pd.Series(
            data=[1, 1, 7, 0, 0],
            index=["SRR000001", "SRR000013", "SR012", "ABCD123", "SRR001"],
            name="count"
        )
        pd.testing.assert_series_equal(exp, obs)

    def test_esresult_validate_result_single(self):
        esearch_result = self.generate_es_result('single', '_correct')
        esearch_result.result = pd.Series(
            data=[1], index=["SRR000001"], name="count")

        obs = esearch_result.validate_result()
        self.assertDictEqual(obs, {})

    def test_esresult_validate_result_single_ambiguous(self):
        esearch_result = self.generate_es_result('single', '_ambiguous')
        esearch_result.result = pd.Series(
            data=[7], index=['SR012'], name='count')

        obs = esearch_result.validate_result()
        exp = {'SR012': 'ID is ambiguous.'}
        self.assertDictEqual(obs, exp)

    def test_esresult_validate_result_multi(self):
        esearch_result = self.generate_es_result('multi', '_correct')
        esearch_result.result = pd.Series(
            data=[1, 1, 1],
            index=['SRR000001', 'SRR000013', 'ERR3978173'],
            name='count'
        )

        obs = esearch_result.validate_result()
        self.assertDictEqual(obs, {})

    def test_esresult_validate_result_multi_invalid(self):
        esearch_result = self.generate_es_result('multi', '_invalid')
        esearch_result.result = pd.Series(
            data=[0, 0], index=['ABCD123', 'SRR001'], name='count')

        obs = esearch_result.validate_result()
        exp = {'ABCD123': 'ID is invalid.', 'SRR001': 'ID is invalid.'}
        self.assertDictEqual(obs, exp)

    def test_esresult_validate_result_multi_mixed(self):
        esearch_result = self.generate_es_result('multi', '_mixed')
        esearch_result.result = pd.Series(
            data=[1, 1, 7, 0, 0],
            index=['SRR000001', 'SRR000013', 'SR012', 'ABCD123', 'SRR001'],
            name='count')

        obs = esearch_result.validate_result()
        exp = {
            'SR012': 'ID is ambiguous.', 'ABCD123': 'ID is invalid.',
            'SRR001': 'ID is invalid.'
        }
        self.assertDictEqual(obs, exp)

    def test_esresult_size(self):
        esearch_result = self.generate_es_result('multi', '_mixed')
        esearch_result.parse_search_results(
            self.json_to_response('multi', '_mixed'),
            ["SRR000001", "SRR000013", "SR012", "ABCD123", "SRR001"])

        obs = esearch_result.size()
        self.assertEqual(5, obs)

    def test_esearch_is_not_empty(self):
        esearch_result = self.generate_es_result('multi', '_mixed')
        esearch_result.parse_search_results(
            self.json_to_response('multi', '_mixed'),
            ["SRR000001", "SRR000013", "SR012", "ABCD123", "SRR001"])

        obs = esearch_result.isEmpty()
        self.assertFalse(obs)

    def test_esanalyzer_analyze_result(self):
        es_analyzer = ESearchAnalyzer(["SRR000001"], 'INFO')
        es_analyzer.analyze_result(
            response=self.json_to_response('single', '_correct'),
            request=self.generate_es_request("SRR000001")
        )

        self.assertTrue(
            isinstance(es_analyzer.result, ESearchResult))

    @patch('entrezpy.esearch.esearcher.Esearcher')
    @patch.object(_esearch, 'ESearchAnalyzer')
    def test_get_run_id_count(self, mock_analyzer, mock_search):
        ids = ['ABC', '123']
        mock_analyzer.return_value = FakeESAnalyzer(ids)
        mock_search.return_value.inquire = mock_analyzer

        _ = get_run_id_count(
            'someone@somewhere.com', 1, ids, 'INFO')

        mock_search.assert_called_once_with(
            ANY, 'someone@somewhere.com', apikey=None, apikey_var=None,
            threads=1, qid=None)
        mock_analyzer.assert_has_calls([
            call(ids, 'INFO'),
            call({'db': 'sra', 'term': 'ABC OR 123'}, analyzer=ANY)])


if __name__ == "__main__":
    unittest.main()
