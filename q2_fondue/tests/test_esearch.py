# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest

import pandas as pd

from q2_fondue.entrezpy_clients._esearch import ESearchResult, ESearchAnalyzer
from q2_fondue.tests._utils import _TestPluginWithEntrezFakeComponents


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


if __name__ == "__main__":
    unittest.main()
