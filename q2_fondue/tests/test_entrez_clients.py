# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import json
import unittest

import pandas as pd

from q2_fondue.entrezpy_clients._efetch import EFetchResult, InvalidIDs
from q2_fondue.entrezpy_clients._esearch import ESearchResult, ESearchAnalyzer
from q2_fondue.tests._utils import _TestPluginWithEntrezFakeComponents


class TestEntrezClients(_TestPluginWithEntrezFakeComponents):
    package = 'q2_fondue.tests'

    def test_efresult_extract_custom_attributes(self):
        obs = self.efetch_result_single._extract_custom_attributes(
            self.metadata_dict)
        exp = {
            "ENA-FIRST-PUBLIC": "2020-05-31",
            "ENA-LAST-UPDATE": "2020-03-04",
            "environment (biome)": "berry plant",
            "geographic location (country and/or sea)": "Germany",
            "sample storage temperature": "-80"
        }
        self.assertDictEqual(exp, obs)

    def test_efresult_process_single_run(self):
        obs = self.efetch_result_single._process_single_run(self.metadata_dict)
        exp = {
            "ENA-FIRST-PUBLIC": "2020-05-31",
            "ENA-LAST-UPDATE": "2020-03-04",
            "environment (biome)": "berry plant",
            "geographic location (country and/or sea)": "Germany",
            "sample storage temperature": "-80",
            "Library Name": "unspecified",
            "Library Layout": "SINGLE",
            "Library Selection": "PCR",
            "Library Source": "METAGENOMIC",
            "Spots": "39323",
            "Tax ID": "29760",
            "AvgSpotLen": "293",
            "Organism": "Vitis vinifera",
            "Sample Name": "BAC1.D1.0.32A",
            "BioSample ID": "SAMEA6608408",
            "BioProject ID": "PRJEB37054",
            "Experiment ID": "ERX3980916",
            "Instrument": "Illumina MiSeq",
            "Platform": "ILLUMINA",
            "Study ID": "ERP120343",
            "Bases": "11552099",
            "Bytes": "3914295",
            "Consent": "public",
            "Center Name": "UNIVERSITY OF HOHENHEIM",
            "Sample Accession": "ERS4372624",
            "Sample Title": "Vitis vinifera",
        }
        self.assertDictEqual(exp, obs)

    def test_efresult_add_metadata_single_study(self):
        self.efetch_result_single.add_metadata(
            self.xml_to_response('single'), ['FAKEID1'])

        obs = self.efetch_result_single.metadata
        with open(
                self.get_data_path('metadata_processed_multi.json'), 'r') as f:
            exp = json.load(f)
            del exp['FAKEID2']
        self.assertDictEqual(exp, obs)

    def test_efresult_add_metadata_single_study_complex(self):
        self.efetch_result_single.add_metadata(
            self.xml_to_response('single', "_complex"), ['FAKEID1'])

        obs = self.efetch_result_single.metadata
        with open(
                self.get_data_path('metadata_processed_multi.json'), 'r') as f:
            exp = json.load(f)
            del exp['FAKEID2']
        self.assertDictEqual(exp, obs)

    def test_efresult_add_metadata_multiple_studies(self):
        self.efetch_result_single.add_metadata(
            self.xml_to_response('multi'), ['FAKEID1', 'FAKEID2'])

        obs = self.efetch_result_single.metadata
        with open(
                self.get_data_path('metadata_processed_multi.json'), 'r') as f:
            exp = json.load(f)
        self.assertDictEqual(exp, obs)

    def test_efresult_size(self):
        self.efetch_result_single.add_metadata(
            self.xml_to_response('multi'), ['FAKEID1', 'FAKEID2'])

        obs = self.efetch_result_single.size()
        self.assertEqual(2, obs)

    def test_efresult_is_not_empty(self):
        self.efetch_result_single.add_metadata(
            self.xml_to_response('single'), ['FAKEID1'])

        obs = self.efetch_result_single.isEmpty()
        self.assertFalse(obs)

    def test_efresult_to_df(self):
        self.efetch_result_single.add_metadata(
            self.xml_to_response('multi'), ['FAKEID1', 'FAKEID2'])

        obs = self.efetch_result_single.to_df()
        exp = self.generate_expected_df()
        pd.testing.assert_frame_equal(
            exp.sort_index(axis=1), obs.sort_index(axis=1))

    def test_efanalyzer_analyze_result(self):
        self.efetch_analyzer.analyze_result(
            response=self.xml_to_response('single'),
            request=self.generate_efetch_request(['FAKEID1'])
        )

        self.assertTrue(isinstance(self.efetch_analyzer.result, EFetchResult))
        with open(
                self.get_data_path('metadata_processed_multi.json'), 'r') as f:
            exp = json.load(f)
            del exp['FAKEID2']
        self.assertDictEqual(exp, self.efetch_analyzer.result.metadata)

    def test_esresult_parse_search_results(self):
        esearch_result = self.generate_esearch_result('single', '_correct')
        esearch_result.parse_search_results(
            self.json_to_response('single', '_correct'), ["SRR000001"])

        obs = esearch_result.result
        exp = pd.Series(data=[1], index=["SRR000001"], name="count")
        pd.testing.assert_series_equal(exp, obs)

    def test_esresult_parse_search_results_ambiguous(self):
        esearch_result = self.generate_esearch_result('single', '_ambiguous')
        esearch_result.parse_search_results(
            self.json_to_response('single', '_ambiguous'), ["SR012"])

        obs = esearch_result.result
        exp = pd.Series(data=[7], index=["SR012"], name="count")
        pd.testing.assert_series_equal(exp, obs)

    def test_esresult_parse_search_results_multi(self):
        esearch_result = self.generate_esearch_result('multi', '_correct')
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
        esearch_result = self.generate_esearch_result('multi', '_invalid')
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
        esearch_result = self.generate_esearch_result('multi', '_mixed')
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
        esearch_result = self.generate_esearch_result('single', '_correct')
        esearch_result.result = pd.Series(
            data=[1], index=["SRR000001"], name="count")

        obs = esearch_result.validate_result()
        self.assertTrue(obs)

    def test_esresult_validate_result_single_ambiguous(self):
        esearch_result = self.generate_esearch_result('single', '_ambiguous')
        esearch_result.result = pd.Series(
            data=[7], index=["SR012"], name="count")

        with self.assertRaisesRegexp(
                InvalidIDs, r'.*Ambiguous IDs\: SR012'):
            esearch_result.validate_result()

    def test_esresult_validate_result_multi(self):
        esearch_result = self.generate_esearch_result('multi', '_correct')
        esearch_result.result = pd.Series(
            data=[1, 1, 1],
            index=["SRR000001", "SRR000013", "ERR3978173"],
            name="count"
        )

        obs = esearch_result.validate_result()
        self.assertTrue(obs)

    def test_esresult_validate_result_multi_invalid(self):
        esearch_result = self.generate_esearch_result('multi', '_invalid')
        esearch_result.result = pd.Series(
            data=[0, 0], index=["ABCD123", "SRR001"], name="count")

        with self.assertRaisesRegexp(
                InvalidIDs, r'.*Invalid IDs\: ABCD123, SRR001'):
            esearch_result.validate_result()

    def test_esresult_validate_result_multi_mixed(self):
        esearch_result = self.generate_esearch_result('multi', '_mixed')
        esearch_result.result = pd.Series(
            data=[1, 1, 7, 0, 0],
            index=["SRR000001", "SRR000013", "SR012", "ABCD123", "SRR001"],
            name="count")

        with self.assertRaisesRegexp(
                InvalidIDs,
                r'.*Ambiguous IDs\: SR012\n.*Invalid IDs\: ABCD123, SRR001'):
            esearch_result.validate_result()

    def test_esresult_size(self):
        esearch_result = self.generate_esearch_result('multi', '_mixed')
        esearch_result.parse_search_results(
            self.json_to_response('multi', '_mixed'),
            ["SRR000001", "SRR000013", "SR012", "ABCD123", "SRR001"])

        obs = esearch_result.size()
        self.assertEqual(5, obs)

    def test_esearch_is_not_empty(self):
        esearch_result = self.generate_esearch_result('multi', '_mixed')
        esearch_result.parse_search_results(
            self.json_to_response('multi', '_mixed'),
            ["SRR000001", "SRR000013", "SR012", "ABCD123", "SRR001"])

        obs = esearch_result.isEmpty()
        self.assertFalse(obs)

    def test_esanalyzer_analyze_result(self):
        es_analyzer = ESearchAnalyzer(["SRR000001"])
        es_analyzer.analyze_result(
            response=self.json_to_response('single', '_correct'),
            request=self.generate_esearch_request("SRR000001")
        )

        self.assertTrue(
            isinstance(es_analyzer.result, ESearchResult))


if __name__ == "__main__":
    unittest.main()
