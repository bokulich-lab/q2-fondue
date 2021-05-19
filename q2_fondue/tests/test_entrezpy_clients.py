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

from q2_fondue._entrezpy_clients import DuplicateKeyError, EFetchResult
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

    def test_efresult_extract_custom_attributes_duplicated_attribute(self):
        self.metadata_dict['STUDY']['STUDY_ATTRIBUTES'][
            'STUDY_ATTRIBUTE'].append({
                "TAG": "environment (biome)",
                "VALUE": "berry plant"
            })

        with self.assertRaisesRegexp(
                DuplicateKeyError,
                r'.*keys \(environment \(biome\)\).*duplicated\.'):
            self.efetch_result_single._extract_custom_attributes(
                self.metadata_dict)

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
            "BioSample": "SAMEA6608408",
            "BioProject": "PRJEB37054",
            "Experiment": "ERX3980916",
            "Instrument": "Illumina MiSeq",
            "Platform": "ILLUMINA",
            "SRA Study": "ERP120343",
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


if __name__ == "__main__":
    unittest.main()
