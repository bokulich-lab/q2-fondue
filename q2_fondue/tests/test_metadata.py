# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import io
import json
from unittest.mock import patch

import pandas as pd
import entrezpy.efetch.efetcher as ef
from entrezpy.efetch.efetch_request import EfetchRequest
from entrezpy.requester.requester import Requester
from qiime2.plugin.testing import TestPluginBase

from q2_fondue.metadata import (EFetchResult, DuplicateKeyError,
                                EFetchAnalyzer,
                                _efetcher_inquire)


class FakeParams:
    def __init__(self, temp_dir, uids):
        self.query_id = 'some-id-123'
        self.db = 'sra'
        self.eutil = 'efetch.cgi'
        self.uids = uids
        self.webenv = None
        self.querykey = 0
        self.rettype = 'xml'
        self.retmode = 'text'
        self.strand = None
        self.seqstart = None
        self.seqstop = None
        self.complexity = None
        self.temp_dir = temp_dir


class _TestPluginWithEntrezFakeComponents(TestPluginBase):
    def setUp(self):
        super().setUp()
        self.efetch_result_single = self.generate_efetch_result('single')
        self.efetch_result_multi = self.generate_efetch_result('multi')
        self.efetch_analyzer = EFetchAnalyzer()
        self.request_properties = {'db', 'eutil', 'uids', 'webenv', 'querykey',
                                   'rettype', 'retmode', 'strand', 'seqstart',
                                   'seqstop', 'complexity'}
        with open(self.get_data_path('metadata_response_small.json'),
                  'r') as ff:
            self.metadata_dict = json.load(ff)
        self.maxDiff = None

    def xml_to_response(self, kind):
        path = self.get_data_path(f'metadata_response_{kind}.xml')
        response = io.open(path, "rb", buffering=0)
        return response

    def generate_efetch_request(self, uids, start=0, size=1):
        request_params = FakeParams(self.temp_dir.name, uids)
        return EfetchRequest(
            eutil='efetch.fcgi',
            parameter=request_params,
            start=start,
            size=size)

    def generate_efetch_result(self, kind):
        return EFetchResult(
            response=self.xml_to_response(kind),
            request=self.generate_efetch_request(['FAKEID1', 'FAKEID2'])
        )

    def generate_expected_df(self):
        exp_df = pd.read_json(
            path_or_buf=self.get_data_path('metadata_processed_multi.json'),
            orient='index'
        )
        exp_df.index.name = 'ID'
        numeric_cols = {
            'amount or size of sample collected', 'collection day',
            'collection hours', 'sample storage temperature',
            'sample volume or weight for DNA extraction', 'AvgSpotLen',
            'Bases', 'Bytes'
        }
        for col in numeric_cols:
            exp_df[col] = exp_df[col].astype(str)
        return exp_df


class TestEntrezComponents(_TestPluginWithEntrezFakeComponents):
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
                '.*keys \(environment \(biome\)\).*duplicated\.'):
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
            "Center Name": "UNIVERSITY OF HOHENHEIM"
        }
        self.assertDictEqual(exp, obs)

    # def test_efresult_process_single_run_unkown_error(self):
    #     pass

    def test_efresult_add_metadata_single_study(self):
        self.efetch_result_single.add_metadata(
            self.xml_to_response('single'), ['FAKEID1'])

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


class TestMetadataFetching(_TestPluginWithEntrezFakeComponents):
    package = 'q2_fondue.tests'

    def setUp(self):
        super().setUp()
        self.fake_efetcher = ef.Efetcher(
            'fake_efetcher', 'fake@email.com', apikey=None,
            apikey_var=None, threads=1, qid=None
        )

    def test_efetcher_inquire_single(self):
        with patch.object(Requester, 'request') as mock_request:
            mock_request.return_value = self.xml_to_response('single')
            obs_df = _efetcher_inquire(
                self.fake_efetcher, ['FAKEID1'])
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_efetch_request(['FAKEID1'])
        exp_df = self.generate_expected_df().iloc[[0]]

        for arg in self.request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        pd.testing.assert_frame_equal(
            exp_df.sort_index(axis=1), obs_df.sort_index(axis=1))

    def test_efetcher_inquire_multi(self):
        with patch.object(Requester, 'request') as mock_request:
            mock_request.return_value = self.xml_to_response('multi')
            obs_df = _efetcher_inquire(
                self.fake_efetcher, ['FAKEID1', 'FAKEID2'])
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_efetch_request(
            ['FAKEID1', 'FAKEID2'], size=2)
        exp_df = self.generate_expected_df()

        for arg in self.request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        pd.testing.assert_frame_equal(
            exp_df.sort_index(axis=1), obs_df.sort_index(axis=1))
