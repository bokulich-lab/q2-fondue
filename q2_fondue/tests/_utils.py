# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import io
import json

import pandas as pd
from entrezpy.efetch.efetch_request import EfetchRequest
from entrezpy.esearch.esearch_request import EsearchRequest
from qiime2.plugin.testing import TestPluginBase

from q2_fondue.entrezpy_clients._efetch import (EFetchAnalyzer, EFetchResult)
from q2_fondue.entrezpy_clients._esearch import ESearchResult


class FakeParams:
    def __init__(self, temp_dir, uids=None, term=None, eutil='efetch.cgi',
                 rettype='xml', retmode='text'):
        self.query_id = 'some-id-123'
        self.term = term
        self.usehistory = False
        self.db = 'sra'
        self.eutil = eutil
        self.uids = uids
        self.webenv = None
        self.idtype = None
        self.datetype = None
        self.reldate = None
        self.mindate = None
        self.maxdate = None
        self.querykey = 0
        self.rettype = rettype
        self.retmode = retmode
        self.strand = None
        self.sort = None
        self.field = None
        self.retstart = 0
        self.retmax = 0
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

    def xml_to_response(self, kind, suffix=''):
        path = self.get_data_path(f'metadata_response_{kind}{suffix}.xml')
        response = io.open(path, "rb", buffering=0)
        return response

    def json_to_response(self, kind, suffix=''):
        path = self.get_data_path(f'esearch_response_{kind}{suffix}.json')
        response = json.loads(io.open(path, "rb", buffering=0).read())
        return response

    def generate_efetch_request(self, uids, start=0, size=1):
        request_params = FakeParams(self.temp_dir.name, uids=uids)
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
            'Bases', 'Bytes', 'Spots', 'Tax ID'
        }
        for col in numeric_cols:
            exp_df[col] = exp_df[col].astype(str)
        return exp_df

    def generate_esearch_request(self, term, start=0, size=1):
        request_params = FakeParams(self.temp_dir.name, retmode='json',
                                    term=term, eutil='esearch.fcgi')
        return EsearchRequest(
            eutil='efetch.fcgi',
            parameter=request_params,
            start=start,
            size=size)

    def generate_esearch_result(self, kind, suffix):
        return ESearchResult(
            response=self.json_to_response(kind, suffix),
            request=self.generate_esearch_request(term="abc OR 123"))
