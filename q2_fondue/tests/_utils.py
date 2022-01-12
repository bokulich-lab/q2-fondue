# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import io
import json
import logging

import pandas as pd
from entrezpy.efetch.efetch_request import EfetchRequest
from entrezpy.elink.elink_request import ElinkRequest
from entrezpy.esearch.esearch_request import EsearchRequest
from qiime2.plugin.testing import TestPluginBase

from q2_fondue.entrezpy_clients._efetch import (EFetchAnalyzer, EFetchResult)
from q2_fondue.entrezpy_clients._elink import ELinkResult
from q2_fondue.entrezpy_clients._esearch import ESearchResult
from q2_fondue.entrezpy_clients._sra_meta import (SRAStudy, SRASample,
                                                  SRAExperiment,
                                                  LibraryMetadata, SRARun)


class FakeParams:
    def __init__(self, temp_dir, uids=None, term=None, eutil='efetch.cgi',
                 rettype='xml', retmode='text'):
        self.query_id = 'some-id-123'
        self.term = term
        self.usehistory = False
        self.cmd = None
        self.linkname = None
        self.holding = False
        self.doseq = None
        self.db = 'sra'
        self.dbfrom = 'sra'
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
        self.efetch_result_single = self.generate_ef_result('single')
        self.efetch_result_multi = self.generate_ef_result('multi')
        self.efetch_analyzer = EFetchAnalyzer(log_level='INFO')
        self.efetch_request_properties = {
            'db', 'eutil', 'uids', 'webenv', 'querykey', 'rettype', 'retmode',
            'strand', 'seqstart', 'seqstop', 'complexity'
        }
        self.esearch_request_properties = {
            'db', 'eutil', 'webenv', 'retmode', 'term'
        }
        self.library_meta = LibraryMetadata(
            name='unspecified', layout='SINGLE',
            selection='PCR', source='METAGENOMIC'
        )
        with open(self.get_data_path('metadata_response_small.json'),
                  'r') as ff:
            self.metadata_dict = json.load(ff)
        self.maxDiff = None
        self.fake_logger = logging.getLogger('test_log')

    def xml_to_response(self, kind, suffix='', prefix='metadata'):
        path = self.get_data_path(f'{prefix}_response_{kind}{suffix}.xml')
        response = io.open(path, "rb", buffering=0)
        return response

    def json_to_response(self, kind, suffix='', raw=False, utility='esearch'):
        path = self.get_data_path(f'{utility}_response_{kind}{suffix}.json')
        response = io.open(path, "rb", buffering=0)
        if raw:
            return response
        else:
            return json.loads(io.open(path, "rb", buffering=0).read())

    def generate_ef_request(self, uids, start=0, size=1):
        request_params = FakeParams(self.temp_dir.name, uids=uids)
        return EfetchRequest(
            eutil='efetch.fcgi',
            parameter=request_params,
            start=start,
            size=size
        )

    def generate_ef_result(self, kind, prefix='metadata'):
        return EFetchResult(
            response=self.xml_to_response(kind, prefix=prefix),
            request=self.generate_ef_request(['FAKEID1', 'FAKEID2']),
            log_level='INFO'
        )

    def generate_sra_metadata(self):
        study_id, sample_id = 'ERP120343', 'ERS4372624'
        experiment_id, run_ids = 'ERX3980916', ['FAKEID1', 'FAKEID2']
        study = SRAStudy(
            id=study_id, bioproject_id='PRJEB37054',
            center_name='University of Hohenheim',
            custom_meta={
                'ENA-FIRST-PUBLIC [STUDY]': '2020-05-31',
                'ENA-LAST-UPDATE [STUDY]': '2020-03-04'
            }
        )
        sample = SRASample(
            id=sample_id, biosample_id='SAMEA6608408', name='BAC1.D1.0.32A',
            title='Vitis vinifera', organism='Vitis vinifera', tax_id='29760',
            study_id=study_id, custom_meta={
                'environment (biome) [SAMPLE]': 'berry plant',
                'geographic location (country and/or sea) [SAMPLE]': 'Germany',
                'sample storage temperature [SAMPLE]': '-80'}
        )
        experiment = SRAExperiment(
            id=experiment_id, instrument='Illumina MiSeq', platform='ILLUMINA',
            library=self.library_meta, sample_id=sample_id, custom_meta=None
        )
        runs = [SRARun(
            id=_id, bases=11552099, spots=39323, public=True, bytes=3914295,
            experiment_id=experiment_id, custom_meta={
                'ENA-FIRST-PUBLIC [RUN]': '2020-05-31',
                'ENA-LAST-UPDATE [RUN]': '2020-03-06'}
        ) for _id in run_ids]
        return study, sample, experiment, runs

    def generate_expected_df(self):
        exp_df = pd.read_json(
            path_or_buf=self.get_data_path('metadata_processed_multi.json'),
            orient='index'
        )
        exp_df.index.name = 'ID'
        numeric_cols = {
            'Amount or size of sample collected [sample]',
            'Collection day [sample]', 'Collection hours [sample]',
            'Sample storage temperature [sample]', 'Tax ID',
            'Sample volume or weight for dna extraction [sample]',
        }
        exp_df['Public'] = exp_df['Public'].astype(bool)
        for col in numeric_cols:
            exp_df[col] = exp_df[col].astype(str)
        return exp_df

    def generate_es_request(self, term, start=0, size=1):
        request_params = FakeParams(self.temp_dir.name, retmode='json',
                                    term=term, eutil='esearch.fcgi')
        return EsearchRequest(
            eutil='esearch.fcgi',
            parameter=request_params,
            start=start,
            size=size)

    def generate_es_result(self, kind, suffix):
        return ESearchResult(
            response=self.json_to_response(kind, suffix, utility='esearch'),
            request=self.generate_es_request(term="abc OR 123"))

    def generate_el_request(self):
        request_params = FakeParams(self.temp_dir.name, retmode='json',
                                    eutil='elink.fcgi')
        return ElinkRequest(eutil='elink.fcgi', parameter=request_params)

    def generate_el_result(self, kind, suffix):
        return ELinkResult(
            response=self.json_to_response(kind, suffix, utility='elink'),
            request=self.generate_el_request())
