# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import json

import pandas as pd
from entrezpy.base.analyzer import EutilsAnalyzer
from entrezpy.base.result import EutilsResult
from xmltodict import parse as parsexml


class DuplicateKeyError(Exception):
    pass


class EFetchResult(EutilsResult):
    def __init__(self, response, request):
        super().__init__(request.eutil, request.query_id, request.db)
        self.metadata_raw = None
        self.metadata = {}

    def size(self):
        return len(self.metadata)

    def isEmpty(self):
        return True if self.size() == 0 else False

    def dump(self):
        return {self: {'dump': {'metadata': self.metadata,
                                'query_id': self.query_id,
                                'db': self.db,
                                'eutil': self.function}}}

    def get_link_parameter(self, reqnum=0):
        return {}

    def to_df(self):
        df = pd.DataFrame.from_dict(self.metadata, orient='index')
        df.index.name = 'ID'
        return df

    def _process_single_run(self, attributes_dict):
        processed_meta = self._extract_custom_attributes(
            attributes_dict)

        # add library metadata
        processed_meta.update(self._extract_library_info(attributes_dict))

        # add pool metadata
        processed_meta.update(self._extract_pool_info(attributes_dict))

        # add experiment metadata
        processed_meta.update(self._extract_experiment_info(attributes_dict))

        # add run set metadata
        processed_meta.update(self._extract_run_set_info(attributes_dict))

        return processed_meta

    @staticmethod
    def _extract_custom_attributes(attributes_dict):
        processed_meta = {}
        keys_to_keep = {'SAMPLE', 'STUDY', 'RUN'}
        allowed_duplicates = {'ENA-FIRST-PUBLIC', 'ENA-LAST-UPDATE'}
        for k1, v1 in attributes_dict.items():
            if k1 in keys_to_keep and f'{k1}_ATTRIBUTES' in v1.keys():
                try:
                    for attr in v1[f'{k1}_ATTRIBUTES'][f'{k1}_ATTRIBUTE']:
                        if attr['TAG'] in processed_meta.keys() and \
                                attr['TAG'] not in allowed_duplicates:
                            raise DuplicateKeyError(
                                f'One of the metadata keys ({attr["TAG"]}) '
                                f'is duplicated.')
                        processed_meta[attr['TAG']] = attr.get('VALUE')
                except Exception as e:
                    if not isinstance(e, DuplicateKeyError):
                        # TODO: convert this to a proper logger
                        print(f'Exception has occurred when processing {k1} '
                              f'attributes: "{e}". Contents of the metadata '
                              f'was: {attributes_dict}.')
                    raise
        return processed_meta

    @staticmethod
    def _extract_library_info(attributes_dict):
        lib_meta_proc = {}
        lib_meta = attributes_dict['EXPERIMENT']['DESIGN'].get(
            'LIBRARY_DESCRIPTOR')
        if lib_meta:
            for n in {f'LIBRARY_{x}' for x in ['NAME', 'SELECTION', 'SOURCE']}:
                new_key = " ".join(n.lower().split('_')).title()
                lib_meta_proc[new_key] = lib_meta.get(n)
            lib_meta_proc['Library Layout'] = list(lib_meta.get(
                'LIBRARY_LAYOUT').keys())[0]
        return lib_meta_proc

    @staticmethod
    def _extract_pool_info(attributes_info):
        pool_meta = attributes_info['Pool'].get('Member')
        bases, spots = pool_meta.get('@bases'), pool_meta.get('@spots')
        pool_meta_proc = {
            'AvgSpotLen': str(int(int(bases)/int(spots))),
            'Organism': pool_meta.get('@organism'),
            'Sample Name': pool_meta.get('@sample_name')
        }
        return pool_meta_proc

    @staticmethod
    def _extract_experiment_info(attributes_info):
        biosample_id, bioproject_id = None, None
        exp_meta = attributes_info['EXPERIMENT']

        biosample = exp_meta[
            'DESIGN']['SAMPLE_DESCRIPTOR']['IDENTIFIERS'].get('EXTERNAL_ID')
        bioproject = exp_meta[
            'STUDY_REF']['IDENTIFIERS'].get('EXTERNAL_ID')
        # TODO: should this raise an error if not present?
        if biosample and biosample['@namespace'] == 'BioSample':
            biosample_id = biosample['#text']
        if bioproject and bioproject['@namespace'] == 'BioProject':
            bioproject_id = bioproject['#text']

        platform = list(exp_meta['PLATFORM'].keys())[0]
        instrument = exp_meta['PLATFORM'][platform].get('INSTRUMENT_MODEL')

        exp_meta_proc = {
            'BioSample': biosample_id,
            'BioProject': bioproject_id,
            'Experiment': exp_meta['IDENTIFIERS'].get('PRIMARY_ID'),
            'Instrument': instrument,
            'Platform': platform,
            'SRA Study': exp_meta['STUDY_REF']['IDENTIFIERS'].get('PRIMARY_ID')
        }
        return exp_meta_proc

    @staticmethod
    def _extract_run_set_info(attributes_info):
        runset_meta = attributes_info['RUN_SET']['RUN']
        runset_meta_proc = {
            'Bases': runset_meta['Bases'].get('@count'),
            'Bytes': runset_meta.get('@size'),
        }
        if runset_meta.get('@is_public') == 'true':
            runset_meta_proc['Consent'] = 'public'
        else:
            runset_meta_proc['Consent'] = 'private'
        runset_meta_proc['Center Name'] = \
            attributes_info['SUBMISSION'].get('@center_name')
        return runset_meta_proc

    def add_metadata(self, metadata, uids):
        # use json to quickly get rid of OrderedDicts
        self.metadata_raw = json.loads(json.dumps(parsexml(metadata.read())))
        parsed_results = self.metadata_raw[
            'EXPERIMENT_PACKAGE_SET']['EXPERIMENT_PACKAGE']

        if isinstance(parsed_results, list):
            for i, uid in enumerate(uids):
                self.metadata[uid] = self._process_single_run(
                    parsed_results[i])
        else:
            self.metadata[uids[0]] = self._process_single_run(
                parsed_results)


class EFetchAnalyzer(EutilsAnalyzer):
    def __init__(self):
        super().__init__()

    def init_result(self, response, request):
        if not self.result:
            self.result = EFetchResult(response, request)

    def analyze_error(self, response, request):
        print(json.dumps({
            __name__: {
                'Response': {
                    'dump': request.dump(),
                    'error': response.getvalue()}}}))

    def analyze_result(self, response, request):
        self.init_result(response, request)
        self.result.add_metadata(response, request.uids)

    # override the base method to enable parsing when retmode=text
    def parse(self, raw_response, request):
        response = self.convert_response(
            raw_response.read().decode('utf-8'), request)
        self.analyze_result(response, request)
