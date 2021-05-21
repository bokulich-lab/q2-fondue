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


class InvalidIDs(Exception):
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

    def _process_single_run(self, attributes_dict, extract_id=None):
        processed_meta = self._extract_custom_attributes(
            attributes_dict)

        # add library metadata
        processed_meta.update(self._extract_library_info(attributes_dict))

        # add pool metadata
        processed_meta.update(self._extract_pool_info(attributes_dict))

        # add experiment metadata
        processed_meta.update(self._extract_experiment_info(attributes_dict))

        # add run set metadata
        processed_meta.update(
            self._extract_run_set_info(attributes_dict, extract_id))

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
        external_id = pool_meta['IDENTIFIERS'].get('EXTERNAL_ID')
        if isinstance(external_id, list):
            external_id = next(
                (x for x in external_id if x['@namespace'] == 'BioSample')
            )
        pool_meta_proc = {
            'Bases': bases,
            'Spots': spots,
            'AvgSpotLen': str(int(int(bases)/int(spots))),
            'Organism': pool_meta.get('@organism'),
            'Tax ID': pool_meta.get('@tax_id'),
            'Sample Name': pool_meta.get('@sample_name'),
            'Sample Accession': pool_meta.get('@accession'),
            'Sample Title': pool_meta.get('@sample_title'),
            'BioSample': external_id.get('#text')
        }
        return pool_meta_proc

    @staticmethod
    def _extract_experiment_info(attributes_info):
        exp_meta = attributes_info['EXPERIMENT']
        bioproject = exp_meta['STUDY_REF']['IDENTIFIERS'].get('EXTERNAL_ID')
        if bioproject and bioproject['@namespace'] == 'BioProject':
            bioproject_id = bioproject['#text']
        elif not bioproject:  # if not found, try elsewhere:
            study_ids = attributes_info[
                'STUDY']['IDENTIFIERS'].get('EXTERNAL_ID')
            if isinstance(study_ids, list):
                bioproject_id = next(
                    (x for x in study_ids if x['@namespace'] == 'BioProject')
                ).get('#text')
            else:
                bioproject_id = study_ids.get('#text')
        else:
            bioproject_id = None

        platform = list(exp_meta['PLATFORM'].keys())[0]
        instrument = exp_meta['PLATFORM'][platform].get('INSTRUMENT_MODEL')

        exp_meta_proc = {
            'BioProject': bioproject_id,
            'Experiment': exp_meta['IDENTIFIERS'].get('PRIMARY_ID'),
            'Instrument': instrument,
            'Platform': platform,
            'SRA Study': exp_meta['STUDY_REF']['IDENTIFIERS'].get('PRIMARY_ID')
        }
        return exp_meta_proc

    @staticmethod
    def _extract_run_set_info(attributes_info, extract_id=None):
        runset_meta = attributes_info['RUN_SET']['RUN']
        if isinstance(runset_meta, list):
            if extract_id:
                runset_meta = next(
                    (x for x in runset_meta if x['@accession'] == extract_id)
                )
            else:
                raise NotImplementedError('Extracting all runs from the run'
                                          ' set is currently not supported')
        runset_meta_proc = {
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

        # TODO: we should also handle extracting multiple runs
        #  from the same experiment
        if isinstance(parsed_results, list):
            for i, uid in enumerate(uids):
                self.metadata[uid] = self._process_single_run(
                    parsed_results[i], extract_id=uid)
        else:
            self.metadata[uids[0]] = self._process_single_run(
                parsed_results, extract_id=uids[0])


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
