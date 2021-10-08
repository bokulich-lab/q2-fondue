# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import json
from typing import List
from warnings import warn

import pandas as pd
from entrezpy.base.analyzer import EutilsAnalyzer
from entrezpy.base.result import EutilsResult
from xmltodict import parse as parsexml


class InvalidIDs(Exception):
    pass


class EFetchResult(EutilsResult):
    """Entrezpy client for EFetch utility used to fetch SRA metadata."""
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

    def to_df(self) -> pd.DataFrame:
        """Converts collected metadata into a DataFrame.

        Returns:
            pd.DataFrame: Metadata in a form of a DataFrame with an index
                corresponding to the original accession IDs.
        """
        df = pd.DataFrame.from_dict(self.metadata, orient='index')
        df.index.name = 'ID'

        # remove empty columns, if any
        df.dropna(axis=1, inplace=True, how='all')

        # reorder columns in a more sensible fashion
        cols = ['Experiment ID', 'BioSample ID', 'BioProject ID', 'Study ID',
                'Sample Accession', 'Organism', 'Library Source',
                'Library Selection', 'Library Layout', 'Instrument',
                'Platform', 'Bases', 'Spots', 'AvgSpotLen', 'Bytes', 'Consent']
        cols.extend([c for c in df.columns if c not in cols])

        return df[cols]

    def _process_single_id(
            self, attributes_dict: dict, extract_id: str = None):
        """Processes metadata obtained for a single accession ID.

        Args:
            attributes_dict (dict): Dictionary with all the metadata
                from the XML response.
            extract_id (str): ID of the run/sample for which metadata
                should be extracted. If None, all the runs from any given
                run set will be extracted (not implemented).

        """
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
    def _extract_custom_attributes(attributes_dict: dict):
        """Extracts custom attributes from the metadata dictionary.

        Args:
            attributes_dict (dict): Dictionary with all the metadata
                from the XML response.

        """
        processed_meta = {}
        keys_to_keep = {'SAMPLE', 'STUDY', 'RUN'}
        for k1, v1 in attributes_dict.items():
            if k1 in keys_to_keep and f'{k1}_ATTRIBUTES' in v1.keys():
                try:
                    dupl = 0
                    for attr in v1[f'{k1}_ATTRIBUTES'][f'{k1}_ATTRIBUTE']:
                        current_attr = attr['TAG']
                        if current_attr in processed_meta.keys():
                            warn(
                                f'One of the metadata keys ({current_attr}) '
                                f'is duplicated. It will be retained with '
                                f'a "_{dupl+1}" suffix.'
                            )
                            dupl += 1
                            current_attr = f'{current_attr}_{dupl}'
                        processed_meta[current_attr] = attr.get('VALUE')
                except Exception as e:
                    print(f'Exception has occurred when processing {k1} '
                          f'attributes: "{e}". Contents of the metadata '
                          f'was: {attributes_dict}.')
                    raise
        return processed_meta

    @staticmethod
    def _extract_library_info(attributes_dict: dict):
        """Extracts library-specific information from the metadata dictionary.

        Args:
            attributes_dict (dict): Dictionary with all the metadata
                from the XML response.

        """
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
    def _extract_pool_info(attributes_dict: dict):
        """Extracts pool information from the metadata dictionary.

        Information like base and spot count will be retrieved here and the
        average spot length will be calculated. Moreover, sample attributes
        (name, accession ID, title, BioSample ID) and organism information
        will be extracted.

        Args:
            attributes_dict (dict): Dictionary with all the metadata
                from the XML response.

        """
        pool_meta = attributes_dict['Pool'].get('Member')
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
            'BioSample ID': external_id.get('#text')
        }
        return pool_meta_proc

    @staticmethod
    def _extract_experiment_info(attributes_dict: dict):
        """Extracts experiment-specific data from the metadata dictionary.

        Information like BioProject ID as well as instrument and platform
        details are extracted here.

        Args:
            attributes_dict (dict): Dictionary with all the metadata
                from the XML response.

        """
        exp_meta = attributes_dict['EXPERIMENT']
        bioproject = exp_meta['STUDY_REF']['IDENTIFIERS'].get('EXTERNAL_ID')
        if bioproject and bioproject['@namespace'] == 'BioProject':
            bioproject_id = bioproject['#text']
        elif not bioproject:  # if not found, try elsewhere:
            study_ids = attributes_dict[
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
            'BioProject ID': bioproject_id,
            'Experiment ID': exp_meta['IDENTIFIERS'].get('PRIMARY_ID'),
            'Instrument': instrument,
            'Platform': platform,
            'Study ID': exp_meta['STUDY_REF']['IDENTIFIERS'].get('PRIMARY_ID')
        }
        return exp_meta_proc

    @staticmethod
    def _extract_run_set_info(attributes_dict: dict, extract_id: str = None):
        """Extracts run data from the run set in the metadata dictionary.

        attributes_dict (dict): Dictionary with all the metadata
                from the XML response.
        extract_id (str): ID of the run/sample for which metadata
            should be extracted. If None, all the runs from any given
            run set will be extracted (not implemented).

        """
        runset_meta = attributes_dict['RUN_SET']['RUN']
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
            attributes_dict['SUBMISSION'].get('@center_name')
        return runset_meta_proc

    def add_metadata(self, response, uids: List[str]):
        """Processes response received from Efetch into metadata dictionary.

        Dictionary keys represent original accession IDs and the values
        correspond to corresponding metadata extracted from the XML response.

        Args:
            response (): Response received from Efetch.
            uids (List[str]): List of accession IDs for which
                the data was fetched.

        """
        # use json to quickly get rid of OrderedDicts
        self.metadata_raw = json.loads(json.dumps(parsexml(response.read())))
        parsed_results = self.metadata_raw[
            'EXPERIMENT_PACKAGE_SET']['EXPERIMENT_PACKAGE']

        # TODO: we should also handle extracting multiple runs
        #  from the same experiment
        if isinstance(parsed_results, list):
            for i, uid in enumerate(uids):
                self.metadata[uid] = self._process_single_id(
                    parsed_results[i], extract_id=uid)
        else:
            self.metadata[uids[0]] = self._process_single_id(
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
