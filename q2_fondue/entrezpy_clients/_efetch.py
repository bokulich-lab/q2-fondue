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

from q2_fondue.entrezpy_clients._utils import (SRAStudy, SRASample,
                                               SRAExperiment, LibraryMetadata,
                                               SRARun)


class InvalidIDs(Exception):
    pass


class EFetchResult(EutilsResult):
    """Entrezpy client for EFetch utility used to fetch SRA metadata."""
    def __init__(self, response, request, id_type):
        super().__init__(request.eutil, request.query_id, request.db)
        self.id_type = id_type
        self.metadata_raw = None
        self.metadata = {}
        self.studies = {}
        self.samples = {}
        self.experiments = {}
        self.runs = {}

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

    def _create_study(self, attributes: dict):
        """Extracts experiment-specific data from the metadata dictionary.

        Information like BioProject ID as well as instrument and platform
        details are extracted here.

        Args:
            attributes (dict): Dictionary with all the metadata from
                the XML response.

        """
        exp = attributes['EXPERIMENT']
        study_id = exp['STUDY_REF']['IDENTIFIERS'].get('PRIMARY_ID')
        if study_id not in self.studies.keys():
            bioproject = exp['STUDY_REF']['IDENTIFIERS'].get('EXTERNAL_ID')
            if bioproject and bioproject['@namespace'] == 'BioProject':
                bioproject_id = bioproject['#text']
            elif not bioproject:  # if not found, try elsewhere:
                study_ids = attributes[
                    'STUDY']['IDENTIFIERS'].get('EXTERNAL_ID')
                if isinstance(study_ids, list):
                    bioproject_id = next(
                        (x for x in study_ids
                         if x['@namespace'] == 'BioProject')
                    ).get('#text')
                else:
                    bioproject_id = study_ids.get('#text')
            else:
                bioproject_id = None

            self.studies[study_id] = SRAStudy(
                id=study_id,
                bioproject_id=bioproject_id,
                center_name=attributes['STUDY'].get('@center_name')
            )
        return study_id

    def _create_sample(self, attributes: dict, study_id: str):
        pool_meta = attributes['Pool'].get('Member')
        sample_id = pool_meta.get('@accession')
        if sample_id not in self.samples.keys():
            biosample_id = pool_meta['IDENTIFIERS'].get('EXTERNAL_ID')
            if isinstance(biosample_id, list):
                biosample_id = next(
                    (x for x in biosample_id if x['@namespace'] == 'BioSample')
                )
            self.samples[sample_id] = SRASample(
                id=sample_id,
                name=pool_meta.get('@sample_name'),
                title=pool_meta.get('@sample_title'),
                biosample_id=biosample_id.get('#text'),
                organism=pool_meta.get('@organism'),
                tax_id=pool_meta.get('@tax_id'),
                study_id=study_id
            )
        # append sample to study
        self.studies[study_id].samples.append(self.samples[sample_id])
        return sample_id

    @staticmethod
    def _extract_library_info(attributes: dict):
        """Extracts library-specific information from the metadata dictionary.

        Args:
            attributes (dict): Dictionary with all the metadata
                from the XML response.
        """
        lib_meta = attributes['EXPERIMENT']['DESIGN'].get('LIBRARY_DESCRIPTOR')

        keys = ['name', 'selection', 'source']
        lib = {k: lib_meta.get(f'LIBRARY_{k.upper()}') for k in keys}
        lib['layout'] = list(lib_meta.get('LIBRARY_LAYOUT').keys())[0]

        return LibraryMetadata(**lib)

    def _create_experiment(self, attributes: dict, sample_id: str):
        exp_meta = attributes['EXPERIMENT']
        exp_id = exp_meta['IDENTIFIERS'].get('PRIMARY_ID')
        if exp_id not in self.experiments.keys():
            platform = list(exp_meta['PLATFORM'].keys())[0]
            instrument = exp_meta['PLATFORM'][platform].get('INSTRUMENT_MODEL')
            self.experiments[exp_id] = SRAExperiment(
                id=exp_id,
                instrument=instrument,
                platform=platform,
                sample_id=sample_id,
                library=self._extract_library_info(attributes)
            )
        # append experiment to sample
        self.samples[sample_id].experiments.append(self.experiments[exp_id])
        return exp_id

    def _create_run(
            self, attributes: dict, exp_id: str, desired_id: str = None
    ):
        runset = attributes['RUN_SET']['RUN']
        if not isinstance(runset, list):
            runset = [runset]

        # find the desired run
        if desired_id:
            runset = next(
                (x for x in runset if x['@accession'] == desired_id)
            )
            run_id = runset.get('@accession')

            if runset.get('@is_public') == 'true':
                is_public = True
            else:
                is_public = False

            pool_meta = attributes['Pool'].get('Member')
            if run_id not in self.runs.keys():
                self.runs[run_id] = SRARun(
                    id=run_id,
                    public=is_public,
                    size=runset.get('@size'),
                    bases=pool_meta.get('@bases'),
                    spots=pool_meta.get('@spots'),
                    experiment_id=exp_id
                )
            # append run to experiment
            self.experiments[exp_id].runs.append(self.runs[run_id])
            return [run_id]
        # get all available runs
        else:
            raise NotImplementedError('Extracting all runs from the run'
                                      ' set is currently not supported')

    def _process_single_id(
            self, attributes_dict: dict, desired_id: str = None):
        """Processes metadata obtained for a single accession ID.

        Args:
            attributes_dict (dict): Dictionary with all the metadata
                from the XML response.
            desired_id (str): ID of the run/sample for which metadata
                should be extracted. If None, all the runs from any given
                run set will be extracted (not implemented).

        """
        # create study, if required
        study_id = self._create_study(attributes_dict)

        # create sample, if required
        sample_id = self._create_sample(attributes_dict, study_id)

        # TODO: what happens here when we asked for samples?
        # create experiment, if required
        exp_id = self._create_experiment(attributes_dict, sample_id)

        # TODO: what happens here when we asked for samples?
        # create run
        run_ids = self._create_run(attributes_dict, exp_id, desired_id)

        return run_ids

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
                    parsed_results[i], desired_id=uid)
        else:
            self.metadata[uids[0]] = self._process_single_id(
                parsed_results, desired_id=uids[0])


class EFetchAnalyzer(EutilsAnalyzer):
    def __init__(self, id_type):
        super().__init__()
        self.id_type = id_type

    def init_result(self, response, request):
        if not self.result:
            self.result = EFetchResult(response, request, self.id_type)

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
