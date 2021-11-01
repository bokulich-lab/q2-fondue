# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import json
from itertools import chain
from typing import List, Union
from warnings import warn

import pandas as pd
from entrezpy.base.analyzer import EutilsAnalyzer
from entrezpy.base.result import EutilsResult
from xmltodict import parse as parsexml

from q2_fondue.entrezpy_clients._utils import (rename_columns)
from q2_fondue.entrezpy_clients._sra_meta import (LibraryMetadata, SRARun,
                                                  SRAExperiment, SRASample,
                                                  SRAStudy)


class InvalidIDs(Exception):
    pass


class EFetchResult(EutilsResult):
    """Entrezpy client for EFetch utility used to fetch SRA metadata."""

    def __init__(self, response, request):
        super().__init__(request.eutil, request.query_id, request.db)
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

    def metadata_to_series(self) -> pd.Series:
        """Converts collected run ids into a Series.

        Returns:
            run_ids (pd.Series): Series of run ids.
        """
        run_ids = list(chain.from_iterable(self.metadata.values()))
        return pd.Series(run_ids)

    def metadata_to_df(self) -> pd.DataFrame:
        """Converts collected metadata into a DataFrame.

        Returns:
            pd.DataFrame: Metadata in a form of a DataFrame with an index
                corresponding to the run IDs.
        """
        df = pd.concat([v.generate_meta() for v in self.studies.values()])
        df.index.name = 'ID'

        # remove empty columns, if any
        df.dropna(axis=1, inplace=True, how='all')

        # clean up column names
        df = rename_columns(df)

        # reorder columns in a more sensible fashion
        cols = [
            'Experiment ID', 'Biosample ID', 'Bioproject ID', 'Study ID',
            'Sample Accession', 'Organism', 'Library Source', 'Library Layout',
            'Library Selection', 'Instrument', 'Platform', 'Bases', 'Spots',
            'Avg Spot Len', 'Bytes', 'Public'
        ]
        cols.extend([c for c in df.columns if c not in cols])

        return df[cols]

    def extract_run_ids(self, response):
        """Extracts run IDs from an EFetch response.

        In the pipeline ESearch -> ELink -> EFetch we will receive
        a response containing IDs belonging to runs from the requested
        project. This method parses that response and finds those IDs.

        Args:
            response (io.StringIO): Response received from Efetch.
        """
        response = json.loads(json.dumps(parsexml(response.read())))
        result = response['eSummaryResult'].get('DocSum')
        if result:
            for i, content in enumerate(result):
                content = content.get('Item')
                for item in content:
                    for k, v in item.items():
                        if 'Run acc' in v:
                            runs = f'<Runs>{v.strip()}</Runs>'
                            runs = json.loads(json.dumps(parsexml(runs)))
                            runs = runs['Runs'].get('Run')
                            runs = [runs] if isinstance(runs, dict) else runs
                            self.metadata[i] = [x.get('@acc') for x in runs]
        # TODO: this needs an else-statement with a proper message
        #  when we have logging enabled

    @staticmethod
    def _find_bioproject_id(bioproject: Union[list, dict]) -> str:
        """Finds BioProject ID in the metadata.

        BioProject ID seems to be located in several places so we need
        a way to reliably find it.

        Args:
            bioproject (Union[list, dict]): BioProject's metadata.

        Returns:
            bioproject_id (str): ID of the BioProject.
        """
        if bioproject:  # if not found, try elsewhere:
            if isinstance(bioproject, list):
                bioproject_id = next(
                    (x for x in bioproject
                     if x['@namespace'].lower() == 'bioproject')
                ).get('#text')
            else:
                bioproject_id = bioproject.get('#text')
        else:
            bioproject_id = None
        return bioproject_id

    def _create_study(self, attributes: dict):
        """Extracts experiment-specific data from the metadata dictionary.

        Information like BioProject ID as well as instrument and platform
        details are extracted here.

        Args:
            attributes (dict): Dictionary with all the metadata from
                the XML response.

        """
        study_ids = attributes['STUDY']['IDENTIFIERS'].get('EXTERNAL_ID')
        study_id = attributes['STUDY']['IDENTIFIERS'].get('PRIMARY_ID')
        if study_id not in self.studies.keys():
            bioproject_id = self._find_bioproject_id(study_ids)

            org = attributes['Organization'].get('Name')
            if isinstance(org, dict):
                org = org.get('#text')

            custom_meta = self._extract_custom_attributes(
                attributes['STUDY'], 'study')

            self.studies[study_id] = SRAStudy(
                id=study_id,
                bioproject_id=bioproject_id,
                center_name=org,
                custom_meta=custom_meta
            )
        return study_id

    def _create_samples(self, attributes: dict, study_id: str) -> List[str]:
        """Creates SRASample objects.

        Information like BioSample ID, organism, name as well as custom
        metadata are added here.

        Args:
            attributes (dict): Dictionary with all the metadata from
                the XML response.
            study_id (str): ID of the study which the sample belongs to.
        Returns:
            sample_id (str): ID of the processed sample.
        """
        pool_meta = attributes['Pool'].get('Member')
        sample_attributes = attributes['SAMPLE']
        if not isinstance(pool_meta, list):
            # we have one sample
            pool_meta = [pool_meta]
        sample_ids = []
        for sample in pool_meta:
            sample_id = sample.get('@accession')
            if sample_id not in self.samples.keys():
                biosample_id = sample['IDENTIFIERS'].get('EXTERNAL_ID')
                if isinstance(biosample_id, list):
                    biosample_id = next(
                        (x for x in biosample_id if
                         x['@namespace'] == 'BioSample')
                    )
                if isinstance(sample_attributes, list):
                    sample_attributes = next(
                        (x for x in sample_attributes if
                         x['@accession'] == sample_id)
                    )
                custom_meta = self._extract_custom_attributes(
                    sample_attributes, 'sample')
                self.samples[sample_id] = SRASample(
                    id=sample_id,
                    name=sample.get('@sample_name'),
                    title=sample.get('@sample_title'),
                    biosample_id=biosample_id.get('#text'),
                    organism=sample.get('@organism'),
                    tax_id=sample.get('@tax_id'),
                    study_id=study_id,
                    custom_meta=custom_meta
                )

                # append sample to study
                self.studies[study_id].samples.append(self.samples[sample_id])
            sample_ids.append(sample_id)
        return sample_ids

    @staticmethod
    def _extract_library_info(attributes: dict) -> LibraryMetadata:
        """Extracts library-specific information.

        Args:
            attributes (dict): Dictionary with all the metadata
                from the XML response.
        Returns:
            library_meta (LibraryMetadata): Library metadata object.
        """
        lib_meta = attributes['EXPERIMENT']['DESIGN'].get('LIBRARY_DESCRIPTOR')

        keys = ['name', 'selection', 'source']
        lib = {k: lib_meta.get(f'LIBRARY_{k.upper()}') for k in keys}
        lib['layout'] = list(lib_meta.get('LIBRARY_LAYOUT').keys())[0]

        return LibraryMetadata(**lib)

    def _create_experiment(self, attributes: dict, sample_id: str) -> str:
        """Creates an SRAExperiment object.

        Information like Experiment ID, platform, instrument and library
        metadata as well as other custom metadata are added here.

        Args:
            attributes (dict): Dictionary with all the metadata from
                the XML response.
            sample_id (str): ID of the sample which the experiment belongs to.
        Returns:
            exp_id (str): ID of the processed study.
        """
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
                library=self._extract_library_info(attributes),
                custom_meta=None
            )
            # append experiment to sample
            self.samples[sample_id].experiments.append(
                self.experiments[exp_id])
        return exp_id

    def _create_single_run(
            self, pool_meta: dict, run: dict, exp_id: str) -> str:
        """Creates a single SRARun object.

        Information like Run ID, count of bases as well as other custom
        metadata are added here.

        Args:
            attributes (dict): Dictionary with all the metadata from
                the XML response.
            run (dict): Dictionary with run metadata.
            exp_id (str): ID of the experiment which the run belongs to.
        Returns:
            run_id (str): ID of the processed study.
        """
        run_id = run.get('@accession')

        if run.get('@is_public') == 'true':
            is_public = True
        else:
            is_public = False

        custom_meta = self._extract_custom_attributes(
            run, 'run')

        if run_id not in self.runs.keys():
            self.runs[run_id] = SRARun(
                id=run_id,
                public=is_public,
                bytes=int(run.get('@size')),
                bases=int(pool_meta.get('@bases')),
                spots=int(pool_meta.get('@spots')),
                experiment_id=exp_id,
                custom_meta=custom_meta
            )
            # append run to experiment
            self.experiments[exp_id].runs.append(self.runs[run_id])
        return run_id

    def _create_runs(
            self, attributes: dict, exp_id: str,
            sample_id: str, desired_id: str = None
    ) -> List[str]:
        """Creates all the required runs.

        Depending on whether a specific run should be extracted from the run
        set (defined by desired_id), either just one run will be created or
        one run object per run in the entire run set.

        Args:
            attributes (dict): Dictionary with all the metadata from
                the XML response.
            exp_id (str): ID of the experiment which the run belongs to.
            sample_id (str): ID of the sample which the run belongs to.
            desired_id (str): ID of the run to be extracted. If None, all runs
                will be added.
        Returns:
            run_ids (List[str]): List of all processed run IDs.
        """
        runset = attributes['RUN_SET']['RUN']
        if not isinstance(runset, list):
            runset = [runset]

        # TODO: make sure that this is correct
        pool_meta = attributes['Pool'].get('Member')

        # find the desired run (project and run case)
        if desired_id:
            run = next(
                (x for x in runset if x['@accession'] == desired_id)
            )
            if isinstance(pool_meta, list):
                pool_meta = next(
                    (x for x in pool_meta if x['@accession'] == sample_id)
                )
            run_ids = [self._create_single_run(pool_meta, run, exp_id)]
        # get all available runs (that should happen when we ask for samples)
        else:
            run_ids = []
            for run in runset:
                run_id = self._create_single_run(pool_meta, run, exp_id)
                run_ids.append(run_id)
        return run_ids

    def _process_single_id(
            self, attributes: dict, desired_id: str = None) -> List[str]:
        """Processes metadata obtained for a single accession ID.

        Args:
            attributes (dict): Dictionary with all the metadata
                from the XML response.
            desired_id (str): ID of the run/sample for which metadata
                should be extracted. If None, all the runs from any given
                run set will be extracted (not implemented).
        Returns:
            run_ids (List[str]): List of all processed run IDs.
        """
        # create study, if required
        study_id = self._create_study(attributes)

        # create sample, if required
        sample_ids = self._create_samples(attributes, study_id)

        # TODO: what happens here when we asked for samples?
        # create experiment, if required
        run_ids_all = []
        for sample_id in sample_ids:
            exp_id = self._create_experiment(attributes, sample_id)

            # TODO: what happens here when we asked for samples?
            # create runs
            run_ids = self._create_runs(
                attributes, exp_id, sample_id, desired_id)
            run_ids_all.extend(run_ids)

        return run_ids_all

    @staticmethod
    def _custom_attributes_to_dict(attributes: List[dict], level: str):
        """Converts attributes list into a dictionary

        Args:
            attributes (List[dict]): List of attribute dictionaries, e.g.:
                [{'TAG': 'tag1', 'VALUE': 'value1'},
                 {'TAG': 'tag2', 'VALUE': 'value2'},
                 {'TAG': 'tag1', 'VALUE': 'value2'}]

        Returns:
            attr_dict (dict): De-duplicated dictionary of attributes, e.g:
                {'tag1_1': 'value1', 'tag1_2': 'value2', 'tag2': 'value2'}
        """
        if isinstance(attributes, dict):
            attributes = [attributes]
        attributes = [attr for attr in attributes if "VALUE" in attr.keys()]
        attrs_sorted = sorted(attributes, key=lambda x: (x['TAG'], x['VALUE']))
        tags = [x['TAG'] for x in attrs_sorted]
        values = [x['VALUE'] for x in attrs_sorted]

        # de-duplicate tags
        tags_dedupl = []
        for i, tag in enumerate(tags):
            total, count = tags.count(tag), tags[:i].count(tag)
            if total > 1:
                warn(
                    f'One of the metadata keys ({tag}) is duplicated. '
                    f'It will be retained with a numeric suffix.'
                ) if count == 0 else False
                tags_dedupl.append(f'{tag}_{count + 1} [{level}]')
            else:
                tags_dedupl.append(f'{tag} [{level}]')

        return {t: v for t, v in zip(tags_dedupl, values)}

    def _extract_custom_attributes(self, attributes: dict, level: str) -> dict:
        """Extracts custom attributes from the metadata dictionary.

        Args:
            attributes (dict): Dictionary with all the metadata
                from the XML response.
            level (str): SRA hierarchy level at which metadata should be
                extracted (study, sample, run).
        Returns:
            processed_meta (dict): All metadata extracted for the given level.
        """
        processed_meta = {}
        level = level.upper()
        level_items = attributes.get(f'{level}_ATTRIBUTES')
        if level_items:
            level_attributes = level_items.get(f'{level}_ATTRIBUTE')
            try:
                attr_dedupl = self._custom_attributes_to_dict(
                    level_attributes, level)
                processed_meta.update(attr_dedupl)
            except Exception as e:
                print(f'Exception has occurred when processing {level} '
                      f'attributes: "{e}". Contents of the metadata '
                      f'was: {attributes}.')
                raise
        return processed_meta

    def add_metadata(self, response, uids: List[str]):
        """Processes response received from Efetch into metadata dictionary.

        Dictionary keys represent original accession IDs and the values
        correspond to corresponding metadata extracted from the XML response.

        Args:
            response (io.StringIO): Response received from Efetch.
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
                self.metadata[i] = self._process_single_id(
                    parsed_results[i], desired_id=uid)
        else:
            for i, uid in enumerate(uids):
                self.metadata[i] = self._process_single_id(
                    parsed_results, desired_id=uid)


class EFetchAnalyzer(EutilsAnalyzer):
    def __init__(self):
        super().__init__()
        self.response_type = None

    def init_result(self, response, request):
        self.response_type = request.rettype
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
        if self.response_type == 'docsum':
            # we asked for IDs
            self.result.extract_run_ids(response)
        else:
            # we asked for metadata
            self.result.add_metadata(response, request.uids)

    # override the base method to enable parsing when retmode=text
    def parse(self, raw_response, request):
        response = self.convert_response(
            raw_response.read().decode('utf-8'), request)
        self.analyze_result(response, request)
