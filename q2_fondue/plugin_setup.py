# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import importlib
from q2_types.per_sample_sequences import (
    SequencesWithQuality, PairedEndSequencesWithQuality
)
from q2_types.sample_data import SampleData
from qiime2.core.type import TypeMatch
from qiime2.plugin import (
    Plugin, Citations, Choices, Str, Int, List, Range, Bool
)

from q2_fondue import __version__
from q2_fondue.get_all import get_all
from q2_fondue.query import get_ids_from_query
from q2_fondue.metadata import get_metadata, merge_metadata
from q2_fondue.sequences import _get_sequences, get_sequences, combine_seqs
from q2_fondue.scraper import scrape_collection
from q2_fondue.types._format import (
    SRAMetadataFormat, SRAMetadataDirFmt,
    SRAFailedIDsFormat, SRAFailedIDsDirFmt,
    NCBIAccessionIDsFormat, NCBIAccessionIDsDirFmt
)
from q2_fondue.types._type import SRAMetadata, SRAFailedIDs, NCBIAccessionIDs

common_inputs = {
    'accession_ids': NCBIAccessionIDs | SRAMetadata | SRAFailedIDs
}

common_input_descriptions = {
    'accession_ids': 'Artifact containing run, study, BioProject, experiment '
                     'or sample IDs for which the metadata and/or sequences '
                     'should be fetched. Associated DOI names can be provided'
                     'in an optional column and are preserved in get-all'
                     'and get-metadata actions.',
}

common_params = {
    'email': Str,
    'n_jobs': Int % Range(1, None),
    'log_level': Str % Choices(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
}

common_param_descr = {
    'email': 'Your e-mail address (required by NCBI).',
    'n_jobs': 'Number of concurrent download jobs.',
    'log_level': 'Logging level.'
}

input_descriptions = {
    'linked_doi': 'Optional table containing linked DOI names that is '
                  'only used if accession_ids does not contain any '
                  'DOI names.'
}

output_descriptions = {
    'metadata': 'Table containing metadata for all the requested IDs.',
    'single_reads': 'Artifact containing single-read fastq.gz files '
                    'for all the requested IDs.',
    'paired_reads': 'Artifact containing paired-end fastq.gz files '
                    'for all the requested IDs.',
    'failed_runs': 'List of all run IDs for which fetching {} failed, '
                   'with their corresponding error messages.',
    'ids': 'Artifact containing retrieved SRA run accession IDs.'
}

output_scraper_txt = 'Artifact containing all {} IDs scraped from ' \
                     'a Zotero collection and associated DOI names.'

citations = Citations.load('citations.bib', package='q2_fondue')

plugin = Plugin(
    name='fondue',
    version=__version__,
    website='https://github.com/bokulich-lab/q2-fondue',
    package='q2_fondue',
    description=(
        'This is a QIIME 2 plugin for fetching raw sequencing data and'
        'its associated metadata from data archives like SRA.'
    ),
    short_description='Plugin for fetching sequences and metadata.',
    citations=[citations['Ziemski2022']]
)

plugin.methods.register_function(
    function=get_metadata,
    inputs={
        **common_inputs,
        'linked_doi': NCBIAccessionIDs
    },
    parameters=common_params,
    outputs=[('metadata', SRAMetadata), ('failed_runs', SRAFailedIDs)],
    input_descriptions={
        **common_input_descriptions,
        'linked_doi': input_descriptions['linked_doi']
    },
    parameter_descriptions=common_param_descr,
    output_descriptions={
        'metadata': output_descriptions['metadata'],
        'failed_runs': output_descriptions['failed_runs'].format('metadata')
    },
    name='Fetch sequence-related metadata based on run, study, BioProject, '
         'experiment or sample ID.',
    description=(
        'Fetch sequence-related metadata based on run, study, BioProject, '
        'experiment or sample ID using Entrez. All metadata will be collapsed '
        'into one table.'
    ),
    citations=[citations['Buchmann2019']]
)

plugin.methods.register_function(
    function=_get_sequences,
    inputs={**common_inputs},
    parameters={
        **{k: v for k, v in common_params.items() if k != 'email'},
        'retries': Int % Range(0, None),
        'restricted_access': Bool
    },
    outputs=[
        ('single_reads', SampleData[SequencesWithQuality]),
        ('paired_reads', SampleData[PairedEndSequencesWithQuality]),
        ('failed_runs', SRAFailedIDs)
    ],
    input_descriptions={**common_input_descriptions},
    parameter_descriptions={
        **{k: v for k, v in common_param_descr.items() if k != 'email'},
        'retries': 'Number of retries to fetch sequences.',
        'restricted_access': 'If sequence fetch requires dbGaP repository '
        'key.'
    },
    output_descriptions={
        'single_reads': output_descriptions['single_reads'],
        'paired_reads': output_descriptions['paired_reads'],
        'failed_runs': output_descriptions['failed_runs'].format('sequences')
    },
    name='Fetch sequences based on run ID.',
    description='Fetch sequence data of all run IDs.',
    citations=[citations['SraToolkit']]
)

plugin.pipelines.register_function(
    function=get_sequences,
    inputs={**common_inputs},
    parameters={
        **common_params,
        'retries': Int % Range(0, None),
        'restricted_access': Bool
    },
    outputs=[
        ('single_reads', SampleData[SequencesWithQuality]),
        ('paired_reads', SampleData[PairedEndSequencesWithQuality]),
        ('failed_runs', SRAFailedIDs)
    ],
    input_descriptions={**common_input_descriptions},
    parameter_descriptions={
        **common_param_descr,
        'retries': 'Number of retries to fetch sequences.',
        'restricted_access': 'If sequence fetch requires dbGaP repository '
        'key.'
    },
    output_descriptions={
        'single_reads': output_descriptions['single_reads'],
        'paired_reads': output_descriptions['paired_reads'],
        'failed_runs': output_descriptions['failed_runs'].format('sequences')
    },
    name='Fetch sequences based on run ID.',
    description='Fetch sequence data of all run IDs.',
    citations=[citations['SraToolkit']]
)

plugin.pipelines.register_function(
    function=get_all,
    inputs={**common_inputs,
            'linked_doi': NCBIAccessionIDs},
    parameters={
        **common_params,
        'retries': Int % Range(0, None)
    },
    outputs=[
        ('metadata', SRAMetadata),
        ('single_reads', SampleData[SequencesWithQuality]),
        ('paired_reads', SampleData[PairedEndSequencesWithQuality]),
        ('failed_runs', SRAFailedIDs)
    ],
    input_descriptions={
        **common_input_descriptions,
        'linked_doi': input_descriptions['linked_doi']
    },
    parameter_descriptions={
        **common_param_descr,
        'retries': 'Number of retries to fetch sequences.'
    },
    output_descriptions={
        'metadata': output_descriptions['metadata'],
        'single_reads': output_descriptions['single_reads'],
        'paired_reads': output_descriptions['paired_reads'],
        'failed_runs': output_descriptions['failed_runs'].format(
            'sequences and/or metadata ')
    },
    name='Fetch sequence-related metadata and sequences of all run, study, '
         'BioProject, experiment or sample IDs.',
    description='Pipeline fetching all sequence-related metadata and raw '
                'sequences of provided run, study, BioProject, experiment '
                'or sample IDs.',
    citations=[citations['Buchmann2019'], citations['SraToolkit']]
)

plugin.methods.register_function(
    function=merge_metadata,
    inputs={'metadata': List[SRAMetadata]},
    parameters={},
    outputs=[('merged_metadata', SRAMetadata)],
    input_descriptions={'metadata': 'Metadata files to be merged together.'},
    parameter_descriptions={},
    output_descriptions={
        'merged_metadata': 'Merged metadata containing all rows and columns '
                           '(without duplicates).'
    },
    name='Merge several metadata files into a single metadata object.',
    description=(
        'Merge multiple sequence-related metadata from different q2-fondue '
        'runs and/or projects into a single metadata file.'
    ),
    citations=[]
)

T = TypeMatch([SequencesWithQuality, PairedEndSequencesWithQuality])
plugin.methods.register_function(
    function=combine_seqs,
    inputs={'seqs': List[SampleData[T]]},
    parameters={'on_duplicates': Str % Choices(['error', 'warn'])},
    outputs=[('combined_seqs', SampleData[T])],
    input_descriptions={
        'seqs': 'Sequence artifacts to be combined together.'
    },
    parameter_descriptions={
        'on_duplicates': 'Preferred behaviour when duplicated sequence IDs '
                         'are encountered: "warn" displays a warning and '
                         'continues to combining deduplicated samples while '
                         '"error" raises an error and aborts further '
                         'execution.'
    },
    output_descriptions={
        'combined_seqs': 'Sequences combined from all input artifacts.',
    },
    name='Combine sequences from multiple artifacts.',
    description='Combine paired- or single-end sequences from multiple '
                'artifacts, for example obtained by re-fetching failed '
                'downloads.',
    citations=[]
)

plugin.methods.register_function(
    function=scrape_collection,
    inputs={},
    parameters={
        'collection_name': Str,
        'on_no_dois': Str % Choices(['ignore', 'error']),
        'log_level': Str % Choices(['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    },
    outputs=[('run_ids', NCBIAccessionIDs),
             ('study_ids', NCBIAccessionIDs),
             ('bioproject_ids', NCBIAccessionIDs),
             ('experiment_ids', NCBIAccessionIDs),
             ('sample_ids', NCBIAccessionIDs)],
    input_descriptions={},
    parameter_descriptions={
        'collection_name': 'Name of the collection to be scraped.',
        'on_no_dois': 'Behavior if no DOIs were found.',
        'log_level': 'Logging level.'
    },
    output_descriptions={
        'run_ids': output_scraper_txt.format('run'),
        'study_ids': output_scraper_txt.format('study'),
        'bioproject_ids': output_scraper_txt.format('BioProject'),
        'experiment_ids': output_scraper_txt.format('experiment'),
        'sample_ids': output_scraper_txt.format('sample')
    },
    name='Scrape Zotero collection for run, study, BioProject, experiment and '
    'sample IDs, and associated DOI names.',
    description=(
        'Scrape attachment files of a Zotero collection for run, study, '
        'BioProject, experiment and sample IDs, and associated DOI names.'
    ),
    citations=[citations['stephan_hugel_2019_2917290']]
)

plugin.methods.register_function(
    function=get_ids_from_query,
    inputs={},
    parameters={
        'query': Str,
        **common_params
    },
    outputs=[('ids', NCBIAccessionIDs)],
    input_descriptions={},
    parameter_descriptions={
        'query': 'Search query to retrieve SRA run IDs from '
                 'the BioSample database.',
        **common_param_descr
    },
    output_descriptions={
        'ids': output_descriptions['metadata'],
    },
    name='Find SRA run accession IDs based on a search query.',
    description=(
        'Find SRA run accession IDs in the BioSample database '
        'using a text search query.'
    ),
    citations=[]
)

plugin.register_formats(
    SRAMetadataFormat, SRAMetadataDirFmt,
    SRAFailedIDsFormat, SRAFailedIDsDirFmt,
    NCBIAccessionIDsFormat, NCBIAccessionIDsDirFmt
)
plugin.register_semantic_types(SRAMetadata, SRAFailedIDs, NCBIAccessionIDs)
plugin.register_semantic_type_to_format(
    SRAMetadata, artifact_format=SRAMetadataDirFmt
)
plugin.register_semantic_type_to_format(
    SRAFailedIDs, artifact_format=SRAFailedIDsDirFmt
)
plugin.register_semantic_type_to_format(
    NCBIAccessionIDs, artifact_format=NCBIAccessionIDsDirFmt
)

importlib.import_module('q2_fondue.types._transformer')
