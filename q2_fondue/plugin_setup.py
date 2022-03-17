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
    Plugin, Citations, Choices, Str, Int, List, Range
)

from q2_fondue import __version__
from q2_fondue.get_all import get_all
from q2_fondue.metadata import get_metadata, merge_metadata
from q2_fondue.sequences import get_sequences, combine_seqs
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
    'accession_ids': 'Artifact containing run or BioProject IDs for '
                     'which the metadata and/or sequences should be fetched.',
}

common_params = {
    'email': Str,
    'n_jobs': Int % Range(1, None),
    'log_level': Str % Choices(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
}

common_param_descr = {
    'email': 'Your e-mail address (required by NCBI).',
    'n_jobs': 'Number of concurrent download jobs (default: 1).',
    'log_level': 'Logging level.'
}

output_descriptions = {
    'metadata': 'Table containing metadata for all the requested IDs.',
    'single_reads': 'Artifact containing single-read fastq.gz files '
                    'for all the requested IDs.',
    'paired_reads': 'Artifact containing paired-end fastq.gz files '
                    'for all the requested IDs.',
    'failed_runs': 'List of all run IDs for which fetching {} failed, '
                   'with their corresponding error messages.'
}

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
)

plugin.methods.register_function(
    function=get_metadata,
    inputs={**common_inputs},
    parameters=common_params,
    outputs=[('metadata', SRAMetadata), ('failed_runs', SRAFailedIDs)],
    input_descriptions={**common_input_descriptions},
    parameter_descriptions=common_param_descr,
    output_descriptions={
        'metadata': output_descriptions['metadata'],
        'failed_runs': output_descriptions['failed_runs'].format('metadata')
    },
    name='Fetch sequence-related metadata based on run or BioProject ID.',
    description=(
        'Fetch sequence-related metadata based on run or BioProject ID '
        'using Entrez. All metadata will be collapsed into one table.'
    ),
    citations=[citations['Buchmann2019']]
)

plugin.methods.register_function(
    function=get_sequences,
    inputs={**common_inputs},
    parameters={
        **common_params,
        'retries': Int % Range(0, None)
    },
    outputs=[
        ('single_reads', SampleData[SequencesWithQuality]),
        ('paired_reads', SampleData[PairedEndSequencesWithQuality]),
        ('failed_runs', SRAFailedIDs)
    ],
    input_descriptions={**common_input_descriptions},
    parameter_descriptions={
        **common_param_descr,
        'retries': 'Number of retries to fetch sequences (default: 2).',
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
    inputs={**common_inputs},
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
    input_descriptions={**common_input_descriptions},
    parameter_descriptions={
        **common_param_descr,
        'retries': 'Number of retries to fetch sequences (default: 2).'
    },
    output_descriptions={
        'metadata': output_descriptions['metadata'],
        'single_reads': output_descriptions['single_reads'],
        'paired_reads': output_descriptions['paired_reads'],
        'failed_runs': output_descriptions['failed_runs'].format(
            'sequences and/or metadata ')
    },
    name='Fetch sequence-related metadata and sequences of all run or '
         'BioProject IDs.',
    description='Pipeline fetching all sequence-related metadata and raw '
                'sequences of provided run or BioProject IDs.',
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
        'library_type': Str % Choices(['user', 'group']),
        'library_id': Str,
        'api_key': Str,
        'collection_name': Str
    },
    outputs=[('run_ids', NCBIAccessionIDs),
             ('bioproject_ids', NCBIAccessionIDs), ],
    input_descriptions={},
    parameter_descriptions={
        'library_type': 'Zotero API library type.',
        'library_id': 'Valid Zotero library ID (for library_type \'user\' '
                      'extract from \'your userID for use in API calls\' in '
                      'https://www.zotero.org/settings/keys, '
                      'for \'group\' extract by hovering over group name '
                      'in https://www.zotero.org/groups/)',
        'api_key': 'Valid Zotero API user key (retrieve from '
                   'https://www.zotero.org/settings/keys/new checking '
                   '"Allow library access" and for \'group\' library '
                   '"Read/Write" permissions ).',
        'collection_name': 'Name of the collection to be scraped.'
    },
    output_descriptions={
        'run_ids': 'Artifact containing all run IDs scraped '
                   'from a Zotero collection.',
        'bioproject_ids': 'Artifact containing all BioProject IDs scraped '
                          'from a Zotero collection.'
    },
    name='Scrape Zotero collection for run and BioProject IDs.',
    description=(
        'Scrape HTML and PDF files of a Zotero collection for run and '
        'BioProject IDs.'
    ),
    citations=[citations['stephan_hugel_2019_2917290']]
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
