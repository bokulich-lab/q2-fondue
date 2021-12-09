# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import importlib

from qiime2.plugin import (
    Plugin, Citations, Choices, Str, Int, List, Range, Metadata
)

from q2_fondue import __version__
from q2_fondue.metadata import get_metadata, merge_metadata
from q2_fondue.types._format import SRAMetadataFormat, SRAMetadataDirFmt
from q2_fondue.types._type import SRAMetadata
from q2_types.sample_data import SampleData
from q2_types.per_sample_sequences import (
    SequencesWithQuality, PairedEndSequencesWithQuality)
from q2_fondue.sequences import get_sequences
from q2_fondue.get_all import get_all

common_param_descr = {
    'accession_ids': 'Path to file containing run or BioProject IDs for '
                     'which the metadata and/or sequences should be fetched. '
                     'Should conform to QIIME Metadata format.',
    'email': 'Your e-mail address (required by NCBI).',
    'n_jobs': 'Number of concurrent download jobs (default: 1).',
    'log_level': 'Logging level.'
}

common_params = {
    'accession_ids': Metadata,
    'email': Str,
    'n_jobs': Int % Range(1, None),
    'log_level': Str % Choices(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
}

output_descriptions = {
    'metadata': 'Table containing metadata for all the requested IDs.',
    'single_reads': 'Artifact containing single-read fastq.gz files '
                    'for all the requested IDs.',
    'paired_reads': 'Artifact containing paired-end fastq.gz files '
                    'for all the requested IDs.'
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

plugin.register_formats(SRAMetadataFormat, SRAMetadataDirFmt)
plugin.register_semantic_types(SRAMetadata)
plugin.register_semantic_type_to_format(
    SRAMetadata, artifact_format=SRAMetadataDirFmt)

plugin.methods.register_function(
    function=get_metadata,
    inputs={},
    parameters=common_params,
    outputs=[('metadata', SRAMetadata)],
    input_descriptions={},
    parameter_descriptions=common_param_descr,
    output_descriptions={'metadata': output_descriptions['metadata']},
    name='Fetch sequence-related metadata based on run or BioProject ID.',
    description=(
        'Fetch sequence-related metadata based on run or BioProject ID '
        'using Entrez. All metadata will be collapsed into one table.'
    ),
    citations=[citations['Buchmann2019']]
)

importlib.import_module('q2_fondue.types._transformer')

plugin.methods.register_function(
    function=get_sequences,
    inputs={},
    parameters={
        **common_params,
        'retries': Int % Range(1, None)
    },
    outputs=[('single_reads', SampleData[SequencesWithQuality]),
             ('paired_reads', SampleData[PairedEndSequencesWithQuality])],
    input_descriptions={},
    parameter_descriptions={
        **common_param_descr,
        'retries': 'Number of retries to fetch sequences (default: 2).',
    },
    output_descriptions={
        'single_reads': output_descriptions['single_reads'],
        'paired_reads': output_descriptions['paired_reads']
    },
    name='Fetch sequences based on run ID.',
    description='Fetch sequence data of all run IDs.',
    citations=[citations['SraToolkit']]
)

plugin.pipelines.register_function(
    function=get_all,
    inputs={},
    parameters={
        **common_params,
        'retries': Int % Range(1, None)
    },
    outputs=[('metadata', SRAMetadata),
             ('single_reads', SampleData[SequencesWithQuality]),
             ('paired_reads', SampleData[PairedEndSequencesWithQuality])
             ],
    input_descriptions={},
    parameter_descriptions={
        **common_param_descr,
        'retries': 'Number of retries to fetch sequences (default: 2).'
    },
    output_descriptions=output_descriptions,
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
    output_descriptions={'merged_metadata': 'Merged metadata containing all rows and columns (without duplicates).'},
    name='Merge several metadata files into a single metadata object.',
    description=(
        'Merge multiple sequence-related metadata from different q2-fondue '
        'runs and/or projects into a single metadata file.'
    ),
    citations=[]
)
