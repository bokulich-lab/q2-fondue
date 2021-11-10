# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import importlib

from qiime2.plugin import (Plugin, Citations, Str, Int, Range, Metadata)

from q2_fondue import __version__
from q2_fondue.metadata import get_metadata
from q2_fondue.types._format import SRAMetadataFormat, SRAMetadataDirFmt
from q2_fondue.types._type import SRAMetadata
from q2_types.sample_data import SampleData
from q2_types.per_sample_sequences import (
    SequencesWithQuality, PairedEndSequencesWithQuality)
from q2_fondue.sequences import get_sequences
from q2_fondue.get_all import get_all

str_acc_ids_desc = ('Path to file containing run or BioProject IDs for '
                    'which the {0} should be fetched. Should conform to '
                    'QIIME Metadata format.')

dict_parameter_descriptions = {
    'email': 'Your e-mail address (required by NCBI for metadata fetching).',
    'n_jobs': 'Number of concurrent metadata download jobs (default: 1).',
    'retries': 'Number of retries to fetch sequences (default: 2).',
    'threads': 'Number of threads to be used in parallel to fetch '
               'sequences (default: 6).'
}
dict_output_descriptions = {
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
        'its associated metadata from data archives like SRA.'),
    short_description='Plugin for fetching sequences and metadata.',
)

plugin.register_formats(SRAMetadataFormat, SRAMetadataDirFmt)
plugin.register_semantic_types(SRAMetadata)
plugin.register_semantic_type_to_format(
    SRAMetadata, artifact_format=SRAMetadataDirFmt)

plugin.methods.register_function(
    function=get_metadata,
    inputs={},
    parameters={
        'accession_ids': Metadata,
        'email': Str,
        'n_jobs': Int % Range(1, None)
    },
    outputs=[('metadata', SRAMetadata)],
    input_descriptions={},
    parameter_descriptions={
        'accession_ids': str_acc_ids_desc.format('metadata'),
        'email': dict_parameter_descriptions['email'],
        'n_jobs': dict_parameter_descriptions['n_jobs']
    },
    output_descriptions={
        'metadata': dict_output_descriptions['metadata']
    },
    name='Fetch sequence-related metadata based on run or BioProject ID.',
    description=(
        'Fetch sequence-related metadata based on run or BioProject ID '
        'using Entrez. All metadata will be collapsed into one table.'
    ),
    citations=[]
)

importlib.import_module('q2_fondue.types._transformer')

plugin.methods.register_function(
    function=get_sequences,
    inputs={},
    parameters={
        'accession_ids': Metadata,
        'email': Str,
        'retries': Int % Range(1, None),
        'threads': Int % Range(1, None)
    },
    outputs=[('single_reads', SampleData[SequencesWithQuality]),
             ('paired_reads', SampleData[PairedEndSequencesWithQuality])],
    input_descriptions={},
    parameter_descriptions={
        'accession_ids': str_acc_ids_desc.format('sequences'),
        'email': dict_parameter_descriptions['email'],
        'retries': dict_parameter_descriptions['retries'],
        'threads': dict_parameter_descriptions['threads']
    },
    output_descriptions={
        'single_reads': dict_output_descriptions['single_reads'],
        'paired_reads': dict_output_descriptions['paired_reads']
    },
    name='Fetch sequences based on run or BioProject IDs.',
    description='Fetch sequence data of all run or BioProject IDs.',
    citations=[]
)

plugin.pipelines.register_function(
    function=get_all,
    inputs={},
    parameters={
        'accession_ids': Metadata,
        'email': Str,
        'n_jobs': Int % Range(1, None),
        'retries': Int % Range(1, None),
        'threads': Int % Range(1, None)
    },
    outputs=[('metadata', SRAMetadata),
             ('single_reads', SampleData[SequencesWithQuality]),
             ('paired_reads', SampleData[PairedEndSequencesWithQuality])
             ],
    input_descriptions={},
    parameter_descriptions={
        'accession_ids': str_acc_ids_desc.format(
            'metadata and sequences'),
        **dict_parameter_descriptions},
    output_descriptions=dict_output_descriptions,
    name='Fetch sequence-related metadata and sequences of all run or '
         'BioProject IDs.',
    description='Pipeline fetching all sequence-related metadata and raw '
                'sequences of provided run or BioProject IDs.')
