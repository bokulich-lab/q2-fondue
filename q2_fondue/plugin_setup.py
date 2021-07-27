# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import importlib

from qiime2.plugin import (Plugin, Citations, List, Str, Int, Range)

from q2_fondue import __version__
from q2_fondue.metadata import get_metadata
from q2_fondue.types._format import SRAMetadataFormat, SRAMetadataDirFmt
from q2_fondue.types._type import SRAMetadata
from q2_types.sample_data import SampleData
from q2_types.per_sample_sequences import (
    SequencesWithQuality, PairedEndSequencesWithQuality)
from q2_fondue.sequences import get_sequences

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
        'sample_ids': List[Str],
        'email': Str,
        'n_jobs': Int % Range(1, None)
    },
    outputs=[('metadata', SRAMetadata)],
    input_descriptions={},
    parameter_descriptions={
        'sample_ids': 'A list of sample IDs for which the metadata should '
                      'be fetched.',
        'email': 'Your e-mail address (required by NCBI).',
        'n_jobs': 'Number of concurrent download jobs. Defaults to 1.'
    },
    output_descriptions={
        'metadata': 'Table containing metadata for all the requested studies.'
    },
    name='Fetch sequence-related metadata based on study ID.',
    description=(
        'Fetch sequence-related metadata based on study ID using Entrez. '
        'Metadata from all the studies will be collapsed into one table.'
    ),
    citations=[]
)

importlib.import_module('q2_fondue.types._transformer')

plugin.methods.register_function(
    function=get_sequences,
    inputs={},
    parameters={
        'sample_ids': List[Str],
        'general_retries': Int % Range(1, None),
        'threads': Int % Range(1, None)
    },
    outputs=[('single_reads', SampleData[SequencesWithQuality]),
             ('paired_ends', SampleData[PairedEndSequencesWithQuality])],
    input_descriptions={},
    parameter_descriptions={
        'sample_ids': 'A list of study IDs for which the sequences should '
        'be fetched.',
        'general_retries': 'Number of retries to fetch sequences '
        '(default:2).',
        'threads': 'Number of threads to be used in parallel '
        '(default:6).'
    },
    output_descriptions={
        'single_reads': 'Artifact containing single-read fastq.gz files '
        'for all the requested studies.',
        'paired_ends': 'Artifact containing paired-end fastq.gz files '
        'for all the requested studies.'
    },
    name='Fetch sequences based on study ID.',
    description=(
        'Fetch sequence data of all study IDs.'
    ),
    citations=[]
)
