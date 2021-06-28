# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from qiime2.plugin import (Plugin, Citations, List, Str, Int, Range)

from q2_fondue import __version__
from q2_types.sample_data import SampleData
from q2_types.per_sample_sequences import SequencesWithQuality
from q2_fondue.sequences import get_sequences

citations = Citations.load('citations.bib', package='q2_fondue')

plugin = Plugin(
    name='fondue',
    version=__version__,
    website="https://github.com/bokulich-lab/q2-fondue",
    package='q2_fondue',
    description=(
        'This is a QIIME 2 plugin for fetching raw sequencing data and'
        'its associated metadata from data archives like SRA.'),
    short_description='Plugin for fetching sequences and metadata.',
)

plugin.methods.register_function(
    function=get_sequences,
    inputs={},
    parameters={
        'study_ids': List[Str],
        'general_retries': Int % Range(1, None),
        'threads': Int % Range(1, None)
    },
    outputs=[('sequences', SampleData[SequencesWithQuality])],
    input_descriptions={},
    parameter_descriptions={
        'study_ids': 'A list of study IDs for which the sequences should '
                     'be fetched.',
        'general_retries': 'Number of retries to fetch sequences (default:2).',
        'threads': 'Number of threads when fetching sequences (default:6).'
    },
    output_descriptions={
        'sequences': 'Artifact containing fastq.gz files for all the '
        'requested studies.'
    },
    name='Fetch sequences based on study ID.',
    description=(
        'Fetch sequence data of all study IDs.'
    ),
    citations=[]
)
