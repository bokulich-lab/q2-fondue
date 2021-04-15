# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import pandas as pd
from qiime2.plugin import (Plugin, Citations, List, Str, Int, Range)

from q2_fondue import __version__
from q2_fondue.metadata import (get_metadata, TestMetadataFormat,
                                TestMetadataDirFmt, TestMetadata)

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

# TODO: register all the methods below

plugin.register_formats(TestMetadataFormat, TestMetadataDirFmt)
plugin.register_semantic_types(TestMetadata)
plugin.register_semantic_type_to_format(
    TestMetadata, artifact_format=TestMetadataDirFmt)


@plugin.register_transformer
def _1(data: pd.DataFrame) -> (TestMetadataFormat):
    ff = TestMetadataFormat()
    with ff.open() as fh:
        data.to_csv(fh, sep='\t', header=True)
    return ff


plugin.methods.register_function(
    function=get_metadata,
    inputs={},
    parameters={
        'study_ids': List[Str],
        'email': Str,
        'n_jobs': Int % Range(1, None)
    },
    outputs=[('metadata', TestMetadata)],
    input_descriptions={},
    parameter_descriptions={
        'study_ids': 'A list of study IDs for which the metadata should '
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
