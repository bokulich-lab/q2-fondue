# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from qiime2.plugin import (Plugin, Citations)

from q2_fondue import __version__

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

# TODO: register all the methods below
