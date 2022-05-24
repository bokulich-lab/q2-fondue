# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from setuptools import find_packages, setup

import versioneer

setup(
    name='q2-fondue',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    license='BSD-3-Clause',
    packages=find_packages(),
    author="Michal Ziemski",
    author_email="ziemski.michal@gmail.com",
    description=(
        'This is a QIIME 2 plugin for fetching raw sequencing data and'
        'its associated metadata from data archives like SRA.'
    ),
    url="https://github.com/bokulich-lab/q2-fondue",
    entry_points={
        'qiime2.plugins':
        ['q2-fondue=q2_fondue.plugin_setup:plugin']
    },
    package_data={
        'q2_fondue': ['citations.bib'],
        'q2_fondue.tests': ['data/*', 'data/*/*'],
        'q2_fondue.types.tests': ['data/*']
    },
    zip_safe=False,
)
