# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from typing import List

import pandas as pd

import entrezpy.efetch.efetcher as ef
from qiime2.core.type import SemanticType
from qiime2.plugin import model

# TODO: clean up those formats/types
from q2_fondue._entrezpy_clients import EFetchAnalyzer


class TestMetadataFormat(model.TextFileFormat):
    def _validate(self, n_records=None):
        pass

    def _validate_(self, level):
        self._validate()


TestMetadataDirFmt = model.SingleFileDirectoryFormat(
    'TestMetadataDirFmt', 'metadata.tsv', TestMetadataFormat
)

TestMetadata = SemanticType('TestMetadata')


def _efetcher_inquire(
        efetcher: ef.Efetcher, study_ids: List[str]) -> pd.DataFrame:
    # TODO: some error handling here
    metadata_response = efetcher.inquire(
        {
            'db': 'sra',
            'id': study_ids,  # this has to be a list
            'rettype': 'xml',
            'retmode': 'text'
        }, analyzer=EFetchAnalyzer()
    )
    return metadata_response.result.to_df()


def get_metadata(
        study_ids: list, email: str, n_jobs: int = 1) -> pd.DataFrame:
    efetcher = ef.Efetcher(
        'efetcher', email, apikey=None,
        apikey_var=None, threads=n_jobs, qid=None
    )

    return _efetcher_inquire(efetcher, study_ids)
