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

from q2_fondue._entrezpy_clients import EFetchAnalyzer


def _efetcher_inquire(
        efetcher: ef.Efetcher, study_ids: List[str]) -> pd.DataFrame:
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
