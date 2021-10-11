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
import entrezpy.esearch.esearcher as es

from q2_fondue.entrezpy_clients._efetch import EFetchAnalyzer
from q2_fondue.entrezpy_clients._esearch import ESearchAnalyzer
from qiime2 import Metadata


def _efetcher_inquire(
        efetcher: ef.Efetcher, sample_ids: List[str]) -> pd.DataFrame:
    """Makes an EFetch request using the provided IDs.

    Args:
        efetcher (ef.Efetcher): A valid instance of an Entrezpy Efetcher.
        sample_ids (List[str]): List of all the sample IDs to be fetched.

    Returns:
        pd.DataFrame: DataFrame with metadata obtained for the provided IDs.

    """
    metadata_response = efetcher.inquire(
        {
            'db': 'sra',
            'id': sample_ids,  # this has to be a list
            'rettype': 'xml',
            'retmode': 'text'
        }, analyzer=EFetchAnalyzer()
    )
    return metadata_response.result.to_df()


def _validate_esearch_result(
        esearcher: es.Esearcher, sample_ids: List[str]) -> bool:
    """Validates provided accession IDs using ESearch.

    Args:
        esearcher (es.Esearcher): A valid instance of an Entrezpy Esearcher.
        sample_ids (List[str]): List of all the sample IDs to be validated.

    Returns:
        bool: True if all the IDs are valid.

    """
    esearch_response = esearcher.inquire(
        {
            'db': 'sra',
            'term': " OR ".join(sample_ids),
            'usehistory': False
        }, analyzer=ESearchAnalyzer(sample_ids)
    )

    return esearch_response.result.validate_result()


def get_metadata(
        sample_ids: Metadata, email: str, n_jobs: int = 1) -> pd.DataFrame:
    """Fetches metadata using the provided sample/run accession IDs.

    The IDs will be first validated using an ESearch query. The metadata
    will be fetched only if all the IDs are valid. Otherwise, the user
    will be informed on which IDs require checking.

    Args:
        sample_ids (Metadata): List of all the sample IDs to be fetched.
        email (str): A valid e-mail address (required by NCBI).
        n_jobs (int, default=1): Number of threads to be used in parallel.

    Returns:
        pd.DataFrame: DataFrame with metadata obtained for the provided IDs.

    """
    # validate the ids
    esearcher = es.Esearcher(
        'esearcher', email, apikey=None,
        apikey_var=None, threads=n_jobs, qid=None
    )
    _validate_esearch_result(esearcher, sample_ids)

    efetcher = ef.Efetcher(
        'efetcher', email, apikey=None,
        apikey_var=None, threads=n_jobs, qid=None
    )
    return _efetcher_inquire(efetcher, sample_ids)
