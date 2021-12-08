# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from typing import List, Tuple

import entrezpy.efetch.efetcher as ef
import entrezpy.esearch.esearcher as es
import pandas as pd
from qiime2 import Metadata

from q2_fondue.entrezpy_clients._efetch import EFetchAnalyzer
from q2_fondue.utils import (
    _validate_esearch_result, _determine_id_type
)
from q2_fondue.entrezpy_clients._utils import (set_up_entrezpy_logging,
                                               set_up_logger)
from q2_fondue.entrezpy_clients._pipelines import _get_run_ids_from_projects


def _efetcher_inquire(
        efetcher: ef.Efetcher, run_ids: List[str], log_level: str
) -> Tuple[pd.DataFrame, list]:
    """Makes an EFetch request using the provided IDs.

    Args:
        efetcher (ef.Efetcher): A valid instance of an Entrezpy Efetcher.
        run_ids (List[str]): List of all the run IDs to be fetched.

    Returns:
        pd.DataFrame: DataFrame with metadata obtained for the provided IDs.
        list: List of all the run IDs that were not found.
    """
    metadata_response = efetcher.inquire(
        {
            'db': 'sra',
            'id': run_ids,
            'rettype': 'xml',
            'retmode': 'text'
        }, analyzer=EFetchAnalyzer(log_level)
    )

    return (
        metadata_response.result.metadata_to_df(),
        metadata_response.result.missing_uids
    )


def _execute_efetcher(email, n_jobs, run_ids, log_level):
    efetcher = ef.Efetcher(
        'efetcher', email, apikey=None,
        apikey_var=None, threads=n_jobs, qid=None
    )
    set_up_entrezpy_logging(efetcher, log_level)
    meta_df, missing_ids = _efetcher_inquire(efetcher, run_ids, log_level)
    return meta_df, missing_ids


def _get_run_meta(email, n_jobs, run_ids, log_level, logger):
    # validate the IDs
    esearcher = es.Esearcher(
        'esearcher', email, apikey=None,
        apikey_var=None, threads=n_jobs, qid=None
    )
    set_up_entrezpy_logging(esearcher, log_level)
    _validate_esearch_result(esearcher, run_ids)

    # fetch metadata
    meta_df, missing_ids = _execute_efetcher(email, n_jobs, run_ids, log_level)

    # when hundreds of runs were requested, it could happen that not all
    # metadata will be fetched - in that case, keep running efetcher
    # until all runs are retrieved
    meta_df = [meta_df]
    retries = 20
    while missing_ids and retries > 0:
        logger.info(
            f'{len(missing_ids)} missing IDs were found - we will '
            f'retry fetching those ({retries} retries left).'
        )
        df, missing_ids = _execute_efetcher(
            email, n_jobs, missing_ids, log_level
        )
        meta_df.append(df)
        retries -= 1

    if retries == 0 and missing_ids:
        logger.warning(
            'Metadata for the following run IDs could not be fetched: '
            f'{",".join(missing_ids)}. '
            f'Please try fetching those independently.'
        )

    return pd.concat(meta_df, axis=0)


def _get_project_meta(email, n_jobs, project_ids, log_level, logger):
    run_ids = _get_run_ids_from_projects(email, n_jobs, project_ids, log_level)
    return _get_run_meta(email, n_jobs, run_ids, log_level, logger)


def get_metadata(
        accession_ids: Metadata, email: str,
        n_jobs: int = 1, log_level: str = 'INFO'
) -> pd.DataFrame:
    """Fetches metadata using the provided run/bioproject accession IDs.

    The IDs will be first validated using an ESearch query. The metadata
    will be fetched only if all the IDs are valid. Otherwise, the user
    will be informed on which IDs require checking.

    Args:
        accession_ids (Metadata): List of all the run/project IDs
            to be fetched.
        email (str): A valid e-mail address (required by NCBI).
        n_jobs (int, default=1): Number of threads to be used in parallel.
        log_level (str, default='INFO'): Logging level.

    Returns:
        pd.DataFrame: DataFrame with metadata obtained for the provided IDs.

    """
    logger = set_up_logger(log_level, logger_name=__name__)

    # Retrieve input IDs
    accession_ids = sorted(list(accession_ids.get_ids()))

    # figure out if we're dealing with project or run ids
    id_type = _determine_id_type(accession_ids)

    if id_type == 'run':
        return _get_run_meta(email, n_jobs, accession_ids, log_level, logger)

    elif id_type == 'bioproject':
        return _get_project_meta(
            email, n_jobs, accession_ids, log_level, logger
        )
