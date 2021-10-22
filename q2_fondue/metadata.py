# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import logging
import sys
from typing import List

import entrezpy.conduit as ec
import entrezpy.efetch.efetcher as ef
import entrezpy.esearch.esearcher as es
import pandas as pd
from qiime2 import Metadata

from q2_fondue.entrezpy_clients._efetch import EFetchAnalyzer
from q2_fondue.entrezpy_clients._elink import ELinkAnalyzer
from q2_fondue.entrezpy_clients._esearch import ESearchAnalyzer
from q2_fondue.entrezpy_clients._utils import PREFIX


class InvalidIDs(Exception):
    pass


def _efetcher_inquire(
        efetcher: ef.Efetcher, sample_ids: List[str], id_type: str
) -> pd.DataFrame:
    """Makes an EFetch request using the provided IDs.

    Args:
        efetcher (ef.Efetcher): A valid instance of an Entrezpy Efetcher.
        sample_ids (List[str]): List of all the sample IDs to be fetched.
        id_type (str): Type of provided IDs (e.g.: sample or run)

    Returns:
        pd.DataFrame: DataFrame with metadata obtained for the provided IDs.

    """
    # TODO: this is just temporary - for debugging purposes;
    #  we should really make Entrez logging configurable
    efetcher.logger.addHandler(logging.StreamHandler(sys.stdout))
    efetcher.logger.setLevel('DEBUG')
    efetcher.request_pool.logger.addHandler(logging.StreamHandler(sys.stdout))
    efetcher.request_pool.logger.setLevel('DEBUG')

    metadata_response = efetcher.inquire(
        {
            'db': 'sra',
            'id': sample_ids,  # this has to be a list
            'rettype': 'xml',
            'retmode': 'text'
        }, analyzer=EFetchAnalyzer(id_type)
    )
    return metadata_response.result.metadata_to_df()


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


def _determine_id_type(ids: list):
    ids = [x[:3] for x in ids]
    for kind in ('run', 'sample', 'bioproject'):
        if all([x in PREFIX[kind] for x in ids]):
            return kind
    raise InvalidIDs('The type of provided IDs is either not supported or '
                     'IDs of mixed types were provided. Please provide IDs '
                     'corresponding to either SRA runs (#SRR) or SRA '
                     'samples (#SRS).')


def _get_sample_or_run_meta(email, n_jobs, sample_ids, id_type):
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
    return _efetcher_inquire(efetcher, sample_ids, id_type)


def _get_project_meta(email, n_jobs, project_ids):
    econduit = ec.Conduit(email=email, threads=n_jobs)

    # TODO: create a separate function to set this all up everywhere
    handler = logging.StreamHandler(sys.stdout)
    econduit.logger.setLevel("DEBUG")
    econduit.logger.addHandler(handler)

    samp_ids_pipeline = econduit.new_pipeline()

    # search for project IDs
    es = samp_ids_pipeline.add_search(
        {'db': 'bioproject', 'term': " OR ".join(project_ids)},
        analyzer=ESearchAnalyzer(project_ids)
    )
    # given bioproject, find linked SRA runs
    el = samp_ids_pipeline.add_link(
        {'db': 'sra'},
        analyzer=ELinkAnalyzer(), dependency=es
    )
    # given SRA run IDs, fetch all metadata
    samp_ids_pipeline.add_fetch(
        {'rettype': 'docsum', 'retmode': 'xml', 'retmax': 10000},
        analyzer=EFetchAnalyzer(id_type='run'), dependency=el
    )

    a = econduit.run(samp_ids_pipeline)
    run_ids = a.result.metadata_to_series().tolist()

    return _get_sample_or_run_meta(email, n_jobs, run_ids, 'run')


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
    # Retrieve input IDs
    sample_ids = list(sample_ids.get_ids())

    # figure out if we're dealing with sample or run ids
    id_type = _determine_id_type(sample_ids)

    if id_type in ['sample', 'run']:
        return _get_sample_or_run_meta(email, n_jobs, sample_ids, id_type)

    elif id_type == 'bioproject':
        return _get_project_meta(email, n_jobs, sample_ids)
