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

from entrezpy import conduit as ec
from entrezpy.esearch import esearcher as es

from q2_fondue.entrezpy_clients._efetch import EFetchAnalyzer
from q2_fondue.entrezpy_clients._elink import ELinkAnalyzer
from q2_fondue.entrezpy_clients._esearch import ESearchAnalyzer
from q2_fondue.entrezpy_clients._utils import PREFIX


class InvalidIDs(Exception):
    pass


def _validate_esearch_result(
        esearcher: es.Esearcher, run_ids: List[str]) -> bool:
    """Validates provided accession IDs using ESearch.

    Args:
        esearcher (es.Esearcher): A valid instance of an Entrezpy Esearcher.
        run_ids (List[str]): List of all the run IDs to be validated.

    Returns:
        bool: True if all the IDs are valid.

    """
    esearch_response = esearcher.inquire(
        {
            'db': 'sra',
            'term': " OR ".join(run_ids),
            'usehistory': False
        }, analyzer=ESearchAnalyzer(run_ids)
    )

    return esearch_response.result.validate_result()


def _determine_id_type(ids: list):
    ids = [x[:3] for x in ids]
    for kind in ('run', 'bioproject'):
        if all([x in PREFIX[kind] for x in ids]):
            return kind
    raise InvalidIDs('The type of provided IDs is either not supported or '
                     'IDs of mixed types were provided. Please provide IDs '
                     'corresponding to either SRA runs (#SRR) or NCBI '
                     'BioProject IDs (#PRJ).')


def set_up_entrezpy_logging(entrezpy_obj, log_level):
    """Sets up logging for the given Entrezpy object.

    Args:
        entrezpy_obj (object): An Entrezpy object that has a logger attribute.
        log_level (str): The log level to set.
    """
    handler = set_up_logging_handler(log_level)

    for logger in (entrezpy_obj.logger, entrezpy_obj.request_pool.logger):
        logger.addHandler(handler)
        logger.setLevel(log_level)


def set_up_logger(log_level, cls_obj=None):
    """Sets up the module logger.

    Args:
        log_level (str): The log level to set.
        cls_obj: Class instance for which the logger should be created.

    Returns:
        logging.Logger: The module logger.
    """
    if cls_obj:
        logger = logging.getLogger(
            f'{cls_obj.__module__}.{cls_obj.__qualname__}'
        )
    else:
        logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    handler = set_up_logging_handler(log_level)
    logger.addHandler(handler)
    return logger


def set_up_logging_handler(log_level):
    """Sets up logging handler.

    Args:
        log_level (str): The log level to set.
    """
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)s [%(threadName)s] [%(levelname)s] '
        '[%(name)s]: %(message)s')
    handler.setFormatter(formatter)
    return handler


def _get_run_ids_from_projects(email, n_jobs, project_ids, log_level) -> list:
    econduit = ec.Conduit(email=email, threads=n_jobs)
    set_up_entrezpy_logging(econduit, log_level)

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
        analyzer=EFetchAnalyzer(), dependency=el
    )

    a = econduit.run(samp_ids_pipeline)
    return a.result.metadata_to_series().tolist()
