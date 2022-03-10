# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import os
import signal
from typing import List

from entrezpy.esearch import esearcher as es

from q2_fondue.entrezpy_clients._esearch import ESearchAnalyzer
from q2_fondue.entrezpy_clients._utils import PREFIX, InvalidIDs, set_up_logger


class DownloadError(Exception):
    pass


def _validate_esearch_result(
        esearcher: es.Esearcher, run_ids: List[str], log_level: str) -> dict:
    """Validates provided accession IDs using ESearch.

    Args:
        esearcher (es.Esearcher): A valid instance of an Entrezpy Esearcher.
        run_ids (List[str]): List of all the run IDs to be validated.
        log_level (str): Logging level.

    Returns:
        dict: Dictionary of invalid IDs (as keys) with a description.
    """
    esearch_response = esearcher.inquire(
        {
            'db': 'sra',
            'term': " OR ".join(run_ids),
            'usehistory': False
        }, analyzer=ESearchAnalyzer(run_ids, log_level)
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


def handle_threaded_exception(args):
    logger = set_up_logger('DEBUG', logger_name='ThreadedErrorsManager')
    msg = 'Data fetching was interrupted by the following error: \n'

    if 'gaierror is not JSON serializable' in str(args.exc_value):
        msg += 'EntrezPy failed to connect to NCBI. Please check your ' \
               'internet connection and try again. It may help to wait ' \
               'a few minutes before retrying.'
    # silence threads exiting correctly
    elif issubclass(args.exc_type, SystemExit) and str(args.exc_value) == '0':
        return
    else:
        msg += f'Caught {args.exc_type} with value "{args.exc_value}" ' \
               f'in thread {args.thread}'

    logger.exception(msg)

    # This will send a SIGINT to the main thread, which will gracefully
    # kill the running Q2 action. No artifacts will be saved.
    os.kill(os.getpid(), signal.SIGINT)
