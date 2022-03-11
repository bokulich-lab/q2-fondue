# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import os
import signal
import subprocess
from typing import List

from entrezpy.esearch import esearcher as es
from tqdm import tqdm

from q2_fondue.entrezpy_clients._esearch import ESearchAnalyzer
from q2_fondue.entrezpy_clients._utils import PREFIX, InvalidIDs, set_up_logger

LOGGER = set_up_logger('INFO', logger_name=__name__)


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


def _has_enough_space(acc_id: str, output_dir: str, free_space: int) -> bool:
    """Checks whether there is enough storage available for fasterq-dump
        to process sequences for a given ID.

    vdb-dump will be used to check the amount of space required for the final
    data. Required space is estimated as 10x that of the final data (as per
    NCBI's documentation).

    Args:
        acc_id (str): The accession ID to be processed.
        output_dir (str): Location where the output would be saved.
        free_space (int): The amount of free space available on the disk.

    Return
        bool: Whether there is enough space available for fasterq-dump tool.
    """
    if acc_id is None:
        return True

    cmd_vdb = ['vdb-dump', '--info', acc_id]
    result = subprocess.run(
        cmd_vdb, text=True, capture_output=True, cwd=output_dir
    )

    if result.returncode == 0:
        # convert into dict, should we ever need that in the future
        stdout = result.stdout.split('\n')
        acc_info = {
            k.strip(): v.strip() for
            [k, v] in [
                x.split(':', maxsplit=1) for x in stdout if ':' in x
            ]
        }
        # as per the docs:
        # https://github.com/ncbi/sra-tools/wiki/HowTo:-fasterq-dump
        req_space = 10 * int(acc_info.get('size').replace(',', ''))
        LOGGER.debug(
            'Space required for %s: %sMB (space available: %sMB)',
            acc_id, round(req_space/10**6, 1), round(free_space/10**6, 1)
        )
        return True if req_space < free_space else False
    else:
        LOGGER.error(
            'vdb-dump exited with a "%s" error code (the message was: "%s"). '
            'We will try to fetch the next accession ID.',
            result.returncode, result.stderr
        )
        return True


def _find_next_id(acc_id: str, progress_bar: tqdm):
    pbar_content = list(progress_bar)
    index_next_acc = pbar_content.index(acc_id) + 1
    if index_next_acc >= len(pbar_content):
        return None
    else:
        return pbar_content[index_next_acc]
