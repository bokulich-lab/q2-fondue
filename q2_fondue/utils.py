# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import gzip
import os
import shutil
import signal
import subprocess
from typing import List

from entrezpy.esearch import esearcher as es
from tqdm import tqdm

from q2_fondue.entrezpy_clients._esearch import ESearchAnalyzer
from q2_fondue.entrezpy_clients._utils import (
    PREFIX, InvalidIDs, set_up_logger, set_up_entrezpy_logging)

LOGGER = set_up_logger('INFO', logger_name=__name__)


class DownloadError(Exception):
    pass


def _chunker(seq, size):
    # source: https://stackoverflow.com/a/434328/579416
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def _validate_run_ids(
        email: str, n_jobs: int, run_ids: List[str], log_level: str) -> dict:
    """Validates provided accession IDs using ESearch.

    Args:
        email (str): A valid e-mail address.
        n_jobs (int): Number of threads to be used in parallel.
        run_ids (List[str]): List of all the run IDs to be validated.
        log_level (str): Logging level.

    Returns:
        dict: Dictionary of invalid IDs (as keys) with a description.
    """
    # must process in batches because esearch requests with
    # runID count > 10'000 fail
    invalid_ids = {}
    for batch in _chunker(run_ids, 10000):
        esearcher = es.Esearcher(
            'esearcher', email, apikey=None,
            apikey_var=None, threads=0, qid=None
        )
        set_up_entrezpy_logging(esearcher, log_level)

        esearch_response = esearcher.inquire(
            {
                'db': 'sra',
                'term': " OR ".join(batch),
                'usehistory': False
            }, analyzer=ESearchAnalyzer(batch)
        )
        invalid_ids.update(esearch_response.result.validate_result())

    return invalid_ids


def _determine_id_type(ids: list):
    ids = [x[:3] for x in ids]
    for kind in PREFIX.keys():
        if all([x in PREFIX[kind] for x in ids]):
            return kind
    raise InvalidIDs('The type of provided IDs is either not supported or '
                     'IDs of mixed types were provided. Please provide IDs '
                     'corresponding to either SRA run (#S|E|DRR), study '
                     '(#S|E|DRP) or NCBI BioProject IDs (#PRJ).')


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


def _has_enough_space(acc_id: str, output_dir: str) -> bool:
    """Checks whether there is enough storage available for fasterq-dump
        to process sequences for a given ID.

    fasterq-dump will be used to check the amount of space required for the
    final data. Required space is estimated as 10x that of the final data
    (as per NCBI's documentation).

    Args:
        acc_id (str): The accession ID to be processed.
        output_dir (str): Location where the output would be saved.

    Return
        bool: Whether there is enough space available for fasterq-dump tool.
    """
    if acc_id is None:
        return True

    cmd_fasterq = ['fasterq-dump', '--size-check', 'only', '-x', acc_id]
    result = subprocess.run(
        cmd_fasterq, text=True, capture_output=True, cwd=output_dir
    )

    if result.returncode == 0:
        return True
    elif result.returncode == 3 and 'disk-limit exeeded' in result.stderr:
        LOGGER.warning('Not enough space to fetch run %s.', acc_id)
        return False
    else:
        LOGGER.error(
            'fasterq-dump exited with a "%s" error code (the message '
            'was: "%s"). We will try to fetch the next accession ID.',
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


def _rewrite_fastq(file_in: str, file_out: str):
    with open(file_in, 'rb') as f_in, gzip.open(file_out, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
