# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
from multiprocessing.managers import SyncManager

import glob
from multiprocessing import Pool, Queue, Process, Manager, cpu_count

import gzip
import itertools
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
from qiime2 import Metadata
from warnings import warn

import pandas as pd
from q2_types.per_sample_sequences import \
    (CasavaOneEightSingleLanePerSampleDirFmt)
from tqdm import tqdm

from q2_fondue.entrezpy_clients._pipelines import _get_run_ids
from q2_fondue.entrezpy_clients._utils import set_up_logger
from q2_fondue.utils import (
    _determine_id_type, handle_threaded_exception, DownloadError,
    _has_enough_space, _find_next_id
)

threading.excepthook = handle_threaded_exception

LOGGER = set_up_logger('INFO', logger_name=__name__)


def _run_cmd_fasterq(
        acc: str, output_dir: str, threads: int):
    """Runs fasterq-dump command on a single accession."""
    cmd_prefetch = ['prefetch', '-X', 'u', '-O', acc, acc]
    cmd_fasterq = [
        'fasterq-dump', '-e', str(threads), '--size-check', 'on', '-x', acc
    ]

    result = subprocess.run(
        cmd_prefetch, text=True, capture_output=True, cwd=output_dir)

    if result.returncode == 0:
        result = subprocess.run(
            cmd_fasterq, text=True, capture_output=True, cwd=output_dir)
        # clean up prefetch files on success
        if result.returncode == 0:
            sra_path = os.path.join(output_dir, acc)
            if os.path.isdir(sra_path):
                shutil.rmtree(sra_path)
            elif os.path.isfile(f'{sra_path}.sra'):
                os.remove(f'{sra_path}.sra')
        elif result.returncode == 3 and 'disk-limit exeeded' in result.stderr:
            LOGGER.error(
                'Not enough space for fasterq-dump to process ID=%s.', acc
            )
    return result


def _get_remaining_ids_with_storage_error(acc_id: str, progress_bar: tqdm):
    """Finds remaining ids and appends storage exhaustion error message."""
    index_next_acc = list(progress_bar).index(acc_id) + 1
    remaining_ids = list(progress_bar)[index_next_acc:]
    remaining_ids = dict(zip(
        remaining_ids, len(remaining_ids) * ['Storage exhausted.']
    ))
    return remaining_ids


def _run_fasterq_dump_for_all(
        accession_ids, tmpdirname, threads, retries,
        fetched_queue, done_queue
):
    """Runs prefetch & fasterq-dump for all ids in accession_ids.

    Args:
        accession_ids (list): List of all run IDs to be fetched.
        tmpdirname (str): Name of temporary directory to store the data.
        threads (int, default=1): Number of threads to be used in parallel.
        retries (int, default=2): Number of retries to fetch sequences.
        fetched_queue (multiprocessing.Queue): Queue communicating IDs
            that were successfully fetched.
        done_queue (SyncManager.Queue): Queue communicating filenames
            that were completely processed.

    Returns:
        failed_ids (dict): Failed run IDs with corresponding errors.
    """
    LOGGER.info(
        f'Downloading sequences for {len(accession_ids)} accession IDs...'
    )
    accession_ids_init = accession_ids.copy()
    init_retries = retries
    _, _, init_free_space = shutil.disk_usage(tmpdirname)

    while (retries >= 0) and (len(accession_ids) > 0):
        failed_ids = {}
        pbar = tqdm(sorted(accession_ids))
        for acc in pbar:
            pbar.set_description(
                f'Downloading sequences for run {acc} '
                f'(attempt {-retries + init_retries + 1})'
            )
            result = _run_cmd_fasterq(
                acc, tmpdirname, threads)
            if result.returncode != 0:
                failed_ids[acc] = result.stderr
            else:
                fetched_queue.put(acc)
            pbar.postfix = f'{len(failed_ids)} failed'

            # check space availability
            _, _, free_space = shutil.disk_usage(tmpdirname)
            used_seq_space = init_free_space - free_space
            # current space threshold: 35% of fetched seq space as evaluated
            # from 6 random run and ProjectIDs
            if free_space < (0.35 * used_seq_space) and not \
                    _has_enough_space(_find_next_id(acc, pbar), tmpdirname):
                failed_ids.update(
                    _get_remaining_ids_with_storage_error(acc, pbar))
                LOGGER.warning(
                    'Available storage was exhausted - there will be no '
                    'more retries.'
                )
                retries = -1
                break

        if len(failed_ids.keys()) > 0 and retries > 0:
            # log & add time buffer if we retry
            sleep_lag = (1 / (retries + 1)) * 180
            ls_failed_ids = list(failed_ids.keys())
            LOGGER.info(
                f'Retrying to download the following failed accession IDs in '
                f'{round(sleep_lag / 60, 1)} min: {ls_failed_ids}'
            )
            time.sleep(sleep_lag)

        accession_ids = failed_ids.copy()
        retries -= 1

    msg = 'Download finished.'
    fetched_queue.put(None)
    if failed_ids:
        errors = '\n'.join(
            [f'ID={x}, Error={y}' for x, y in list(failed_ids.items())[:5]]
        )
        msg += f' {len(failed_ids.keys())} out of {len(accession_ids_init)} ' \
               f'runs failed to fetch. Below are the error messages of the ' \
               f"first 5 failed runs:\n{errors}"
    LOGGER.info(msg)
    done_queue.put({'failed_ids': failed_ids})


def _process_one_sequence(filename, output_dir):
    """Renames sequence files to follow the required naming convention."""
    new_name, is_paired = None, False
    if filename.endswith('_1.fastq'):
        # paired-end _1
        acc = re.search(r'(.*)_1\.fastq$', filename).group(1)
        new_name, is_paired = '%s_00_L001_R1_001.fastq' % acc, True
    elif filename.endswith('_2.fastq'):
        # paired-end _2
        acc = re.search(r'(.*)_2\.fastq$', filename).group(1)
        new_name, is_paired = '%s_00_L001_R2_001.fastq' % acc, True
    elif filename.endswith('.fastq'):
        # single-reads
        acc = re.search(r'(.*)\.fastq$', filename).group(1)
        new_name, is_paired = '%s_00_L001_R1_001.fastq' % acc, False
    else:
        return new_name, is_paired
    os.rename(os.path.join(output_dir, filename),
              os.path.join(output_dir, new_name))
    return new_name, is_paired


def _process_downloaded_sequences(
        output_dir, fetched_queue, renaming_queue, n_workers
):
    """Processes downloaded sequences.

    Picks up filenames of fetched sequences from the fetched_queue, renames
    them and inserts processed filenames into the renaming_queue when finished.
    """
    for _id in iter(fetched_queue.get, None):
        filenames = glob.glob(os.path.join(output_dir, f'{_id}*.fastq'))
        filenames = [
            _process_one_sequence(f, output_dir) for f in filenames
        ]
        single = [x for x in filenames if not x[1]]
        paired = sorted([x for x in filenames if x[1]])

        renaming_queue.put(single) if single else False
        renaming_queue.put(paired) if paired else False

    # tell all the workers we are done
    [renaming_queue.put(None) for i in range(n_workers)]


def _read_fastq_seqs(filepath):
    # function adapted from q2_demux._demux import _read_fastq_seqs

    # Originally func is adapted from @jairideout's SO post:
    # http://stackoverflow.com/a/39302117/3424666
    fh = open(filepath, 'rt')
    for seq_header, seq, qual_header, qual in itertools.zip_longest(*[fh] * 4):
        yield (seq_header.strip(), seq.strip(), qual_header.strip(),
               qual.strip())


def _write_empty_casava(read_type, casava_out_path):
    """Writes empty casava file to output directory.

    Warns about `read_type` sequences that are not available
    and saves empty casava file.
    """
    warn_msg = f'No {read_type}-read sequences available ' \
               f'for these accession IDs.'
    warn(warn_msg)
    LOGGER.warning(warn_msg)

    if read_type == 'single':
        ls_file_names = ['xxx_00_L001_R1_001.fastq.gz']
    else:
        ls_file_names = ['xxx_00_L001_R1_001.fastq.gz',
                         'xxx_00_L001_R2_001.fastq.gz']
    # create empty CasavaDirFmt due to Q2 not supporting optional
    # output types
    for new_empty_name in ls_file_names:
        path_out = str(casava_out_path.path / new_empty_name)
        with gzip.open(str(path_out), mode='w'):
            pass


def _copy_single_to_casava(
        filename, tmp_dir, casava_result_path
):
    """Copies single-end sequences to Casava directory.

    Downloaded sequence files will be copied from tmp_dir to
    casava_result_path following single-end sequence rules.
    """
    # Edited from original in: q2_demux._subsample.subsample_single
    # ensure correct order of file names:
    fwd_path_in = os.path.join(tmp_dir, filename)
    fwd_path_out = os.path.join(casava_result_path, f'{filename}.gz')

    with gzip.open(fwd_path_out, mode='w') as fwd:
        for fwd_rec in _read_fastq_seqs(fwd_path_in):
            fwd.write(('\n'.join(fwd_rec) + '\n').encode('utf-8'))


def _copy_paired_to_casava(
        filenames, tmp_dir, casava_result_path
):
    """Copies paired-end sequences to Casava directory.

    Downloaded sequence files will be copied from tmp_dir to
    casava_result_path following paired-end sequence rules.
    """
    # Edited from original in: q2_demux._subsample.subsample_paired
    # ensure correct order of file names:
    fwd_path_in = os.path.join(tmp_dir, filenames[0])
    fwd_path_out = os.path.join(casava_result_path, f'{filenames[0]}.gz')
    rev_path_in = os.path.join(tmp_dir, filenames[1])
    rev_path_out = os.path.join(casava_result_path, f'{filenames[1]}.gz')

    with gzip.open(fwd_path_out, mode='w') as fwd, \
            gzip.open(rev_path_out, mode='w') as rev:
        file_pair = zip(
            _read_fastq_seqs(fwd_path_in), _read_fastq_seqs(rev_path_in)
        )
        for fwd_rec, rev_rec in file_pair:
            fwd.write(('\n'.join(fwd_rec) + '\n').encode('utf-8'))
            rev.write(('\n'.join(rev_rec) + '\n').encode('utf-8'))


def _write2casava_dir(
        tmp_dir, casava_out_single, casava_out_paired,
        renaming_queue, done_queue
):
    """Writes single- or paired-end files to casava directory.

    Picks up jobs (filenames) from the renaming_queue and decides whether they
    should be processed as single- or paired-end files.
    For example, [('fileA', False)] would be processed as single-end,
    while [('fileB_1', True), ('fileB_2', True)] as paired-end.
    When done, it inserts filenames into the done_queue to announce completion.
    """
    for filenames in iter(renaming_queue.get, None):
        if len(filenames) == 1:
            filename = os.path.split(filenames[0][0])[-1]
            _copy_single_to_casava(filename, tmp_dir, casava_out_single)
            done_queue.put([filename])
        elif len(filenames) == 2:
            filenames = [
                os.path.split(x[0])[-1] for x in sorted(filenames)
            ]
            _copy_paired_to_casava(filenames, tmp_dir, casava_out_paired)
            done_queue.put(filenames)
        renaming_queue.task_done()
    return True


def _announce_completion(queue: SyncManager.Queue):
    """Announces that processing is finished by inserting a None value into
        a queue and retrieve its all elements.

    List of filenames will be retrieved and assigned to either a single- or
    paired-end list of outputs. The single dictionary containing failed IDs
    will also be retrieved and returned.

    Args:
        queue (SyncManager.Queue): an instance of a queue which should
            be processed

    Returns:
        failed_ids (list): List of all failed IDs.
        single_files (list): Filename list for all single-end reads.
        paired_files (list): Filename list for all paired-end reads.
    """
    queue.put(None)
    results = []
    failed_ids = {}
    for i in iter(queue.get, None):
        if isinstance(i, list):
            results.append(i)
        elif isinstance(i, dict):
            failed_ids = i['failed_ids']
    single_files = [x for x in results if len(x) == 1]
    paired_files = [x for x in results if len(x) == 2]
    return failed_ids, single_files, paired_files


def get_sequences(
        accession_ids: Metadata, email: str, retries: int = 2,
        n_jobs: int = 1, log_level: str = 'INFO',
) -> (CasavaOneEightSingleLanePerSampleDirFmt,
      CasavaOneEightSingleLanePerSampleDirFmt,
      pd.DataFrame):
    """
    Fetches single-read and paired-end sequences based on provided
    accession IDs.

    Function uses SRA-toolkit fasterq-dump to get single-read and paired-end
    sequences of accession IDs. It supports multiple tries (`retries`)
    and can use multiple `threads`. If download fails, function will create
    an artifact with a list of failed IDs.

    Args:
        accession_ids (Metadata): List of all run/project IDs to be fetched.
        email (str): A valid e-mail address (required by NCBI).
        retries (int, default=2): Number of retries to fetch sequences.
        n_jobs (int, default=1): Number of threads to be used in parallel.
        log_level (str, default='INFO'): Logging level.

    Returns:
        Two directories with fetched single-read and paired-end sequences
        respectively for provided accession IDs. If the provided accession IDs
        only contain one type of sequences (single-read or paired-end) the
        other directory is empty (with artificial ID starting with xxx_)

        failed_ids (pd.DataFrame): Run IDs that failed to download with errors.
    """
    LOGGER.setLevel(log_level.upper())

    casava_out_single = CasavaOneEightSingleLanePerSampleDirFmt()
    casava_out_paired = CasavaOneEightSingleLanePerSampleDirFmt()

    accession_ids = list(accession_ids.get_ids())

    id_type = _determine_id_type(accession_ids)
    if id_type == 'bioproject':
        accession_ids = _get_run_ids(
            email, n_jobs, accession_ids, id_type, log_level
        )

    fetched_q = Queue()
    manager = Manager()
    renamed_q = manager.Queue()
    processed_q = manager.Queue()

    with tempfile.TemporaryDirectory() as tmp_dir:
        # run fasterq-dump for all accessions
        fetcher_process = Process(
            target=_run_fasterq_dump_for_all,
            args=(
                sorted(accession_ids), tmp_dir, n_jobs, retries,
                fetched_q, processed_q
            ),
            daemon=True
        )
        # processing downloaded files
        worker_count = int(min(max(n_jobs - 1, 1), cpu_count() - 1))
        LOGGER.debug(f'Using {worker_count} workers.')
        renamer_process = Process(
            target=_process_downloaded_sequences,
            args=(tmp_dir, fetched_q, renamed_q, worker_count),
            daemon=True
        )
        # writing to Casava directory
        worker_pool = Pool(
            worker_count, _write2casava_dir,
            (tmp_dir, str(casava_out_single.path),
             str(casava_out_paired.path), renamed_q, processed_q)
        )

        # start all processes
        fetcher_process.start()
        renamer_process.start()
        worker_pool.close()

        # wait for all the results
        fetcher_process.join()
        renamer_process.join()
        worker_pool.join()

        # announce processing is done
        failed_ids, single_files, paired_files = \
            _announce_completion(processed_q)

        # make sure either of the sequences were downloaded
        if len(single_files) == 0 and len(paired_files) == 0:
            raise DownloadError(
                'Neither single- nor paired-end sequences could '
                'be downloaded. Please check your accession IDs.'
            )

        # write downloaded single-read seqs from tmp to casava dir
        if len(single_files) == 0:
            _write_empty_casava('single', casava_out_single)

        # write downloaded paired-end seqs from tmp to casava dir
        if len(paired_files) == 0:
            _write_empty_casava('paired', casava_out_paired)

    LOGGER.info('Processing finished.')

    failed_ids = pd.DataFrame(
        data={'Error message': failed_ids.values()},
        index=pd.Index(failed_ids.keys(), name='ID')
    )
    return casava_out_single, casava_out_paired, failed_ids


def combine_seqs(
        seqs: CasavaOneEightSingleLanePerSampleDirFmt,
        on_duplicates: str = 'error',
) -> CasavaOneEightSingleLanePerSampleDirFmt:
    """Combines paired- or single-end sequences from multiple artifacts.

    Args:
        seqs (CasavaOneEightSingleLanePerSampleDirFmt): A list of paired-
            or single-end sequences.
        on_duplicates (str, default='error'): If 'warn', function will warn
            about duplicated sequence IDs found in the input artifacts and
            proceed to combining inputs. If 'error', a ValueError will
            be raised.

    Returns:
        A directory containing sequences from all artifacts.
    """
    casava_out = CasavaOneEightSingleLanePerSampleDirFmt()
    error_on_duplicates = True if on_duplicates == 'error' else False

    all_files = pd.concat(
        objs=[seq_artifact.manifest for seq_artifact in seqs], axis=0
    )

    # exclude empty sequence files, in case any
    org_count = all_files.shape[0]
    all_files = all_files[~all_files.index.isin({'xxx'})]
    excluded_count = org_count - all_files.shape[0]
    if excluded_count:
        warn(f'{excluded_count} empty sequence files were found and excluded.')

    # check for duplicates
    duplicated = all_files.index.duplicated(keep='first')
    if any(duplicated):
        msg = 'Duplicate sequence files were found for the ' \
              'following IDs{}: %s.' % ", ".join(all_files[duplicated].index)
        if error_on_duplicates:
            raise ValueError(msg.format(''))
        else:
            warn(msg.format(' - duplicates will be dropped'))
            all_files = all_files[~duplicated]

    all_files.forward.apply(lambda x: shutil.move(x, casava_out.path))
    if all(all_files.reverse):
        all_files.reverse.apply(lambda x: shutil.move(x, casava_out.path))

    return casava_out
