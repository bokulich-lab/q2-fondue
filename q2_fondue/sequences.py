# ----------------------------------------------------------------------------
# Copyright (c) 2022, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import gzip
import itertools
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
from warnings import warn

import pandas as pd
from q2_types.per_sample_sequences import \
    (CasavaOneEightSingleLanePerSampleDirFmt)
from qiime2 import Metadata
from tqdm import tqdm

from q2_fondue.entrezpy_clients._pipelines import _get_run_ids_from_projects
from q2_fondue.entrezpy_clients._utils import set_up_logger
from q2_fondue.utils import (_determine_id_type, handle_threaded_exception,
                             DownloadError)

threading.excepthook = handle_threaded_exception


def _run_cmd_fasterq(
        acc: str, output_dir: str, threads: int):
    """Runs fasterq-dump command on a single accession."""
    cmd_prefetch = ['prefetch', '-O', acc, acc]
    cmd_fasterq = ['fasterq-dump', '-e', str(threads), acc]

    result = subprocess.run(
        cmd_prefetch, text=True, capture_output=True, cwd=output_dir)

    if result.returncode == 0:
        result = subprocess.run(
            cmd_fasterq, text=True, capture_output=True, cwd=output_dir)
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
        accession_ids, tmpdirname, threads, retries, logger
) -> list:
    """Runs prefetch & fasterq-dump for all ids in accession_ids.

    Args:
        accession_ids (list): List of all run IDs to be fetched.
        tmpdirname (str): Name of temporary directory to store the data.
        threads (int, default=1): Number of threads to be used in parallel.
        retries (int, default=2): Number of retries to fetch sequences.
        logger (logging.Logger): An instance of a logger.

    Returns:
        failed_ids (list): List of failed run IDs.
    """
    logger.info(
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
            pbar.postfix = f'{len(failed_ids)} failed'

            # check space availability
            _, _, free_space = shutil.disk_usage(tmpdirname)
            used_seq_space = init_free_space - free_space
            # current space threshold: 35% of fetched seq space as evaluated
            # from 6 random run and ProjectIDs
            if free_space < (0.35 * used_seq_space):
                failed_ids.update(
                    _get_remaining_ids_with_storage_error(acc, pbar))
                logger.warning(
                    'Available storage was exhausted - there will be no '
                    'more retries.')
                retries = -1
                break

        if len(failed_ids.keys()) > 0 and retries > 0:
            # log & add time buffer if we retry
            sleep_lag = (1/(retries+1))*180
            ls_failed_ids = list(failed_ids.keys())
            logger.info(
                f'Retrying to download the following failed accession IDs in '
                f'{round(sleep_lag/60,1)} min: {ls_failed_ids}'
            )
            time.sleep(sleep_lag)

        accession_ids = failed_ids.copy()
        retries -= 1

    msg = 'Download finished.'
    if failed_ids:
        errors = '\n'.join(
            [f'ID={x}, Error={y}' for x, y in list(failed_ids.items())[:5]]
        )
        msg += f' {len(failed_ids.keys())} out of {len(accession_ids_init)} ' \
               f'runs failed to fetch. Below are the error messages of the ' \
               f"first 5 failed runs:\n{errors}"
    logger.info(msg)
    return list(failed_ids.keys())


def _process_downloaded_sequences(output_dir):
    """Processes downloaded sequences and returns a list of processed files.

    Renames single-read and paired-end sequences according to casava file
    format and outputs list of single-read and paired-end sequence
    filenames.
    """
    # rename all files to casava format & save single and paired
    # file names to list
    ls_single, ls_paired = [], []

    for filename in sorted(os.listdir(output_dir)):
        if filename.endswith('_1.fastq'):
            # paired-end _1
            acc = re.search(r'(.*)_1\.fastq$', filename).group(1)
            new_name = '%s_00_L001_R1_001.fastq' % acc
            ls_paired.append(new_name)
        elif filename.endswith('_2.fastq'):
            # paired-end _2
            acc = re.search(r'(.*)_2\.fastq$', filename).group(1)
            new_name = '%s_00_L001_R2_001.fastq' % acc
            ls_paired.append(new_name)
        elif filename.endswith('.fastq'):
            # single-reads
            acc = re.search(r'(.*)\.fastq$', filename).group(1)
            new_name = '%s_00_L001_R1_001.fastq' % acc
            ls_single.append(new_name)
        else:
            continue
        os.rename(os.path.join(output_dir, filename),
                  os.path.join(output_dir, new_name))

    return ls_single, ls_paired


def _read_fastq_seqs(filepath):
    # function adapted from q2_demux._demux import _read_fastq_seqs

    # Originally func is adapted from @jairideout's SO post:
    # http://stackoverflow.com/a/39302117/3424666
    fh = open(filepath, 'rt')
    for seq_header, seq, qual_header, qual in itertools.zip_longest(*[fh] * 4):
        yield (seq_header.strip(), seq.strip(), qual_header.strip(),
               qual.strip())


def _write_empty_casava(read_type, casava_out_path, logger):
    """Writes empty casava file to output directory.

    Warns about `read_type` sequences that are not available
    and saves empty casava file.
    """
    warn_msg = f'No {read_type}-read sequences available ' \
               f'for these accession IDs.'
    warn(warn_msg)
    logger.warning(warn_msg)

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


def _write2casava_dir_single(
        tmpdirname, casava_result_path, ls_files_2_consider):
    """Writes single-read files to casava directory.

    Downloaded sequence files will be copied from tmpdirname to
    casava_result_path following single read sequence rules.
    """
    # Edited from original in: q2_demux._subsample.subsample_single
    for filename in os.listdir(tmpdirname):
        if filename in ls_files_2_consider:
            fwd_path_in = os.path.join(tmpdirname, filename)
            filename_out = filename + '.gz'
            fwd_path_out = str(casava_result_path.path / filename_out)

            with gzip.open(str(fwd_path_out), mode='w') as fwd:
                for fwd_rec in _read_fastq_seqs(fwd_path_in):
                    fwd.write(('\n'.join(fwd_rec) + '\n').encode('utf-8'))


def _write2casava_dir_paired(
        tmpdirname, casava_result_path, ls_files_2_consider):
    """Writes paired-end files to casava directory.

    Downloaded sequence files will be copied from tmpdirname to
    casava_result_path following paired-end sequence rules.
    """
    # Edited from original in: q2_demux._subsample.subsample_paired
    # ensure correct order of file names:
    ls_files_sorted = sorted(ls_files_2_consider)

    # iterate and save
    for i in range(0, len(ls_files_sorted), 2):
        filename_1 = ls_files_sorted[i]
        filename_2 = ls_files_sorted[i+1]

        fwd_path_in = os.path.join(tmpdirname, filename_1)
        filename_1_out = filename_1 + '.gz'
        fwd_path_out = str(casava_result_path.path / filename_1_out)
        rev_path_in = os.path.join(tmpdirname, filename_2)
        filename_2_out = filename_2 + '.gz'
        rev_path_out = str(casava_result_path.path / filename_2_out)

        with gzip.open(str(fwd_path_out), mode='w') as fwd:
            with gzip.open(str(rev_path_out), mode='w') as rev:
                file_pair = zip(_read_fastq_seqs(fwd_path_in),
                                _read_fastq_seqs(rev_path_in))
                for fwd_rec, rev_rec in file_pair:
                    fwd.write(('\n'.join(fwd_rec) + '\n').encode('utf-8'))
                    rev.write(('\n'.join(rev_rec) + '\n').encode('utf-8'))


def get_sequences(
        accession_ids: Metadata, email: str, retries: int = 2,
        n_jobs: int = 1, log_level: str = 'INFO',
) -> (CasavaOneEightSingleLanePerSampleDirFmt,
      CasavaOneEightSingleLanePerSampleDirFmt,
      pd.Series):
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

        failed_ids (pd.Series): A list of run IDs that failed to download.
    """
    logger = set_up_logger(log_level, logger_name=__name__)

    casava_out_single = CasavaOneEightSingleLanePerSampleDirFmt()
    casava_out_paired = CasavaOneEightSingleLanePerSampleDirFmt()

    accession_ids = list(accession_ids.get_ids())

    id_type = _determine_id_type(accession_ids)
    if id_type == 'bioproject':
        accession_ids = _get_run_ids_from_projects(
            email, n_jobs, accession_ids, log_level
        )

    with tempfile.TemporaryDirectory() as tmpdirname:
        # run fasterq-dump for all accessions
        failed_ids = _run_fasterq_dump_for_all(
            accession_ids, tmpdirname, n_jobs, retries, logger)

        # processing downloaded files
        ls_single_files, ls_paired_files = _process_downloaded_sequences(
            tmpdirname)

        # make sure either of the sequences were downloaded
        if len(ls_single_files) == 0 and len(ls_paired_files) == 0:
            raise DownloadError(
                'Neither single- nor paired-end sequences could '
                'be downloaded. Please check your accession IDs.'
            )

        # write downloaded single-read seqs from tmp to casava dir
        if len(ls_single_files) == 0:
            _write_empty_casava('single', casava_out_single, logger)
        else:
            _write2casava_dir_single(tmpdirname, casava_out_single,
                                     ls_single_files)

        # write downloaded paired-end seqs from tmp to casava dir
        if len(ls_paired_files) == 0:
            _write_empty_casava('paired', casava_out_paired, logger)
        else:
            _write2casava_dir_paired(tmpdirname, casava_out_paired,
                                     ls_paired_files)

    return \
        casava_out_single, casava_out_paired, pd.Series(failed_ids, name='ID')


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
