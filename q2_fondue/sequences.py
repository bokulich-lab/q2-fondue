# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import glob

import gzip
import os
from typing import List

import dotenv
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
    _has_enough_space, _find_next_id, _rewrite_fastq
)

threading.excepthook = handle_threaded_exception

LOGGER = set_up_logger('INFO', logger_name=__name__)


def _run_cmd_fasterq(
        acc: str, output_dir: str, threads: int, key_file: str):
    """Runs fasterq-dump command on a single accession."""
    if key_file != '':
        key_params = ['--ngc', key_file]
    else:
        key_params = []

    cmd_prefetch = ['prefetch', '-X', 'u', '-O', acc, *key_params, acc]
    cmd_fasterq = [
        'fasterq-dump', '-e', str(threads), '--size-check', 'on', '-x',
        *key_params, acc]

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
        accession_ids, tmpdirname, threads, key_file, retries
) -> (dict, List[str]):
    """Runs prefetch & fasterq-dump for all ids in accession_ids.

    Args:
        accession_ids (list): List of all run IDs to be fetched.
        tmpdirname (str): Name of temporary directory to store the data.
        threads (int): Number of threads to be used in parallel.
        key_file (str): Filepath to dbGaP repository key.
        retries (int): Number of retries to fetch sequences.

    Returns:
        failed_ids (dict): Failed run IDs with corresponding errors.
    """
    LOGGER.info(
        f'Downloading sequences for {len(accession_ids)} accession IDs...'
    )
    accession_ids_init = accession_ids.copy()
    init_retries = retries
    _, _, init_free_space = shutil.disk_usage(tmpdirname)

    successful_ids = []
    while (retries >= 0) and (len(accession_ids) > 0):
        failed_ids = {}
        pbar = tqdm(sorted(accession_ids))
        for acc in pbar:
            pbar.set_description(
                f'Downloading sequences for run {acc} '
                f'(attempt {-retries + init_retries + 1})'
            )
            result = _run_cmd_fasterq(
                acc, tmpdirname, threads, key_file)
            if result.returncode != 0:
                failed_ids[acc] = result.stderr
            else:
                successful_ids.append(acc)
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
    if failed_ids:
        errors = '\n'.join(
            [f'ID={x}, Error={y}' for x, y in list(failed_ids.items())[:5]]
        )
        msg += f' {len(failed_ids.keys())} out of {len(accession_ids_init)} ' \
               f'runs failed to fetch. Below are the error messages of the ' \
               f"first 5 failed runs:\n{errors}"
    LOGGER.info(msg)
    return failed_ids, successful_ids


def _process_one_sequence(filename, output_dir):
    """Renames sequence files to follow the required naming convention."""
    # Renaming
    new_name, is_paired = None, False
    if filename.endswith('_1.fastq'):
        # paired-end _1: barcode 00
        acc = re.search(r'(.*)_1\.fastq$', filename).group(1)
        new_name, is_paired = '%s_00_L001_R1_001.fastq' % acc, True
    elif filename.endswith('_2.fastq'):
        # paired-end _2
        acc = re.search(r'(.*)_2\.fastq$', filename).group(1)
        new_name, is_paired = '%s_00_L001_R2_001.fastq' % acc, True
    elif filename.endswith('.fastq'):
        # single-reads: barcode 01
        acc = re.search(r'(.*)\.fastq$', filename).group(1)
        new_name, is_paired = '%s_01_L001_R1_001.fastq' % acc, False
    else:
        return new_name, is_paired
    os.rename(os.path.join(output_dir, filename),
              os.path.join(output_dir, new_name))
    return new_name, is_paired


def _process_downloaded_sequences(ids, output_dir):
    """Processes downloaded sequences.

    Picks up filenames of fetched sequences from the fetched_queue, renames
    them and inserts processed filenames into the renaming_queue when finished.
    """
    single_files, paired_files = [], []
    for _id in ids:
        filenames = glob.glob(os.path.join(output_dir, f'{_id}*.fastq'))
        filenames = [
            _process_one_sequence(f, output_dir) for f in filenames
        ]
        _single_files = [x for x in filenames if not x[1]]
        if _single_files:
            single_files.append(_single_files)

        _paired_files = [x for x in filenames if x[1]]
        if _paired_files:
            paired_files.append(_paired_files)

    return single_files, paired_files


def _write_empty_casava(read_type, casava_out_path):
    """Writes empty casava file to output directory.

    Warns about `read_type` sequences that are not available
    and saves empty casava file.
    """
    LOGGER.warning(
        'No %s-end sequences available for these accession IDs.', read_type
    )

    if read_type == 'single':
        ls_file_names = ['xxx_01_L001_R1_001.fastq.gz']
    else:
        ls_file_names = ['xxx_00_L001_R1_001.fastq.gz',
                         'xxx_00_L001_R2_001.fastq.gz']
    # create empty CasavaDirFmt due to Q2 not supporting optional
    # output types
    for new_empty_name in ls_file_names:
        path_out = str(casava_out_path.path / new_empty_name)
        with gzip.open(str(path_out), mode='w'):
            pass


def _copy_to_casava(
        filenames: list, tmp_dir: str, casava_result_path: str
):
    """Copies single/paired-end sequences to Casava directory.

    Downloaded sequence files (single- or paired-end) will be
    copied from tmp_dir to casava_result_path.
    """
    fwd_path_in = os.path.join(tmp_dir, filenames[0])
    fwd_path_out = os.path.join(casava_result_path, f'{filenames[0]}.gz')
    _rewrite_fastq(fwd_path_in, fwd_path_out)

    if len(filenames) > 1:
        rev_path_in = os.path.join(tmp_dir, filenames[1])
        rev_path_out = os.path.join(casava_result_path, f'{filenames[1]}.gz')
        _rewrite_fastq(rev_path_in, rev_path_out)


def _write2casava_dir(files, tmp_dir, casava_out, is_paired):
    """Writes single- or paired-end files to casava directory.

    Picks up jobs (filenames) from the renaming_queue and decides whether they
    should be processed as single- or paired-end files.
    For example, [('fileA', False)] would be processed as single-end,
    while [('fileB_1', True), ('fileB_2', True)] as paired-end.
    When done, it inserts filenames into the done_queue to announce completion.
    """
    for filenames in files:
        if is_paired:
            filenames = [
                os.path.split(x[0])[-1] for x in sorted(filenames)
            ]
            _copy_to_casava(filenames, tmp_dir, casava_out)
        else:
            filename = os.path.split(filenames[0][0])[-1]
            _copy_to_casava([filename], tmp_dir, casava_out)


def _is_empty(artifact):
    samples = artifact.view(
        CasavaOneEightSingleLanePerSampleDirFmt
    ).manifest.index
    return all(sample == 'xxx' for sample in samples)


def _remove_empty(*artifact_lists):
    processed_artifacts = []
    for artifacts in artifact_lists:
        processed_artifacts.append(
            [artifact for artifact in artifacts if not _is_empty(artifact)]
        )
    return tuple(processed_artifacts)


def process_lists(process_func, *lists):
    """
    Process multiple lists using a specified function.

    Parameters:
    - process_func: A function that will be applied to each element of the lists.
    - *lists: Any number of lists to be processed.

    Returns:
    A tuple of lists with processed elements.
    """
    processed_lists = []

    for current_list in lists:
        processed_list = [process_func(element) for element in current_list]
        processed_lists.append(processed_list)

    return tuple(processed_lists)

def _make_empty_artifact(ctx, paired):
    if paired:
        filenames = [
            'xxx_00_L001_R1_001.fastq.gz', 'xxx_00_L001_R2_001.fastq.gz'
        ]
        _type = 'SampleData[PairedEndSequencesWithQuality]'
    else:
        filenames = ['xxx_01_L001_R1_001.fastq.gz']
        _type = 'SampleData[SequencesWithQuality]'

    casava_out = CasavaOneEightSingleLanePerSampleDirFmt()
    for filename in filenames:
        with gzip.open(str(casava_out.path / filename), mode='w'):
            pass

    return ctx.make_artifact(_type, casava_out)


def _get_sequences(
        accession_ids: Metadata, retries: int = 2,
        n_jobs: int = 1, log_level: str = 'INFO',
        restricted_access: bool = False
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
        restricted_access (bool, default=False): If sequence fetch requires
        dbGaP repository key.
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

    with tempfile.TemporaryDirectory() as tmp_dir:
        # get dbGAP key for restricted access sequences
        if restricted_access:
            dotenv.load_dotenv()
            key_file = os.getenv('KEY_FILEPATH')
            if not os.path.isfile(key_file):
                raise ValueError(
                    'The provided dbGAP repository key filepath does not '
                    'exist. Please check your environment variable '
                    'KEY_FILEPATH.'
                )
        else:
            key_file = ''
        # run fasterq-dump for all accessions
        failed_ids, successful_ids = _run_fasterq_dump_for_all(
            accession_ids=sorted(accession_ids),
            tmpdirname=tmp_dir,
            threads=n_jobs,
            key_file=key_file,
            retries=retries
        )

        # process downloaded files
        single_files, paired_files = _process_downloaded_sequences(
            ids=successful_ids, output_dir=tmp_dir,
        )

        # write to Casava directory
        if len(single_files) > 0:
            _write2casava_dir(
                files=single_files,
                tmp_dir=tmp_dir,
                casava_out=str(casava_out_single.path),
                is_paired=False
            )
        if len(paired_files) > 0:
            _write2casava_dir(
                files=paired_files,
                tmp_dir=tmp_dir,
                casava_out=str(casava_out_paired.path),
                is_paired=True
            )

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


def get_sequences(
        ctx, accession_ids, email, retries=2,
        n_jobs=1, log_level='INFO', restricted_access=False,
):
    _get_seqs = ctx.get_action('fondue', '_get_sequences')
    _combine = ctx.get_action('fondue', 'combine_seqs')

    _accession_ids = list(accession_ids.view(Metadata).get_ids())
    id_type = _determine_id_type(_accession_ids)
    if id_type != 'run':
        _accession_ids = _get_run_ids(
            email, n_jobs, _accession_ids, None, id_type, log_level
        )

    single, paired, failed = [], [], []
    for accession_id in _accession_ids:
        ids = ctx.make_artifact(
            'NCBIAccessionIDs', pd.Series([accession_id], name='id')
        )
        _single, _paired, _failed = _get_seqs(
            ids, retries, n_jobs, log_level, restricted_access
        )

        single.append(_single)
        paired.append(_paired)
        failed.append(_failed)

    single, paired = _remove_empty(single, paired)

    if single:
        single, = _combine(single)
    else:
        single = _make_empty_artifact(ctx, False)

    if paired:
        paired, = _combine(paired)
    else:
        paired = _make_empty_artifact(ctx, True)

    failed = pd.concat([x.view(pd.DataFrame) for x in failed], axis=0)
    failed = ctx.make_artifact('SRAFailedIDs', failed)

    return single, paired, failed


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
