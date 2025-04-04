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

from q2_fondue.entrezpy_clients._pipelines import _get_run_ids
from q2_fondue.entrezpy_clients._utils import set_up_logger
from q2_fondue.utils import (
    _determine_id_type, handle_threaded_exception, DownloadError,
    _has_enough_space, _rewrite_fastq
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
                'Not enough space for fasterq-dump', extra={'accession_id': acc}
            )
    return result


def _run_fasterq_dump(
        accession_id: str, tmp_dir: str, threads: int, key_file: str, retries: int
):
    """Runs prefetch & fasterq-dump for the given accession_id.

    Args:
        accession_id (str): Run ID to be fetched.
        tmp_dir (str): Name of temporary directory to store the data.
        threads (int): Number of threads to be used in parallel.
        key_file (str): Filepath to dbGaP repository key.
        retries (int): Number of retries to fetch sequences.

    Returns:
        success (bool): True if all sequences were fetched successfully.
        error_msg (str): Error message returned by fasterq-dump or prefetch.
    """
    LOGGER.info('Downloading sequences', extra={'accession_id': accession_id})
    _, _, init_free_space = shutil.disk_usage(tmp_dir)

    error_msg, success = None, False
    while retries >= 0:
        # check space availability
        _, _, free_space = shutil.disk_usage(tmp_dir)
        used_seq_space = init_free_space - free_space
        # current space threshold: 35% of fetched seq space as evaluated
        # from 6 random run and ProjectIDs
        if free_space < (0.35 * used_seq_space) and not _has_enough_space(accession_id, tmp_dir):
            LOGGER.warning(
                'Available storage was exhausted - there will be no more retries.',
                extra={'accession_id': accession_id}
            )
            error_msg = 'Not enough space for fasterq-dump'
            break

        result = _run_cmd_fasterq(accession_id, tmp_dir, threads, key_file)
        if result.returncode != 0:
            error_msg = result.stderr
            LOGGER.error(
                f"Fetching failed. Error: {error_msg}", extra={'accession_id': accession_id}
            )

            # log & add time buffer
            sleep_lag = (1 / (retries + 1)) * 180
            LOGGER.info(
                f'Retrying to download in {round(sleep_lag / 60, 1)} min.',
                extra={'accession_id': accession_id}
            )
            time.sleep(sleep_lag)
            retries -= 1
        else:
            success = True
            error_msg = None
            break

    if success:
        LOGGER.info('Successfully downloaded sequences', extra={'accession_id': accession_id})
    else:
        LOGGER.error('Failed to download sequences', extra={'accession_id': accession_id})

    return success, error_msg


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


def _process_downloaded_sequences(
        accession_id: str, output_dir: str
) -> (list, list):
    """Processes downloaded sequences.

    Renames the files downloaded for the given accession ID.
    """
    LOGGER.info('Processing downloaded sequences', extra={'accession_id': accession_id})
    filenames = glob.glob(os.path.join(output_dir, f'{accession_id}*.fastq'))
    filenames = [
        _process_one_sequence(f, output_dir) for f in filenames
    ]
    single = [x for x in filenames if not x[1]]
    paired = sorted([x for x in filenames if x[1]])

    return single, paired


def _write_empty_casava(read_type: str, casava_out: str, accession_id: str):
    """Writes empty casava file to output directory.

    Warns about `read_type` sequences that are not available
    and saves empty casava file.
    """
    LOGGER.warning(
        f'No {read_type}-end sequences available',
        extra={'accession_id': accession_id}
    )

    if read_type == 'single':
        ls_file_names = ['xxx_01_L001_R1_001.fastq.gz']
    else:
        ls_file_names = ['xxx_00_L001_R1_001.fastq.gz',
                         'xxx_00_L001_R2_001.fastq.gz']
    # create empty CasavaDirFmt due to Q2 not supporting optional
    # output types
    for new_empty_name in ls_file_names:
        path_out = os.path.join(casava_out, new_empty_name)
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


def _write_to_casava(
        filenames: list, tmp_dir: str, casava_out: str, accession_id: str
):
    """Writes single- or paired-end files to casava directory.

    Picks up jobs (filenames) from the renaming_queue and decides whether they
    should be processed as single- or paired-end files.
    For example, [('fileA', False)] would be processed as single-end,
    while [('fileB_1', True), ('fileB_2', True)] as paired-end.
    When done, it inserts filenames into the done_queue to announce completion.
    """
    if len(filenames) == 1:
        LOGGER.info('Writing single-end sequences to Casava directory', extra={'accession_id': accession_id})
        filename = os.path.split(filenames[0][0])[-1]
        _copy_to_casava([filename], tmp_dir, casava_out)
    elif len(filenames) == 2:
        LOGGER.info('Writing paired-end sequences to Casava directory', extra={'accession_id': accession_id})
        filenames = [
            os.path.split(x[0])[-1] for x in sorted(filenames)
        ]
        _copy_to_casava(filenames, tmp_dir, casava_out)
    else:
        LOGGER.error(
            'More than two files were found for the same ID while writing outputs to Casava directory',
            extra={'accession_id': accession_id}
        )


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
        accession_id: str, retries: int = 2, n_download_jobs: int = 1,
        log_level: str = 'INFO', restricted_access: bool = False
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
        accession_id (str): Run ID to be fetched.
        email (str): A valid e-mail address (required by NCBI).
        retries (int, default=2): Number of retries to fetch sequences.
        restricted_access (bool, default=False): If sequence fetch requires
        dbGaP repository key.
        n_download_jobs (int, default=1): Number of download jobs to be used.
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

        success, error_msg = _run_fasterq_dump(
            accession_id, tmp_dir, n_download_jobs, key_file, retries
        )

        if success:
            single, paired = _process_downloaded_sequences(accession_id, tmp_dir)

            # make sure either of the sequences were downloaded
            if len(single) == 0 and len(paired) == 0:
                raise DownloadError(
                    'Neither single- nor paired-end sequences could '
                    'be downloaded. Please check your accession IDs.'
                )

            # write downloaded single-read seqs from tmp to casava dir
            if len(single) == 0:
                _write_empty_casava(
                    'single', str(casava_out_single), accession_id
                )
            else:
                _write_to_casava(
                    single, tmp_dir, str(casava_out_single), accession_id
                )

            # write downloaded paired-end seqs from tmp to casava dir
            if len(paired) == 0:
                _write_empty_casava(
                    'paired', str(casava_out_paired), accession_id
                )
            else:
                _write_to_casava(
                    paired, tmp_dir, str(casava_out_paired), accession_id
                )

            failed_ids = pd.DataFrame(
                data={'Error message': []}, index=pd.Index([], name='ID')
            )
        else:
            LOGGER.error(
                f'Failed to download sequences. Error: {error_msg}',
                extra={'accession_id': accession_id}
            )
            _write_empty_casava(
                'single', str(casava_out_single), accession_id
            )
            _write_empty_casava(
                'paired', str(casava_out_paired), accession_id
            )
            failed_ids = pd.DataFrame(
                data={'Error message': [error_msg]},
                index=pd.Index([accession_id], name='ID')
            )

    LOGGER.info('Processing finished', extra={'accession_id': accession_id})
    return casava_out_single, casava_out_paired, failed_ids


def get_sequences(
        ctx, accession_ids, email, retries=2,
        n_download_jobs=1, n_jobs=1, log_level='INFO',
        restricted_access=False
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
    for _id in _accession_ids:
        _single, _paired, _failed = _get_seqs(
            _id, retries, n_download_jobs, log_level, restricted_access
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
