# ----------------------------------------------------------------------------
# Copyright (c) 2025, Bokulich Laboratories.
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
from q2_types.per_sample_sequences import CasavaOneEightSingleLanePerSampleDirFmt
from qiime2 import Artifact

from q2_fondue.entrezpy_clients._esearch import ESearchAnalyzer
from q2_fondue.entrezpy_clients._utils import (
    PREFIX,
    InvalidIDs,
    set_up_logger,
    set_up_entrezpy_logging,
)

LOGGER = set_up_logger("INFO", logger_name=__name__)


class DownloadError(Exception):
    pass


def _chunker(seq, size):
    # source: https://stackoverflow.com/a/434328/579416
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def _validate_run_ids(
    email: str, n_jobs: int, run_ids: List[str], log_level: str
) -> dict:
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
            "esearcher", email, apikey=None, apikey_var=None, threads=0, qid=None
        )
        set_up_entrezpy_logging(esearcher, log_level)

        esearch_response = esearcher.inquire(
            {"db": "sra", "term": " OR ".join(batch), "usehistory": False},
            analyzer=ESearchAnalyzer(batch),
        )
        invalid_ids.update(esearch_response.result.validate_result())

    return invalid_ids


def _determine_id_type(ids: list):
    ids = [x[:3] for x in ids]
    for kind in PREFIX.keys():
        if all([x in PREFIX[kind] for x in ids]):
            return kind
    raise InvalidIDs(
        "The type of provided IDs is either not supported or "
        "IDs of mixed types were provided. Please provide IDs "
        "corresponding to either SRA run (#S|E|DRR), study "
        "(#S|E|DRP) or NCBI BioProject IDs (#PRJ)."
    )


def handle_threaded_exception(args):
    logger = set_up_logger("DEBUG", logger_name="ThreadedErrorsManager")
    msg = "Data fetching was interrupted by the following error: \n"

    if "gaierror is not JSON serializable" in str(args.exc_value):
        msg += (
            "EntrezPy failed to connect to NCBI. Please check your "
            "internet connection and try again. It may help to wait "
            "a few minutes before retrying."
        )
    # silence threads exiting correctly
    elif issubclass(args.exc_type, SystemExit) and str(args.exc_value) == "0":
        return
    else:
        msg += (
            f'Caught {args.exc_type} with value "{args.exc_value}" '
            f"in thread {args.thread}"
        )

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

    cmd_fasterq = ["fasterq-dump", "--size-check", "only", "-x", acc_id]
    result = subprocess.run(cmd_fasterq, text=True, capture_output=True, cwd=output_dir)

    if result.returncode == 0:
        return True
    elif result.returncode == 3 and "disk-limit exeeded" in result.stderr:
        LOGGER.warning("Not enough space to fetch run %s.", acc_id)
        return False
    else:
        LOGGER.error(
            'fasterq-dump exited with a "%s" error code (the message '
            'was: "%s"). We will try to fetch the next accession ID.',
            result.returncode,
            result.stderr,
        )
        return True


def _rewrite_fastq(file_in: str, file_out: str) -> None:
    """Rewrites a FASTQ file with gzip compression.

    Takes an uncompressed FASTQ file and writes it to a new location with
    gzip compression.

    Args:
        file_in (str): Path to input uncompressed FASTQ file
        file_out (str): Path where compressed FASTQ file should be written
    """
    with open(file_in, "rb") as f_in, gzip.open(file_out, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


def _is_empty(artifact: Artifact) -> bool:
    """Checks if a sequence artifact is empty.

    Determines if a sequence artifact is empty by checking if all sample IDs
    are "xxx", which indicates an empty placeholder artifact.

    Args:
        artifact: A QIIME 2 sequence artifact

    Returns:
        bool: True if the artifact is empty, False otherwise
    """
    samples = artifact.view(CasavaOneEightSingleLanePerSampleDirFmt).manifest.index
    return all(sample == "xxx" for sample in samples)


def _remove_empty(*artifact_lists) -> tuple:
    """Removes empty artifacts from lists of sequence artifacts.

    Takes one or more lists of sequence artifacts and filters out any empty
    artifacts (those containing only placeholder 'xxx' samples). Returns
    tuple of filtered lists maintaining the same order as input.

    Args:
        *artifact_lists: Variable number of lists containing sequence artifacts
            to filter

    Returns:
        tuple: Tuple of filtered lists with empty artifacts removed, in same
            order as input lists
    """
    processed_artifacts = []
    for artifacts in artifact_lists:
        processed_artifacts.append(
            [artifact for artifact in artifacts if not _is_empty(artifact)]
        )
    return tuple(processed_artifacts)


def _make_empty_artifact(ctx, paired: bool) -> Artifact:
    """Creates an empty sequence artifact.

    Creates an empty QIIME 2 sequence artifact containing placeholder files.
    For paired-end sequences, creates two empty fastq files (R1 and R2).
    For single-end sequences, creates one empty fastq file (R1).

    Args:
        ctx: QIIME 2 plugin context
        paired (bool): Whether to create paired-end (True) or
            single-end (False) artifact

    Returns:
        QIIME 2 artifact: Empty sequence artifact of appropriate type
            (paired or single-end)
    """
    if paired:
        filenames = ["xxx_00_L001_R1_001.fastq.gz", "xxx_00_L001_R2_001.fastq.gz"]
        _type = "SampleData[PairedEndSequencesWithQuality]"
    else:
        filenames = ["xxx_01_L001_R1_001.fastq.gz"]
        _type = "SampleData[SequencesWithQuality]"

    casava_out = CasavaOneEightSingleLanePerSampleDirFmt()
    for filename in filenames:
        with gzip.open(str(casava_out.path / filename), mode="w"):
            pass

    return ctx.make_artifact(_type, casava_out)
