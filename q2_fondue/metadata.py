# ----------------------------------------------------------------------------
# Copyright (c) 2025, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import threading
from typing import List, Tuple

import entrezpy.efetch.efetcher as ef
import pandas as pd
from qiime2 import Metadata

from q2_fondue.entrezpy_clients._efetch import EFetchAnalyzer
from q2_fondue.utils import (
    _validate_run_ids,
    _determine_id_type,
    handle_threaded_exception,
)
from q2_fondue.entrezpy_clients._utils import (
    set_up_entrezpy_logging,
    set_up_logger,
    InvalidIDs,
)
from q2_fondue.entrezpy_clients._pipelines import _get_run_ids


threading.excepthook = handle_threaded_exception
BATCH_SIZE = 150


def _efetcher_inquire(
    efetcher: ef.Efetcher, run_ids: List[str], log_level: str
) -> Tuple[pd.DataFrame, dict]:
    """Makes an EFetch request using the provided IDs.

    Args:
        efetcher (ef.Efetcher): A valid instance of an Entrezpy Efetcher.
        run_ids (List[str]): List of all the run IDs to be fetched.
        log_level (str): Logging level.

    Returns:
        pd.DataFrame: DataFrame with metadata obtained for the provided IDs.
        dict: Dictionary of the run IDs that were not found with
            respective error messages.
    """
    metadata_response = efetcher.inquire(
        {
            "db": "sra",
            "id": run_ids,
            "rettype": "xml",
            "retmode": "xml",
            "retmax": len(run_ids),
            "reqsize": BATCH_SIZE,
        },
        analyzer=EFetchAnalyzer(log_level),
    )

    if metadata_response.result is None:
        return (pd.DataFrame(), {m_id: metadata_response.error_msg for m_id in run_ids})
    else:
        return metadata_response.result.metadata_to_df(), {}


def _execute_efetcher(email, n_jobs, run_ids, log_level):
    efetcher = ef.Efetcher(
        "efetcher", email, apikey=None, apikey_var=None, threads=n_jobs, qid=None
    )
    set_up_entrezpy_logging(efetcher, log_level)

    return _efetcher_inquire(efetcher, run_ids, log_level)


def _get_run_meta(
    email, n_jobs, run_ids, validated, log_level, logger
) -> (pd.DataFrame, dict):
    if not validated:
        invalid_ids = _validate_run_ids(email, n_jobs, run_ids, log_level)
        valid_ids = sorted(list(set(run_ids) - set(invalid_ids.keys())))

        if not valid_ids:
            raise InvalidIDs("All provided IDs were invalid. Please check your input.")
        if invalid_ids:
            logger.warning(
                f"The following provided IDs are invalid: "
                f'{",".join(invalid_ids.keys())}. Please correct them and '
                f"try fetching those independently."
            )
    else:
        # we assume that IDs retrieved by linking from aggregate IDs
        # (e.g., BioProject or study) should only return valid IDs,
        # since we asked NCBI to get those for us
        valid_ids = run_ids

    # fetch metadata
    logger.info("Fetching metadata for %i run IDs.", len(valid_ids))
    meta_df, missing_ids = _execute_efetcher(email, n_jobs, valid_ids, log_level)

    if missing_ids:
        logger.warning(
            "Metadata for the following run IDs could not be fetched: "
            f'{",".join(missing_ids.keys())}. '
            f"Please try fetching those independently."
        )

    return meta_df, missing_ids


def _get_other_meta(
    email, n_jobs, project_ids, id_type, log_level, logger
) -> (pd.DataFrame, dict):
    run_ids = _get_run_ids(email, n_jobs, project_ids, None, id_type, log_level)

    return _get_run_meta(email, n_jobs, run_ids, True, log_level, logger)


def _find_doi_mapping_and_type(mapping_doi_ids: Metadata) -> (pd.Series, str):
    """If present, save DOI name to ID mappings together with type
    of IDs the DOI names are matching to.

    Args:
        mapping_doi_ids (Metadata): Table of accession IDs with
            associated DOI names.
    Returns:
        pd.Series: Series of DOI names with matched accession IDs.
        str: Type of accession IDs in matching.
    """
    id2doi = mapping_doi_ids.to_dataframe().iloc[:, 0]
    doi_ids = sorted(list(mapping_doi_ids.get_ids()))
    id2doi_type = _determine_id_type(doi_ids)

    return (id2doi, id2doi_type)


def get_metadata(
    accession_ids: Metadata,
    email: str,
    threads: int = 1,
    log_level: str = "INFO",
    linked_doi: Metadata = None,
) -> (pd.DataFrame, pd.DataFrame):
    """Fetches metadata using the provided run/bioproject/study/sample or
    experiment accession IDs.

    If aggregate IDs (such as bioproject, study, sample, experiment IDs) were
    provided, first run IDs will be fetched using a Conduit Pipeline.
    The run IDs will be validated using an ESearch query. The metadata will
    be fetched only for the valid run IDs. Invalid run IDs will be raised
    with a warning. Run IDs for which the metadata could not be fetched will
    be returned with the corresponding error message as missing_ids.

    Args:
        accession_ids (Metadata): Table of all the accession IDs
            to be fetched (either run, bioproject, study, sample or
            experiment IDs). If table does not contain DOI names, names
            from `linked_doi` will be matched.
        linked_doi (Metadata): Optional table of accession IDs with
            associated DOI names. Preferably used when refetching failed
            run IDs that can be matched after metadata was fetched
            successfully. Ignored if `accession_ids` already contains DOI
            names.
        email (str): A valid e-mail address (required by NCBI).
        threads (int, default=1): Number of threads to be used in parallel.
        log_level (str, default='INFO'): Logging level.

    Returns:
        pd.DataFrame: DataFrame with metadata obtained for the provided IDs.
        pd.DataFrame: DataFrame with runs IDs for which no metadata was
            fetched and the associated error messages.
    """
    logger = set_up_logger(log_level, logger_name=__name__)

    # extract DOI names to IDs mapping for later
    if any(x in accession_ids.columns for x in ["doi", "DOI"]):
        id2doi, id2doi_type = _find_doi_mapping_and_type(accession_ids)
    elif linked_doi and any(x in linked_doi.columns for x in ["doi", "DOI"]):
        id2doi, id2doi_type = _find_doi_mapping_and_type(linked_doi)
    else:
        id2doi, id2doi_type = None, None

    # Retrieve input IDs
    accession_ids = sorted(list(accession_ids.get_ids()))

    # figure out which id type we're dealing with
    id_type = _determine_id_type(accession_ids)

    # get actual metadata
    if id_type == "run":
        meta, missing_ids = _get_run_meta(
            email, threads, accession_ids, False, log_level, logger
        )
    else:
        meta, missing_ids = _get_other_meta(
            email, threads, accession_ids, id_type, log_level, logger
        )

    # match DOI names to metadata if present
    match_study_meta = {
        "bioproject": "Bioproject ID",
        "study": "Study ID",
        "experiment": "Experiment ID",
        "sample": "Sample Accession",
    }
    if id2doi is not None and id2doi_type == "run":
        meta = meta.join(id2doi, how="left")
    elif id2doi is not None and id2doi_type != "run":
        meta = meta.merge(
            id2doi, how="left", left_on=match_study_meta[id2doi_type], right_index=True
        )

    missing_ids = pd.DataFrame(
        data={"Error message": missing_ids.values()},
        index=pd.Index(missing_ids.keys(), name="ID"),
    )
    return meta, missing_ids


def merge_metadata(metadata: pd.DataFrame) -> pd.DataFrame:
    """Merges provided multiple metadata into a single metadata object.

    Args:
        metadata (pd.DataFrame): List of metadata DataFrames to be merged.

    Returns:
        metadata_merged (pd.DataFrame): Final metadata DataFrame.
    """
    logger = set_up_logger("INFO", logger_name=__name__)
    logger.info("Merging %s metadata DataFrames.", len(metadata))

    metadata_merged = pd.concat(metadata, axis=0, join="outer")

    records_count = metadata_merged.shape[0]
    metadata_merged.drop_duplicates(inplace=True)
    if records_count != metadata_merged.shape[0]:
        logger.info(
            "%s duplicate record(s) found in the metadata " "were dropped.",
            records_count - metadata_merged.shape[0],
        )

    if len(metadata_merged.index) != len(set(metadata_merged.index)):
        logger.warning(
            "Records with same IDs but differing values were found in "
            "the metadata and will not be removed."
        )

    logger.info(
        "Merged metadata DataFrame has %s rows and %s columns.",
        metadata_merged.shape[0],
        metadata_merged.shape[1],
    )

    return metadata_merged
