# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from entrezpy import conduit as ec

from entrezpy.elink.elink_analyzer import ElinkAnalyzer

from q2_fondue.entrezpy_clients._efetch import EFetchAnalyzer
from q2_fondue.entrezpy_clients._esearch import ESearchAnalyzer
from q2_fondue.entrezpy_clients._utils import set_up_entrezpy_logging


def _get_run_ids(
        email: str, n_jobs: int, ids: list,
        source: str, log_level: str) -> list:
    """Pipeline to retrieve metadata of run IDs associated with
    studies (`source`='study'), bioprojects (`source`='bioproject'),
    samples (`source`='sample') or experiments (`source`='experiment')
    provided in `ids`.

    Args:
        email (str): User email.
        n_jobs (int): Number of jobs.
        ids (list): List of study, bioproject, sample or experiment IDs.
        source (str): Type of IDs provided ('study', 'bioproject',
                      'sample' or 'experiment').
        log_level (str): The log level to set.

    Returns:
        list: Run IDs associated with provided ids.
    """
    # create pipeline to fetch all run IDs
    if source == 'bioproject':
        db = 'bioproject'
        elink = True
    else:
        db = 'sra'
        elink = False

    econduit = ec.Conduit(email=email, threads=n_jobs)
    set_up_entrezpy_logging(econduit, log_level)
    samp_ids_pipeline = econduit.new_pipeline()

    # search for IDs
    es = samp_ids_pipeline.add_search(
        {'db': db, 'term': " OR ".join(ids)},
        analyzer=ESearchAnalyzer(ids)
    )
    if elink:
        # given bioproject, find linked SRA runs
        el = samp_ids_pipeline.add_link(
            {'db': 'sra'},
            analyzer=ElinkAnalyzer(), dependency=es
        )
    else:
        el = es

    # given SRA run IDs, fetch all metadata
    samp_ids_pipeline.add_fetch(
        {'rettype': 'docsum', 'retmode': 'xml'},
        analyzer=EFetchAnalyzer(log_level), dependency=el
    )

    a = econduit.run(samp_ids_pipeline)

    return sorted(a.result.metadata)
