# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from entrezpy import conduit as ec

from q2_fondue.entrezpy_clients._efetch import EFetchAnalyzer
from q2_fondue.entrezpy_clients._elink import ELinkAnalyzer
from q2_fondue.entrezpy_clients._esearch import ESearchAnalyzer
from q2_fondue.entrezpy_clients._utils import set_up_entrezpy_logging


def _get_run_ids_from_projects(email, n_jobs, project_ids, log_level) -> list:
    econduit = ec.Conduit(email=email, threads=n_jobs)
    set_up_entrezpy_logging(econduit, log_level)

    samp_ids_pipeline = econduit.new_pipeline()

    # search for project IDs
    es = samp_ids_pipeline.add_search(
        {'db': 'bioproject', 'term': " OR ".join(project_ids)},
        analyzer=ESearchAnalyzer(project_ids)
    )
    # given bioproject, find linked SRA runs
    el = samp_ids_pipeline.add_link(
        {'db': 'sra'},
        analyzer=ELinkAnalyzer(), dependency=es
    )
    # given SRA run IDs, fetch all metadata
    samp_ids_pipeline.add_fetch(
        {'rettype': 'docsum', 'retmode': 'xml', 'retmax': 10000},
        analyzer=EFetchAnalyzer(log_level), dependency=el
    )

    a = econduit.run(samp_ids_pipeline)
    return a.result.metadata_to_series().tolist()
