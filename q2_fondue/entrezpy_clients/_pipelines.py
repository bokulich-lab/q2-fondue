# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
from typing import Union

from entrezpy import conduit as ec

from entrezpy.elink.elink_analyzer import ElinkAnalyzer

from q2_fondue.entrezpy_clients._efetch import EFetchAnalyzer
from q2_fondue.entrezpy_clients._esearch import ESearchAnalyzer
from q2_fondue.entrezpy_clients._utils import set_up_entrezpy_logging

import entrezpy.esearch.esearcher as searcher

from q2_fondue.utils import _chunker

BATCH_SIZE = 500


def _get_run_ids(
        email: str, n_jobs: int, ids: Union[list, None],
        query: Union[str, None], source: str, log_level: str
) -> list:
    """Pipeline to retrieve run IDs associated with BioSample query
    (provided in `query`) or other aggregate IDs like studies
    (`source`='study'), bioprojects (`source`='bioproject'), samples
    (`source`='sample') or experiments (`source`='experiment')
    provided in `ids`.

    Args:
        email (str): User email.
        n_jobs (int): Number of jobs.
        ids (list): List of study, bioproject, sample or experiment IDs.
        query (str): Search query to find IDs by.
        source (str): Type of IDs provided ('study', 'bioproject',
                      'sample' or 'experiment').
        log_level (str): The log level to set.

    Returns:
        list: Run IDs associated with provided ids.
    """
    term = " OR ".join(ids) if ids else query

    # create pipeline to fetch all run IDs
    elink = True
    if source == 'bioproject':
        db = 'bioproject'
    elif source == 'biosample':
        db = 'biosample'
    else:
        db = 'sra'
        elink = False

    # find UIDS based on a query;
    # instead of saving the result on the history server
    # we will store all the UIDs recovered based on the
    # search query and use those in the mini-pipeline below;
    # this way we are not limited by ELink only accepting up to
    # who knows how many IDs and erroring out if we provide too
    # many (which could be the case e.g.: when we ask for more
    # than 10000 BioProject IDs or the text query returns more
    # than 10000 IDs presumably)
    esearcher = searcher.Esearcher(
        'esearcher', email, apikey=None,
        apikey_var=None, threads=n_jobs, qid=None)
    esearch_response = esearcher.inquire(
        {
            'db': db, 'term': term,
            'usehistory': False, 'rettype': 'json'
        },
        analyzer=ESearchAnalyzer(ids))

    # use the UIDs to link to other DBs and fetch related records;
    # we won't be using multi-threading here as this shouldn't take
    # long (we're only fetching IDs) and we don't want those dead
    # threads afterwards
    econduit = ec.Conduit(email=email, threads=0)
    set_up_entrezpy_logging(econduit, log_level)
    run_ids_pipeline = econduit.new_pipeline()

    # create a pipeline to link and fetch the run IDs;
    # we process the IDs obtained from the previous step in batches
    # as ELink cannot handle more than a certain amount of IDs
    # at the same time (recommended by NCBI)
    for _ids in _chunker(esearch_response.result.uids, BATCH_SIZE):
        if elink:
            el = run_ids_pipeline.add_link(
                {
                    'db': 'sra', 'dbfrom': db,
                    'id': _ids, 'link': False
                },
                analyzer=ElinkAnalyzer(),
            )
        else:
            el = None

        # given SRA run IDs, fetch all metadata
        efetch_params = {
            'rettype': 'docsum', 'retmode': 'xml',
            'reqsize': BATCH_SIZE, 'retmax': len(_ids)
        }
        if not elink:
            # we need to specify these manually as in this scenario
            # EFetch is not linked to anything
            efetch_params.update({'id': _ids, 'db': db})

        run_ids_pipeline.add_fetch(
            efetch_params,
            analyzer=EFetchAnalyzer(log_level), dependency=el
        )

    econduit.run(run_ids_pipeline)

    # recover run IDs from all instances of EFetchAnalyzer
    all_run_ids = []
    for x in econduit.analyzers.values():
        if isinstance(x, EFetchAnalyzer):
            all_run_ids.extend(x.result.metadata)

    return sorted(all_run_ids)
