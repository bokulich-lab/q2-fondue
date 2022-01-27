# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import qiime2 as q2
import pandas as pd
import threading

from q2_fondue.utils import handle_threaded_exception


threading.excepthook = handle_threaded_exception


def get_all(ctx, accession_ids, email, n_jobs=1, retries=2, log_level='INFO'):

    # get required methods
    get_metadata = ctx.get_action('fondue', 'get_metadata')
    get_sequences = ctx.get_action('fondue', 'get_sequences')

    # fetch metadata
    metadata, = get_metadata(accession_ids, email, n_jobs, log_level)

    # fetch sequences - use metadata to get run ids, regardless if
    # runs or projects were requested
    run_ids = q2.Artifact.import_data(
        'NCBIAccessionIDs', pd.Series(metadata.view(pd.DataFrame).index)
    )
    seq_single, seq_paired, failed_ids, = get_sequences(
        run_ids, email, retries, n_jobs, log_level
    )

    return metadata, seq_single, seq_paired, failed_ids
