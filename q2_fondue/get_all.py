# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import threading

from q2_fondue.utils import handle_threaded_exception
from qiime2 import Metadata


threading.excepthook = handle_threaded_exception


def get_all(ctx, accession_ids, email, n_jobs=1, retries=2, log_level='INFO'):

    # get required methods
    get_metadata = ctx.get_action('fondue', 'get_metadata')
    get_sequences = ctx.get_action('fondue', 'get_sequences')

    # fetch metadata
    df_metadata, = get_metadata(accession_ids, email, n_jobs, log_level)

    # fetch sequences - use metadata df to get run ids, regardless if
    # runs or projects were requested
    run_ids = df_metadata.view(Metadata)
    seq_single, seq_paired, failed_ids, = get_sequences(
        run_ids, email, retries, n_jobs, log_level
    )

    return df_metadata, seq_single, seq_paired, failed_ids
