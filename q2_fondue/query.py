# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import threading
import pandas as pd

from q2_fondue.utils import handle_threaded_exception
from q2_fondue.entrezpy_clients._pipelines import _get_run_ids

threading.excepthook = handle_threaded_exception


def get_ids_from_query(
        query: str, email: str,
        threads: int = 1, log_level: str = 'INFO'
) -> pd.Series:
    """Retrieves SRA run IDs based on a search query performed
        on the BioSample database.

    Args:
        query (str): Search query to be executed on
            the BioSample database.
        email (str): A valid e-mail address (required by NCBI).
        threads (int, default=1): Number of threads to be used in parallel.
        log_level (str, default='INFO'): Logging level.

    Returns:
        ids (pd.Series): Retrieved SRA run IDs.
    """
    run_ids = _get_run_ids(
        email, threads, None, query, 'biosample', log_level
    )

    return pd.Series(run_ids, name='ID')
