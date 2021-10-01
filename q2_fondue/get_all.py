# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

def get_all(ctx,
            sample_ids,
            email,
            n_jobs=1,
            retries=2,
            threads=6):

    # get required methods
    get_metadata = ctx.get_action('fondue', 'get_metadata')
    get_sequences = ctx.get_action('fondue', 'get_sequences')

    # fetch metadata
    df_metadata, = get_metadata(sample_ids, email, n_jobs)

    # fetch sequences
    seq_single, seq_paired, = get_sequences(sample_ids, retries, threads)

    return (df_metadata, seq_single, seq_paired)
