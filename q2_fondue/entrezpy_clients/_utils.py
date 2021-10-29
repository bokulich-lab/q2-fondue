# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import pandas as pd


PREFIX = {
    'run': ('SRR', 'ERR', 'DRR'),
    'experiment': ('SRX', 'ERX', 'DRX'),
    'sample': ('SRS', 'ERS', 'DRS'),
    'study': ('SRP', 'ERP', 'DRP'),
    'bioproject': ('PRJN', 'PRJE', 'PRJD')
}


def get_attrs(obj, excluded=()):
    return [k for k, v in vars(obj).items()
            if k not in excluded and not k.startswith('__')]


def rename_columns(df: pd.DataFrame):
    # clean up ID columns
    col_map = {}
    id_cols = [col for col in df.columns if col.endswith('_id')]
    for col in id_cols:
        col_split = col.split('_')
        col_map[col] = f'{col_split[0].capitalize()} {col_split[1].upper()}'

    # clean up other multi-word columns
    wordy_cols = [col for col in df.columns
                  if '_' in col and col not in id_cols]
    for col in wordy_cols:
        col_map[col] = ' '.join([x.capitalize() for x in col.split('_')])

    # capitalize the rest
    remainder_cols = [col for col in df.columns
                      if col not in id_cols and col not in wordy_cols]
    for col in remainder_cols:
        col_map[col] = col.capitalize()

    return df.rename(columns=col_map, inplace=False)
