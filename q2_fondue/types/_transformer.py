# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import pandas as pd
import qiime2

from ..plugin_setup import plugin
from ._format import SRAMetadataFormat, SRAFailedIDsFormat


@plugin.register_transformer
def _1(data: pd.DataFrame) -> (SRAMetadataFormat):
    ff = SRAMetadataFormat()
    with ff.open() as fh:
        data.to_csv(fh, sep='\t', header=True)
    return ff


@plugin.register_transformer
def _2(ff: SRAMetadataFormat) -> (pd.DataFrame):
    with ff.open() as fh:
        df = pd.read_csv(fh, sep='\t', header=0, index_col=0, dtype='str')
        return df


@plugin.register_transformer
def _3(ff: SRAMetadataFormat) -> (qiime2.Metadata):
    with ff.open() as fh:
        df = pd.read_csv(fh, sep='\t', header=0, index_col=0, dtype='str')
        return qiime2.Metadata(df)


@plugin.register_transformer
def _4(data: pd.Series) -> (SRAFailedIDsFormat):
    ff = SRAFailedIDsFormat()
    with ff.open() as fh:
        data.to_csv(fh, sep='\t', header=True, index=False)
    return ff


@plugin.register_transformer
def _5(ff: SRAFailedIDsFormat) -> (pd.Series):
    with ff.open() as fh:
        s = pd.read_csv(
            fh, header=0, dtype='str', squeeze=True
        )
        return s


@plugin.register_transformer
def _6(ff: SRAFailedIDsFormat) -> (qiime2.Metadata):
    with ff.open() as fh:
        df = pd.read_csv(fh, header=0, index_col=0, dtype='str')
        return qiime2.Metadata(df)
