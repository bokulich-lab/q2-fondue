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
from ._format import (
    SRAMetadataFormat, SRAFailedIDsFormat, NCBIAccessionIDsFormat
)


def _meta_fmt_to_metadata(ff):
    with ff.open() as fh:
        df = pd.read_csv(fh, sep='\t', header=0, index_col=0, dtype='str')
        return qiime2.Metadata(df)


def _meta_fmt_to_series(ff):
    with ff.open() as fh:
        s = pd.read_csv(fh, header=0, dtype='str').squeeze()
        return s


def _series_to_meta_fmt(data: pd.Series, meta_fmt):
    with meta_fmt.open() as fh:
        data.to_csv(fh, sep='\t', header=True, index=False)
    return meta_fmt


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
    return _meta_fmt_to_metadata(ff)


@plugin.register_transformer
def _4(data: pd.DataFrame) -> (SRAFailedIDsFormat):
    ff = SRAFailedIDsFormat()
    with ff.open() as fh:
        data.to_csv(fh, sep='\t', header=True, index=True)
    return ff


@plugin.register_transformer
def _5(ff: SRAFailedIDsFormat) -> (pd.DataFrame):
    with ff.open() as fh:
        df = pd.read_csv(
            fh, sep='\t', header=0, index_col=0, dtype='str'
        )
        return df


@plugin.register_transformer
def _6(ff: SRAFailedIDsFormat) -> (qiime2.Metadata):
    return _meta_fmt_to_metadata(ff)


@plugin.register_transformer
def _7(data: pd.DataFrame) -> (NCBIAccessionIDsFormat):
    ff = NCBIAccessionIDsFormat()
    with ff.open() as fh:
        data.to_csv(fh, sep='\t', header=True, index=True)
    return ff


@plugin.register_transformer
def _77(data: pd.Series) -> (NCBIAccessionIDsFormat):
    ff = NCBIAccessionIDsFormat()
    return _series_to_meta_fmt(data, ff)


@plugin.register_transformer
def _8(ff: NCBIAccessionIDsFormat) -> (pd.DataFrame):
    with ff.open() as fh:
        df = pd.read_csv(
            fh, sep='\t', header=0, index_col=0, dtype='str'
        )
        return df


@plugin.register_transformer
def _9(ff: NCBIAccessionIDsFormat) -> (qiime2.Metadata):
    return _meta_fmt_to_metadata(ff)


@plugin.register_transformer
def _10(ff: SRAMetadataFormat) -> (NCBIAccessionIDsFormat):
    fout = NCBIAccessionIDsFormat()
    with ff.open() as fh, fout.open() as fo:
        df = pd.read_csv(fh, sep='\t', header=0, index_col=0, dtype='str')
        df.index.to_frame().to_csv(fo, sep='\t', header=True, index=False)
    return fout
