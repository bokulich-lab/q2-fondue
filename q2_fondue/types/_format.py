# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import pandas as pd
from qiime2.plugin import ValidationError
from qiime2.plugin import model


class SRAMetadataFormat(model.TextFileFormat):

    REQUIRED_IDS = [
        'ID', 'Biosample ID', 'Bioproject ID', 'Experiment ID',
        'Study ID', 'Sample Accession'
    ]
    REQUIRED_HEADER_FIELDS = [
        'Organism', 'Instrument', 'Platform', 'Bases', 'Bytes', 'Public',
        'Library Selection', 'Library Source', 'Library Layout'
    ]
    REQUIRED_HEADER_FIELDS.extend(REQUIRED_IDS)

    def _validate(self):
        df = pd.read_csv(str(self), sep='\t')

        missing_cols = [
            x for x in self.REQUIRED_HEADER_FIELDS if x not in df.columns]
        if missing_cols:
            raise ValidationError(
                'Some required columns are missing from the metadata file: '
                f'{", ".join(missing_cols)}.'
            )

        # some IDs must be present in all samples
        nans = df.isnull().sum(axis=0)[self.REQUIRED_IDS]
        missing_ids = nans.where(nans > 0).dropna().index.tolist()
        if missing_ids:
            raise ValidationError(
                'Some samples are missing IDs in the following fields: '
                f'{", ".join(missing_ids)}.'
            )

    def _validate_(self, level):
        self._validate()


SRAMetadataDirFmt = model.SingleFileDirectoryFormat(
    'SRAMetadataDirFmt', 'sra-metadata.tsv', SRAMetadataFormat
)


class SRAFailedIDsFormat(model.TextFileFormat):
    """
    This is a "fake" format only used to store a list of failed SRA IDs,
    which can be converted to QIIME's metadata and input into any fondue
    action.
    """

    def _validate_(self, level):
        df = pd.read_csv(str(self), sep='\t', index_col=0)

        if df.shape[1] > 1:
            raise ValidationError(
                'Failed IDs artifact should only contain a single column '
                'with error message for the runs that could not be fetched '
                '(indexed by run ID).'
            )


SRAFailedIDsDirFmt = model.SingleFileDirectoryFormat(
    'SRAFailedIDsDirFmt', 'sra-failed-ids.tsv', SRAFailedIDsFormat
)
