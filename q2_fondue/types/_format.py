# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
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
        'ID', 'BioSample ID', 'BioProject ID', 'Experiment ID', 'Study ID'
    ]
    REQUIRED_HEADER_FIELDS = [
        'Organism', 'Instrument', 'Platform', 'Bases', 'Bytes', 'Consent',
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
