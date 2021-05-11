# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from qiime2.plugin import model


# TODO: add proper validation here
class SRAMetadataFormat(model.TextFileFormat):
    def _validate(self, n_records=None):
        pass

    def _validate_(self, level):
        self._validate()


SRAMetadataDirFmt = model.SingleFileDirectoryFormat(
    'SRAMetadataDirFmt', 'sra-metadata.tsv', SRAMetadataFormat
)
