# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import itertools

import pandas as pd
from qiime2.plugin import ValidationError
from qiime2.plugin import model
from qiime2.metadata.base import is_id_header, FORMATTED_ID_HEADERS
from q2_fondue.entrezpy_clients._utils import PREFIX


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


class NCBIAccessionIDsFormat(model.TextFileFormat):
    """
    This is a format used to store a list of SRA accession IDs (run,
    study, BioProject, sample and experiment IDs), which can be converted
    to QIIME's metadata. Artifacts containing of run, study and BioProject
    IDs can be input into any fondue action.
    """

    ALLOWED_PREFIXES = tuple(itertools.chain(*[
        v for k, v in PREFIX.items()
        if k in ('bioproject', 'run', 'study', 'sample', 'experiment')
    ]))

    def _validate_id(self, _id: str):
        if not _id.startswith(self.ALLOWED_PREFIXES):
            raise ValidationError(
                'Some of the provided IDs are invalid - only SRA run, study, '
                'BioProject, sample and experiment IDs are allowed. Please '
                'check your input and try again.'
            )

    def _validate_(self, level):
        df = pd.read_csv(str(self), sep='\t')
        cols = df.columns.tolist()

        if df.shape[1] > 2 or (df.shape[1] == 2 and not any(
                x in cols for x in ['doi', 'DOI'])):
            raise ValidationError(
                'NCBI Accession IDs artifact should only contain a single '
                'column with IDs of the SRA runs, studies or NCBI\'s '
                'BioProjects and an optional column `doi` with '
                'associated DOIs.'
            )

        # check that there is a valid ID header:
        if not any([is_id_header(x) for x in cols]):
            raise ValidationError(
                f'NCBI Accession IDs artifact must contain a valid '
                f'ID header from {FORMATTED_ID_HEADERS}.'
            )

        df.iloc[:, 0].apply(self._validate_id)


NCBIAccessionIDsDirFmt = model.SingleFileDirectoryFormat(
    'NCBIAccessionIDsDirFmt', 'ncbi-accession-ids.tsv', NCBIAccessionIDsFormat
)
