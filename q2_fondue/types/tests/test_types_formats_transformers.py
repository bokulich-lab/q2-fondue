# ----------------------------------------------------------------------------
# Copyright (c) 2025, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest

import pandas as pd
import qiime2
from qiime2.plugin import ValidationError
from qiime2.plugin.testing import TestPluginBase

from q2_fondue.types import (
    SRAMetadata,
    SRAMetadataDirFmt,
    SRAMetadataFormat,
    SRAFailedIDs,
    SRAFailedIDsDirFmt,
    SRAFailedIDsFormat,
    NCBIAccessionIDsFormat,
    NCBIAccessionIDs,
    NCBIAccessionIDsDirFmt,
)


class TestFormats(TestPluginBase):
    package = "q2_fondue.types.tests"

    def test_sra_metadata_fmt(self):
        meta_path = self.get_data_path("sra-metadata.tsv")
        format = SRAMetadataFormat(meta_path, mode="r")
        format.validate()

    def test_sra_metadata_fmt_missing_columns(self):
        meta_path = self.get_data_path("sra-metadata-missing-columns.tsv")
        format = SRAMetadataFormat(meta_path, mode="r")
        with self.assertRaisesRegex(
            ValidationError,
            "Some required columns are missing from the metadata file: "
            "Organism, Instrument, Platform, Bases, Bytes, Public, "
            "Library Selection, Library Source, Library Layout, Study ID.",
        ):
            format.validate()

    def test_sra_metadata_fmt_missing_ids(self):
        meta_path = self.get_data_path("sra-metadata-missing-ids.tsv")
        format = SRAMetadataFormat(meta_path, mode="r")
        with self.assertRaisesRegex(
            ValidationError,
            "Some samples are missing IDs in the following fields: "
            "Biosample ID, Study ID",
        ):
            format.validate()

    def test_sra_failed_ids_fmt(self):
        meta_path = self.get_data_path("sra-failed-ids.tsv")
        format = SRAFailedIDsFormat(meta_path, mode="r")
        format.validate()

    def test_sra_failed_ids_fmt_empty(self):
        meta_path = self.get_data_path("sra-failed-ids-empty.tsv")
        format = SRAFailedIDsFormat(meta_path, mode="r")
        format.validate()

    def test_sra_failed_ids_fmt_many_columns(self):
        meta_path = self.get_data_path("sra-metadata.tsv")
        format = SRAFailedIDsFormat(meta_path, mode="r")
        with self.assertRaisesRegex(
            ValidationError, "Failed IDs artifact should only contain a single column"
        ):
            format.validate()

    def test_ncbi_accession_ids_fmt_runs(self):
        meta_path = self.get_data_path("ncbi-ids-runs.tsv")
        format = NCBIAccessionIDsFormat(meta_path, mode="r")
        format.validate()

    def test_ncbi_accession_ids_fmt_projects(self):
        meta_path = self.get_data_path("ncbi-ids-bioprojects.tsv")
        format = NCBIAccessionIDsFormat(meta_path, mode="r")
        format.validate()

    def test_ncbi_accession_ids_fmt_studies(self):
        meta_path = self.get_data_path("ncbi-ids-studies.tsv")
        format = NCBIAccessionIDsFormat(meta_path, mode="r")
        format.validate()

    def test_ncbi_accession_ids_fmt_other(self):
        meta_path = self.get_data_path("ncbi-ids-other.tsv")
        format = NCBIAccessionIDsFormat(meta_path, mode="r")
        format.validate()

    def test_ncbi_accession_ids_fmt_empty(self):
        meta_path = self.get_data_path("sra-failed-ids-empty.tsv")
        format = NCBIAccessionIDsFormat(meta_path, mode="r")
        format.validate()

    def test_ncbi_accession_ids_fmt_many_columns(self):
        meta_path = self.get_data_path("sra-metadata.tsv")
        format = NCBIAccessionIDsFormat(meta_path, mode="r")
        with self.assertRaisesRegex(
            ValidationError, "NCBI Accession IDs artifact should only contain"
        ):
            format.validate()

    def test_ncbi_accession_ids_fmt_wrong_ids(self):
        meta_path = self.get_data_path("ncbi-ids-wrong.tsv")
        format = NCBIAccessionIDsFormat(meta_path, mode="r")
        with self.assertRaisesRegex(
            ValidationError, "Some of the provided IDs are invalid"
        ):
            format.validate()

    def test_ncbi_accession_ids_fmt_with_doi(self):
        meta_path = self.get_data_path("ncbi-ids-runs-doi.tsv")
        format = NCBIAccessionIDsFormat(meta_path, mode="r")
        format.validate()

    def test_ncbi_accession_ids_fmt_other_than_doi(self):
        meta_path = self.get_data_path("ncbi-ids-runs-no-doi.tsv")
        format = NCBIAccessionIDsFormat(meta_path, mode="r")
        with self.assertRaisesRegex(ValidationError, "should only contain a single"):
            format.validate()

    def test_ncbi_accession_ids_fmt_wrong_id_header(self):
        meta_path = self.get_data_path("ncbi-ids-runs-wrong-id-header.tsv")
        format = NCBIAccessionIDsFormat(meta_path, mode="r")
        with self.assertRaisesRegex(
            ValidationError, "IDs artifact must contain a valid"
        ):
            format.validate()


class TestTypes(TestPluginBase):
    package = "q2_fondue.types.tests"

    def test_sra_metadata_semantic_type_registration(self):
        self.assertRegisteredSemanticType(SRAMetadata)

    def test_sra_failed_ids_semantic_type_registration(self):
        self.assertRegisteredSemanticType(SRAFailedIDs)

    def test_ncbi_accession_ids_semantic_type_registration(self):
        self.assertRegisteredSemanticType(NCBIAccessionIDs)

    def test_sra_metadata_to_format_registration(self):
        self.assertSemanticTypeRegisteredToFormat(SRAMetadata, SRAMetadataDirFmt)

    def test_sra_failed_ids_to_format_registration(self):
        self.assertSemanticTypeRegisteredToFormat(SRAFailedIDs, SRAFailedIDsDirFmt)

    def test_ncbi_accession_ids_to_format_registration(self):
        self.assertSemanticTypeRegisteredToFormat(
            NCBIAccessionIDs, NCBIAccessionIDsDirFmt
        )


class TestTransformers(TestPluginBase):
    package = "q2_fondue.types.tests"

    def setUp(self):
        super().setUp()
        meta_path = self.get_data_path("sra-metadata.tsv")
        self.sra_meta = SRAMetadataFormat(meta_path, mode="r")
        self.sra_meta_df = pd.read_csv(
            meta_path, sep="\t", header=0, index_col=0, dtype="str"
        )
        failed_ids_path = self.get_data_path("sra-failed-ids.tsv")
        self.sra_failed = SRAFailedIDsFormat(failed_ids_path, mode="r")
        self.sra_failed_df = pd.read_csv(
            failed_ids_path, sep="\t", header=0, index_col=0, dtype="str"
        )
        ncbi_ids_path = self.get_data_path("ncbi-ids-runs.tsv")
        self.ncbi_ids = NCBIAccessionIDsFormat(ncbi_ids_path, mode="r")
        self.ncbi_ids_ser = pd.read_csv(ncbi_ids_path, header=0, dtype="str").squeeze()
        self.ncbi_ids_df = pd.read_csv(
            ncbi_ids_path, sep="\t", header=0, index_col=0, dtype="str"
        )

    def test_dataframe_to_sra_metadata(self):
        transformer = self.get_transformer(pd.DataFrame, SRAMetadataFormat)
        obs = transformer(self.sra_meta_df)
        self.assertIsInstance(obs, SRAMetadataFormat)

        obs = pd.read_csv(str(obs), sep="\t", header=0, index_col=0, dtype="str")
        pd.testing.assert_frame_equal(obs, self.sra_meta_df)

    def test_sra_metadata_to_dataframe(self):
        _, obs = self.transform_format(
            SRAMetadataFormat, pd.DataFrame, "sra-metadata.tsv"
        )
        self.assertIsInstance(obs, pd.DataFrame)
        pd.testing.assert_frame_equal(obs, self.sra_meta_df)

    def test_sra_metadata_to_q2_metadata(self):
        _, obs = self.transform_format(
            SRAMetadataFormat, qiime2.Metadata, "sra-metadata.tsv"
        )
        exp = qiime2.Metadata(self.sra_meta_df)
        self.assertEqual(obs, exp)

    def test_dataframe_to_sra_failed_ids(self):
        transformer = self.get_transformer(pd.DataFrame, SRAFailedIDsFormat)
        obs = transformer(self.sra_failed_df)
        self.assertIsInstance(obs, SRAFailedIDsFormat)

        obs = pd.read_csv(str(obs), sep="\t", header=0, index_col=0, dtype="str")
        pd.testing.assert_frame_equal(obs, self.sra_failed_df)

    def test_sra_failed_ids_to_dataframe(self):
        _, obs = self.transform_format(
            SRAFailedIDsFormat, pd.DataFrame, "sra-failed-ids.tsv"
        )
        self.assertIsInstance(obs, pd.DataFrame)
        pd.testing.assert_frame_equal(obs, self.sra_failed_df)

    def test_sra_failed_ids_to_q2_metadata(self):
        _, obs = self.transform_format(
            SRAFailedIDsFormat, qiime2.Metadata, "sra-failed-ids.tsv"
        )
        exp = qiime2.Metadata(self.sra_failed_df)
        self.assertEqual(obs, exp)

    def test_series_to_ncbi_accession_ids(self):
        transformer = self.get_transformer(pd.Series, NCBIAccessionIDsFormat)
        obs = transformer(self.ncbi_ids_ser)
        self.assertIsInstance(obs, NCBIAccessionIDsFormat)

        obs = pd.read_csv(str(obs), header=0, dtype="str").squeeze()
        pd.testing.assert_series_equal(obs, self.ncbi_ids_ser)

    def test_dataframe_to_ncbi_accession_ids(self):
        transformer = self.get_transformer(pd.DataFrame, NCBIAccessionIDsFormat)
        obs = transformer(self.ncbi_ids_df)
        self.assertIsInstance(obs, NCBIAccessionIDsFormat)

        obs = pd.read_csv(str(obs), sep="\t", header=0, index_col=0, dtype="str")
        pd.testing.assert_frame_equal(obs, self.ncbi_ids_df)

    def test_ncbi_accession_ids_to_dataframe(self):
        _, obs = self.transform_format(
            NCBIAccessionIDsFormat, pd.DataFrame, "ncbi-ids-runs.tsv"
        )
        self.assertIsInstance(obs, pd.DataFrame)
        pd.testing.assert_frame_equal(obs, self.ncbi_ids_df)

    def test_ncbi_accession_ids_to_q2_metadata(self):
        _, obs = self.transform_format(
            NCBIAccessionIDsFormat, qiime2.Metadata, "ncbi-ids-runs.tsv"
        )
        exp = qiime2.Metadata(self.ncbi_ids_df)
        self.assertEqual(obs, exp)

    def test_sra_metadata_to_ncbi_accession_ids(self):
        transformer = self.get_transformer(SRAMetadataFormat, NCBIAccessionIDsFormat)
        obs = transformer(self.sra_meta)
        self.assertIsInstance(obs, NCBIAccessionIDsFormat)

        obs = pd.read_csv(str(obs), sep="\t", header=0, index_col=0, dtype="str")
        pd.testing.assert_frame_equal(obs, self.ncbi_ids_df)


if __name__ == "__main__":
    unittest.main()
