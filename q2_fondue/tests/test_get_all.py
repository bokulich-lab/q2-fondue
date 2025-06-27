# ----------------------------------------------------------------------------
# Copyright (c) 2025, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import unittest
from unittest.mock import ANY, Mock

import pandas as pd
from qiime2 import Artifact

from q2_fondue.get_all import get_all
from q2_fondue.tests.test_sequences import SequenceTests


class FakeCtx(Mock):
    def __init__(self, ids_path, meta_path, failed_ids=None):
        super().__init__()
        self.ids = Artifact.import_data("NCBIAccessionIDs", ids_path)
        self.meta = Artifact.import_data("SRAMetadata", meta_path)
        self.failed_empty = Artifact.import_data("SRAFailedIDs", pd.DataFrame())
        if failed_ids:
            self.failed = Artifact.import_data(
                "SRAFailedIDs",
                pd.DataFrame(
                    data={"Error message": ["Some error message" for _ in failed_ids]},
                    index=pd.Index(failed_ids, name="ID"),
                ),
            )
        else:
            self.failed = self.failed_empty

        self.get_metadata = Mock(return_value=(self.meta, self.failed_empty))
        self.get_sequences = Mock(return_value=(Mock(), Mock(), self.failed))

    def get_action(self, plugin, action):
        if action == "get_metadata":
            return self.get_metadata
        elif action == "get_sequences":
            return self.get_sequences


class TestGetAll(SequenceTests):
    package = "q2_fondue.tests"

    def test_get_all_single(self):
        """
        Test verifying that pipeline get_all calls all expected actions,
        individual actions are tested in details in respective test classes
        """
        mock_ctx = FakeCtx(
            ids_path=self.get_data_path("SRR123456_md.tsv"),
            meta_path=self.get_data_path("sra-metadata-mock.tsv"),
        )
        obs_meta, _, _, obs_failed = get_all(
            mock_ctx, mock_ctx.ids, "fake@email.com", retries=1
        )

        mock_ctx.get_metadata.assert_called_once_with(
            mock_ctx.ids, "fake@email.com", 1, "INFO", None
        )
        mock_ctx.get_sequences.assert_called_once_with(
            ANY, "fake@email.com", 1, 1, "INFO"
        )

        run_ids = mock_ctx.get_sequences.call_args_list[0][0][0]
        run_ids = run_ids.view(pd.DataFrame).index.to_list()
        self.assertListEqual(run_ids, ["SRR123456"])

        self.assertEqual(obs_meta, mock_ctx.meta)
        self.assertEqual(obs_failed, mock_ctx.failed)

    def test_get_all_multi_with_missing_ids(self):
        """
        Test verifying that pipeline get_all calls all expected actions,
        individual actions are tested in details in respective test classes
        """
        mock_ctx = FakeCtx(
            ids_path=self.get_data_path("SRR1234567_md.tsv"),
            meta_path=self.get_data_path("sra-metadata-mock.tsv"),
            failed_ids=["SRR123457"],
        )
        obs_meta, _, _, obs_failed = get_all(
            mock_ctx, mock_ctx.ids, "fake@email.com", retries=1
        )

        mock_ctx.get_metadata.assert_called_once_with(
            mock_ctx.ids, "fake@email.com", 1, "INFO", None
        )
        mock_ctx.get_sequences.assert_called_once_with(
            ANY, "fake@email.com", 1, 1, "INFO"
        )

        run_ids = mock_ctx.get_sequences.call_args_list[0][0][0]
        run_ids = run_ids.view(pd.DataFrame).index.to_list()
        self.assertListEqual(run_ids, ["SRR123456"])

        self.assertEqual(obs_meta, mock_ctx.meta)
        self.assertListEqual(
            obs_failed.view(pd.DataFrame).index.to_list(), ["SRR123457"]
        )


if __name__ == "__main__":
    unittest.main()
