# ----------------------------------------------------------------------------
# Copyright (c) 2025, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import pandas as pd
import unittest

from pandas.testing import assert_frame_equal
from qiime2.plugins import fondue
from unittest.mock import patch

from q2_fondue.tests.test_sequences import SequenceTests


class TestQuery(SequenceTests):
    package = "q2_fondue.tests"

    @patch("q2_fondue.query._get_run_ids", return_value=["SRR123", "SRR234"])
    def test_query(self, mock_ids):
        query = "some magical query text"

        (obs_ids,) = fondue.actions.get_ids_from_query(
            query, "fake@email.com", 1, "DEBUG"
        )
        exp_ids = pd.DataFrame(
            index=pd.Index(["SRR123", "SRR234"], name="ID"),
            columns=[],
        )

        mock_ids.assert_called_once_with(
            "fake@email.com", 1, None, query, "biosample", "DEBUG"
        )
        assert_frame_equal(obs_ids.view(pd.DataFrame), exp_ids)


if __name__ == "__main__":
    unittest.main()
