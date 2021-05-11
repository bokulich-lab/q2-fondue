# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest
from unittest.mock import patch

import entrezpy.efetch.efetcher as ef
import pandas as pd
from entrezpy.requester.requester import Requester

from q2_fondue.metadata import (_efetcher_inquire)
from q2_fondue.tests._utils import _TestPluginWithEntrezFakeComponents


class TestMetadataFetching(_TestPluginWithEntrezFakeComponents):
    package = 'q2_fondue.tests'

    def setUp(self):
        super().setUp()
        self.fake_efetcher = ef.Efetcher(
            'fake_efetcher', 'fake@email.com', apikey=None,
            apikey_var=None, threads=1, qid=None
        )

    def test_efetcher_inquire_single(self):
        with patch.object(Requester, 'request') as mock_request:
            mock_request.return_value = self.xml_to_response('single')
            obs_df = _efetcher_inquire(
                self.fake_efetcher, ['FAKEID1'])
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_efetch_request(['FAKEID1'])
        exp_df = self.generate_expected_df().iloc[[0]]

        for arg in self.request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        pd.testing.assert_frame_equal(
            exp_df.sort_index(axis=1), obs_df.sort_index(axis=1))

    def test_efetcher_inquire_multi(self):
        with patch.object(Requester, 'request') as mock_request:
            mock_request.return_value = self.xml_to_response('multi')
            obs_df = _efetcher_inquire(
                self.fake_efetcher, ['FAKEID1', 'FAKEID2'])
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_efetch_request(
            ['FAKEID1', 'FAKEID2'], size=2)
        exp_df = self.generate_expected_df()

        for arg in self.request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        pd.testing.assert_frame_equal(
            exp_df.sort_index(axis=1), obs_df.sort_index(axis=1))


if __name__ == "__main__":
    unittest.main()
