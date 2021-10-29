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
import entrezpy.esearch.esearcher as es
import pandas as pd
from entrezpy.requester.requester import Requester

from q2_fondue.metadata import (_efetcher_inquire, _validate_esearch_result,
                                _determine_id_type, InvalidIDs)
from q2_fondue.tests._utils import _TestPluginWithEntrezFakeComponents


class TestMetadataFetching(_TestPluginWithEntrezFakeComponents):
    package = 'q2_fondue.tests'

    def setUp(self):
        super().setUp()
        self.fake_efetcher = ef.Efetcher(
            'fake_efetcher', 'fake@email.com', apikey=None,
            apikey_var=None, threads=1, qid=None
        )
        self.fake_esearcher = es.Esearcher(
            'fake_esearcher', 'fake@email.com', apikey=None,
            apikey_var=None, threads=1, qid=None
        )

    def test_determine_id_type_sample(self):
        ids = ['SRS123', 'ERS123']

        obs = _determine_id_type(ids)
        exp = 'sample'
        self.assertEqual(exp, obs)

    def test_determine_id_type_run(self):
        ids = ['SRR123', 'ERR123']

        obs = _determine_id_type(ids)
        exp = 'run'
        self.assertEqual(exp, obs)

    def test_determine_id_type_mixed(self):
        ids = ['SRS123', 'ERR123']

        with self.assertRaisesRegexp(
                InvalidIDs, 'type of provided IDs is either not supported'):
            _determine_id_type(ids)

    def test_determine_id_type_unknown(self):
        ids = ['ABC123', 'DEF123']

        with self.assertRaisesRegexp(
                InvalidIDs, 'type of provided IDs is either not supported'):
            _determine_id_type(ids)

    def test_efetcher_inquire_single(self):
        with patch.object(Requester, 'request') as mock_request:
            mock_request.return_value = self.xml_to_response('single')
            obs_df = _efetcher_inquire(
                self.fake_efetcher, ['FAKEID1'], id_type='run')
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_ef_request(['FAKEID1'])
        exp_df = self.generate_expected_df().iloc[[0]]

        for arg in self.efetch_request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        pd.testing.assert_frame_equal(
            exp_df.sort_index(axis=1), obs_df.sort_index(axis=1))

    def test_efetcher_inquire_multi(self):
        with patch.object(Requester, 'request') as mock_request:
            mock_request.return_value = self.xml_to_response('multi')
            obs_df = _efetcher_inquire(
                self.fake_efetcher, ['FAKEID1', 'FAKEID2'], id_type='run')
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_ef_request(
            ['FAKEID1', 'FAKEID2'], size=2)
        exp_df = self.generate_expected_df()

        for arg in self.efetch_request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        pd.testing.assert_frame_equal(
            exp_df.sort_index(axis=1), obs_df.sort_index(axis=1))

    def test_esearcher_inquire_single(self):
        with patch.object(Requester, 'request') as mock_request:
            mock_request.return_value = self.json_to_response(
                'single', '_correct', True)
            obs_result = _validate_esearch_result(
                self.fake_esearcher, ['SRR000001'])
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_es_request('SRR000001')

        for arg in self.esearch_request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        self.assertTrue(obs_result)

    def test_esearcher_inquire_multi(self):
        with patch.object(Requester, 'request') as mock_request:
            mock_request.return_value = self.json_to_response(
                'multi', '_correct', True)
            obs_result = _validate_esearch_result(
                self.fake_esearcher, ['SRR000001', 'SRR000013', 'ERR3978173'])
        obs_request, = mock_request.call_args.args
        exp_request = self.generate_es_request(
            'SRR000001 OR SRR000013 OR ERR3978173')

        for arg in self.esearch_request_properties:
            self.assertEqual(
                getattr(exp_request, arg), getattr(obs_request, arg))
        mock_request.assert_called_once()
        self.assertTrue(obs_result)


if __name__ == "__main__":
    unittest.main()
