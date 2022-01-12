# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest

from q2_fondue.entrezpy_clients._elink import ELinkAnalyzer, ELinkResult
from q2_fondue.tests._utils import _TestPluginWithEntrezFakeComponents


class TestElinkClients(_TestPluginWithEntrezFakeComponents):
    package = 'q2_fondue.tests'

    def setUp(self):
        super().setUp()
        self.elink_response = {
            'dbfrom': 'bioproject',
            'ids': ['33627'],
            'linksetdbhistories': [
                {'dbto': 'sra', 'linkname': 'bioproject_sra', 'querykey': '2'}
            ],
            'webenv': 'MCID_61725b77d7807e40801b66a9'
        }

    def test_elresult_parse_link_results(self):
        elink_result = self.generate_el_result('single', '')
        elink_result.parse_link_results(
            self.json_to_response('single', utility='elink'))

        exp_webenv = 'MCID_61725b77d7807e40801b66a9'
        exp_query_key = '2'
        self.assertDictEqual(self.elink_response, elink_result.result)
        self.assertEqual(exp_webenv, elink_result.webenv)
        self.assertEqual(exp_query_key, elink_result.query_key)
        self.assertEqual(4, elink_result.size())

    def test_elresult_get_link_params(self):
        elink_result = self.generate_el_result('single', '')
        elink_result.webenv = 'some env'
        elink_result.query_key = '4'

        obs_params = elink_result.get_link_parameter()
        exp_params = {
            'db': 'sra', 'WebEnv': 'some env', 'query_key': '4',
            'cmd': 'neighbor_history', 'retmax': 10000
        }
        self.assertDictEqual(exp_params, obs_params)

    def test_esanalyzer_analyze_result(self):
        el_analyzer = ELinkAnalyzer()
        el_analyzer.analyze_result(
            response=self.json_to_response('single', utility='elink'),
            request=self.generate_el_request()
        )

        self.assertTrue(
            isinstance(el_analyzer.result, ELinkResult))


if __name__ == "__main__":
    unittest.main()
