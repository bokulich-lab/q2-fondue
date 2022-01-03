# ----------------------------------------------------------------------------
# Copyright (c) 2022, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest

import pandas as pd
from pandas._testing import assert_frame_equal, assert_series_equal

from q2_fondue.entrezpy_clients._efetch import EFetchResult
from q2_fondue.tests._utils import _TestPluginWithEntrezFakeComponents


class TestEfetchClients(_TestPluginWithEntrezFakeComponents):
    package = 'q2_fondue.tests'

    def test_efetch_create_study(self):
        obs_id = self.efetch_result_single._create_study(self.metadata_dict)

        self.assertEqual('ERP120343', obs_id)
        self.assertEqual(1, len(self.efetch_result_single.studies))

        # check study attributes
        obs_study = self.efetch_result_single.studies['ERP120343']
        exp_study, _, _, _ = self.generate_sra_metadata()
        self.assertEqual(exp_study, obs_study)

    def test_efetch_create_samples(self):
        study_id = 'ERP120343'
        exp_std, exp_samp, _, _ = self.generate_sra_metadata()
        self.efetch_result_single.studies[study_id] = exp_std

        obs_ids = self.efetch_result_single._create_samples(
            self.metadata_dict, study_id=study_id)

        self.assertEqual(['ERS4372624'], obs_ids)
        self.assertEqual(1, len(self.efetch_result_single.samples))

        obs_sample = self.efetch_result_single.samples['ERS4372624']
        self.assertEqual(exp_samp, obs_sample)

    def test_efetch_extract_library_info(self):
        obs_lib = self.efetch_result_single._extract_library_info(
            self.metadata_dict)
        self.assertEqual(self.library_meta, obs_lib)

    def test_efetch_create_experiment(self):
        study_id, sample_id,  = 'ERP120343', 'ERS4372624'
        exp_std, exp_samp, exp_exp, _ = self.generate_sra_metadata()
        self.efetch_result_single.studies[study_id] = exp_std
        self.efetch_result_single.samples[sample_id] = exp_samp

        obs_id = self.efetch_result_single._create_experiment(
            self.metadata_dict, sample_id=sample_id)

        self.assertEqual('ERX3980916', obs_id)
        self.assertEqual(1, len(self.efetch_result_single.experiments))

        obs_experiment = self.efetch_result_single.experiments['ERX3980916']
        self.assertEqual(exp_exp, obs_experiment)

    def test_efetch_create_single_run(self):
        study_id, sample_id,  = 'ERP120343', 'ERS4372624'
        experiment_id = 'ERX3980916'
        exp_std, exp_samp, exp_exp, exp_runs = self.generate_sra_metadata()
        self.efetch_result_single.studies[study_id] = exp_std
        self.efetch_result_single.samples[sample_id] = exp_samp
        self.efetch_result_single.experiments[experiment_id] = exp_exp

        obs_id = self.efetch_result_single._create_single_run(
            self.metadata_dict,
            exp_id=experiment_id,
            desired_id='FAKEID1'
        )

        self.assertEqual('FAKEID1', obs_id)
        self.assertEqual(1, len(self.efetch_result_single.runs))

        obs_run = self.efetch_result_single.runs['FAKEID1']
        self.assertEqual(exp_runs[0], obs_run)

    def test_efetch_process_single_id(self):
        study_id, sample_id, = 'ERP120343', 'ERS4372624'
        experiment_id, run_ids = 'ERX3980916', ['FAKEID1']

        obs_ids = self.efetch_result_single._process_single_id(
            self.metadata_dict, desired_id=run_ids[0])

        self.assertListEqual(run_ids, obs_ids)
        self.assertEqual(1, len(self.efetch_result_single.runs))
        self.assertEqual(1, len(self.efetch_result_single.experiments))
        self.assertEqual(1, len(self.efetch_result_single.samples))
        self.assertEqual(1, len(self.efetch_result_single.studies))

        self.assertEqual(
            study_id, self.efetch_result_single.samples[sample_id].study_id)
        self.assertEqual(
            sample_id,
            self.efetch_result_single.experiments[experiment_id].sample_id)

    def test_efetch_custom_attributes_to_dict_from_list(self):
        attr_dicts = [{'TAG': 'tag1', 'VALUE': 'value1'},
                      {'TAG': 'tag2', 'VALUE': 'value3'},
                      {'TAG': 'tag1', 'VALUE': 'value2'},
                      {'TAG': 'tag2', 'VALUE': 'value2'},
                      {'TAG': 'tag3', 'VALUE': 'value8'}
                      ]
        obs_attr = self.efetch_result_single._custom_attributes_to_dict(
            attr_dicts, level='unicorn'
        )
        exp_attr = {'tag1_1 [unicorn]': 'value1', 'tag1_2 [unicorn]': 'value2',
                    'tag2_1 [unicorn]': 'value2', 'tag2_2 [unicorn]': 'value3',
                    'tag3 [unicorn]': 'value8'}
        self.assertDictEqual(exp_attr, obs_attr)

    def test_efetch_custom_attributes_to_dict_from_dict(self):
        attr_dicts = {'TAG': 'tag1', 'VALUE': 'value1'}
        obs_attr = self.efetch_result_single._custom_attributes_to_dict(
            attr_dicts, level='unicorn'
        )
        exp_attr = {'tag1 [unicorn]': 'value1'}
        self.assertDictEqual(exp_attr, obs_attr)

    def test_efetch_extract_custom_attributes(self):
        attr_dict = self.metadata_dict['STUDY']
        obs_attr = self.efetch_result_single._extract_custom_attributes(
            attr_dict, level='study'
        )
        exp_attr = {'ENA-FIRST-PUBLIC [STUDY]': '2020-05-31',
                    'ENA-LAST-UPDATE [STUDY]': '2020-03-04'}
        self.assertDictEqual(exp_attr, obs_attr)

    def test_efetch_add_metadata_one_experiment_one_run(self):
        self.efetch_result_single.add_metadata(
            self.xml_to_response('single'), ['FAKEID1']
        )
        self.assertDictEqual(self.efetch_result_single.metadata,
                             {0: ['FAKEID1']})
        self.assertEqual(1, self.efetch_result_single.size())
        self.assertFalse(self.efetch_result_single.isEmpty())

    def test_efetch_add_metadata_one_experiment_many_runs(self):
        self.efetch_result_single.add_metadata(
            self.xml_to_response('single', '_complex'), ['FAKEID1', 'FAKEID3']
        )
        self.assertDictEqual(self.efetch_result_single.metadata,
                             {0: ['FAKEID1'], 1: ['FAKEID3']})
        self.assertEqual(2, self.efetch_result_single.size())
        self.assertFalse(self.efetch_result_single.isEmpty())

    def test_efetch_add_metadata_many_experiments_one_run(self):
        self.efetch_result_single.add_metadata(
            self.xml_to_response('multi'), ['FAKEID1']
        )
        self.assertDictEqual(self.efetch_result_single.metadata,
                             {0: ['FAKEID1']})
        self.assertEqual(1, self.efetch_result_single.size())
        self.assertFalse(self.efetch_result_single.isEmpty())

    def test_efetch_add_metadata_many_experiments_many_runs(self):
        self.efetch_result_single.add_metadata(
            self.xml_to_response('multi'), ['FAKEID1', 'FAKEID2']
        )
        self.assertDictEqual(self.efetch_result_single.metadata,
                             {0: ['FAKEID1'], 1: ['FAKEID2']})
        self.assertEqual(2, self.efetch_result_single.size())
        self.assertFalse(self.efetch_result_single.isEmpty())

    def test_efetch_add_metadata_many_experiments_many_runs_missing_ids(self):
        self.efetch_result_single.add_metadata(
            self.xml_to_response('single'), ['FAKEID1', 'FAKEID2']
        )

        self.assertDictEqual(self.efetch_result_single.metadata,
                             {0: ['FAKEID1']})
        self.assertEqual(1, self.efetch_result_single.size())
        self.assertFalse(self.efetch_result_single.isEmpty())
        self.assertListEqual(['FAKEID2'],
                             self.efetch_result_single.missing_uids)

    def test_efetch_to_df(self):
        self.efetch_result_single.add_metadata(
            self.xml_to_response('multi'), ['FAKEID1', 'FAKEID2'])

        obs = self.efetch_result_single.metadata_to_df().sort_index(axis=1)
        exp = self.generate_expected_df().sort_index(axis=1)
        assert_frame_equal(exp, obs)

    def test_efetch_extract_run_ids(self):
        self.efetch_result_single.extract_run_ids(
            self.xml_to_response('runs', prefix='efetch'))
        exp_ids = {
            0: ['SRR13961771'],
            1: ['SRR000007', 'SRR000018', 'SRR000020', 'SRR000038',
                'SRR000043', 'SRR000046', 'SRR000048', 'SRR000050',
                'SRR000057', 'SRR000058'],
            2: ['SRR13961759']
        }
        self.assertDictEqual(exp_ids, self.efetch_result_single.metadata)

    def test_efetch_metadata_to_series(self):
        self.efetch_result_single.extract_run_ids(
            self.xml_to_response('runs', prefix='efetch'))

        obs_ids = self.efetch_result_single.metadata_to_series()
        exp_ids = pd.Series([
            'SRR13961771', 'SRR000007', 'SRR000018', 'SRR000020',
            'SRR000038', 'SRR000043', 'SRR000046', 'SRR000048', 'SRR000050',
            'SRR000057', 'SRR000058', 'SRR13961759'], index=range(12))
        assert_series_equal(exp_ids, obs_ids)

    def test_efanalyzer_analyze_result(self):
        self.efetch_analyzer.analyze_result(
            response=self.xml_to_response('single'),
            request=self.generate_ef_request(['FAKEID1'])
        )

        self.assertTrue(isinstance(self.efetch_analyzer.result, EFetchResult))
        exp = {0: ['FAKEID1']}
        self.assertDictEqual(exp, self.efetch_analyzer.result.metadata)


if __name__ == "__main__":
    unittest.main()
