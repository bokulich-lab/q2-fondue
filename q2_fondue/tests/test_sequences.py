# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import tempfile
from qiime2.plugin.testing import TestPluginBase
from q2_fondue.sequences import get_sequences


class TestSequenceFetching(TestPluginBase):
    package = 'q2_fondue.tests'

    def setUp(self):
        super().setUp()
        self.temp_dir = tempfile.TemporaryDirectory(
            prefix='q2-fondue-test-temp-')
        # todo use temp_dir for output_dir

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_method_get_one_single_sequence(self):
        # todo add verification of test outputs!
        study_ids = ['SRR000001']
        get_sequences(study_ids, output_dir=self.temp_dir.name)

    def test_method_get_multiple_single_sequences(self):
        study_ids = [
            'ERR3978173', 'ERR3978174']
        get_sequences(study_ids, output_dir=self.temp_dir.name)

    def test_method_get_single_and_paired_sequences(self):
        # ! currently only single reads
        study_ids = [
            'SRR000001', 'ERR3978173']
        get_sequences(study_ids=study_ids, output_dir=self.temp_dir.name)

    def test_method_invalid_acc(self):
        study_ids = ['ERR39781ab']
        with self.assertRaisesRegex(
                ValueError, 'could not be downloaded with'):
            get_sequences(study_ids=study_ids, output_dir=self.temp_dir.name)

    # def test_action(self):
    #     # todo add testing of action
    #     study_ids = [
    #         'ERR3978173', 'ERR3978174']
    #     q2_fondue.actions.get_sequences(study_ids)
