# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from qiime2.plugin.testing import TestPluginBase
from q2_fondue.sequences import get_sequences
from qiime2.plugins import fondue


class TestSequenceFetching(TestPluginBase):
    package = 'q2_fondue.tests'

    def setUp(self):
        super().setUp()

    def test_method_get_one_study(self):
        # todo add proper verification of test outputs below!
        # contains single and paired reads
        study_ids = ['SRR000001']
        get_sequences(study_ids)

    def test_method_get_single_and_paired_sequences(self):
        # ! currently only single reads supported
        # todo add verification
        # 'ERR3978173' only single reads
        study_ids = [
            'SRR000001', 'ERR3978173']
        get_sequences(study_ids)

    def test_method_invalid_acc(self):
        study_ids = ['ERR39781ab']
        with self.assertRaisesRegex(
                ValueError, 'could not be downloaded with'):
            get_sequences(study_ids)

    def test_q2action(self):
        # todo add verification
        study_ids = ['ERR3978174']
        fondue.actions.get_sequences(study_ids)
