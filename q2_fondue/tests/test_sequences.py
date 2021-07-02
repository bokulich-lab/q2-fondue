# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from qiime2.plugin.testing import TestPluginBase
from q2_fondue.sequences import get_sequences
import gzip
import itertools
from q2_types.per_sample_sequences import (
    FastqGzFormat)


class TestSequenceFetching(TestPluginBase):
    package = 'q2_fondue.tests'

    def setUp(self):
        super().setUp()

    def test_method_get_single_sequences(self):
        # todo: test currently dependent on this one accession id: ERR3978173
        # todo: check how to adjust test to loose dependency
        study_ids = ['ERR3978173']
        single_read_output = get_sequences(study_ids)

        single_samples = single_read_output.sequences.iter_views(FastqGzFormat)
        nb_obs_samples = 0

        for (_, file_loc) in single_samples:
            self.assertTrue('R1' in str(file_loc))

            # assemble sequences
            nb_obs_samples += 1
            file_fh = gzip.open(str(file_loc), 'rt')

            # Assemble expected sequences, per-sample
            file_seqs = [r for r in itertools.zip_longest(*[file_fh] * 4)]

            self.assertTrue(len(file_seqs) == 39323)

        self.assertTrue(nb_obs_samples == 1)

    def test_method_get_single_and_paired_sequences(self):
        # ! currently only single reads supported
        # todo add verification
        # 'ERR3978173' only single reads
        study_ids = ['SRR000001', 'ERR3978173']
        get_sequences(study_ids)

    def test_method_invalid_accession_id(self):
        study_ids = ['ERR39781ab']
        with self.assertRaisesRegex(
                ValueError, 'could not be downloaded with'):
            get_sequences(study_ids)
