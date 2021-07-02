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


class SequenceTests(TestPluginBase):
    # class is inspired by class SubsampleTest in
    # q2_demux.tests.test_subsample
    package = 'q2_fondue.tests'

    def _validate_sequences_in_samples(self, read_output):
        nb_obs_samples = 0
        ls_seq_length = []
        samples = read_output.sequences.iter_views(FastqGzFormat)

        # iterate over each sample
        for (_, file_loc) in samples:
            # # Process forward only if `forward==True`
            # if 'R1' in str(file_loc):
            #     seq_type = 'single'
            # # Process rev only if `forward==False`
            # if 'R2' in str(file_loc):
            #     seq_type = 'double'

            # assemble sequences
            nb_obs_samples += 1
            file_fh = gzip.open(str(file_loc), 'rt')

            # Assemble expected sequences, per-sample
            file_seqs = [r for r in itertools.zip_longest(*[file_fh] * 4)]

            ls_seq_length.append(len(file_seqs))

        return nb_obs_samples, ls_seq_length


class TestSequenceFetching(SequenceTests):
    def setUp(self):
        super().setUp()

    def test_method_get_single_sequences(self):
        # test currently dependent on this one accession id: ERR3978173
        # todo: check how to adjust test to lose this dependency
        study_ids = ['ERR3978173']
        single_read_output = get_sequences(study_ids)

        nb_obs_samples, ls_seq_length = self._validate_sequences_in_samples(
            single_read_output)

        self.assertTrue(nb_obs_samples == 1)
        self.assertTrue(ls_seq_length == [39323])

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
