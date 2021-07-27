# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import gzip
import itertools
from qiime2.plugin.testing import TestPluginBase
from q2_types.per_sample_sequences import FastqGzFormat
from q2_fondue.sequences import get_sequences


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
            # assemble sequences
            nb_obs_samples += 1
            file_fh = gzip.open(str(file_loc), 'rt')

            # Assemble expected sequences, per-sample
            file_seqs = [r for r in itertools.zip_longest(*[file_fh] * 4)]

            ls_seq_length.append(len(file_seqs))

        return nb_obs_samples, ls_seq_length

    def _validate_counts(self, single_output, paired_output,
                         ls_exp_lengths_single, ls_exp_lengths_paired):
        nb_samples_single, ls_seq_length_single = \
            self._validate_sequences_in_samples(
                single_output)
        print(ls_seq_length_single)
        self.assertTrue(nb_samples_single == 1)
        self.assertTrue(ls_seq_length_single == ls_exp_lengths_single)

        # test paired sequences
        nb_samples_paired, ls_seq_length_paired = \
            self._validate_sequences_in_samples(
                paired_output)
        print(ls_seq_length_paired)
        self.assertTrue(nb_samples_paired == 2)
        self.assertTrue(ls_seq_length_paired == ls_exp_lengths_paired)


class TestSequenceFetching(SequenceTests):

    def test_method_single_only(self):
        # test currently dependent on this one accession id: ERR3978173
        # ! other less ID dependent ideas are welcome
        sample_ids = ['ERR3978173']
        single_read_output, paired_end_output = get_sequences(sample_ids)
        self._validate_counts(single_read_output, paired_end_output,
                              [39323], [0, 0])

    def test_method_paired_only(self):
        # test currently dependent on this one accession id: SRR15233931
        # ! other less ID dependent ideas are welcome
        sample_ids = ['SRR15233931']
        single_read_output, paired_end_output = get_sequences(sample_ids)
        self._validate_counts(single_read_output, paired_end_output,
                              [0], [270510, 270510])

    def test_method_single_n_paired(self):
        # test currently dependent on this one accession id: SRR000001
        # ! other less ID dependent ideas are welcome
        sample_ids = ['SRR000001']
        single_read_output, paired_end_output = get_sequences(sample_ids)
        self._validate_counts(single_read_output, paired_end_output,
                              [236041], [236041, 236041])

    def test_method_invalid_accession_id(self):
        sample_ids = ['ERR39781ab']
        with self.assertRaisesRegex(
                ValueError, 'could not be downloaded with'):
            get_sequences(sample_ids)
