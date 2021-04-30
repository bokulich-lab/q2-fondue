# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import os
import tempfile
from subprocess import run
from qiime2.plugin import model
from q2_types.per_sample_sequences import FastqGzFormat


class SequencesDirFmt(model.DirectoryFormat):
    # ? lacks attribute '__name__' - must be implemented wrongly..
    sequences = model.FileCollection(
        r'*\.fastq\.gz', format=FastqGzFormat)

    @sequences.set_path_maker
    def sequences_path_maker(self, acc):
        return '%s.fastq.gz' % (acc)


def get_sequences(study_ids: list, general_retries: int = 2,
                  threads: int = 6) -> SequencesDirFmt():
    """
    Function to run SRA toolkit fasterq-dump to get sequences of accessions
    in `study_ids`. Supports mulitple tries (`general_retries`) and can use
    multiple `threads`.
    """
    # todo: make helper function out of below components
    results = SequencesDirFmt()  # todo add some DirFmt()

    # func: _download_sequences(tmpdirname, acc, general_retries, threads)
    with tempfile.TemporaryDirectory() as tmpdirname:
        # get all study ids sequences + tmp files into tmpdirname
        for acc in study_ids:
            print("Downloading sequences of study: {}...".format(acc))
            retries = general_retries

            acc_fastq_single = os.path.join(tmpdirname,
                                            acc + '.fastq')
            acc_fastq_paired = os.path.join(tmpdirname,
                                            acc + '_1.fastq')

            cmd_fasterq = ["fasterq-dump",
                           "-O", tmpdirname,
                           "-e", str(threads),
                           acc]
            # todo adjust below according to _denoise_single()
            # todo cmd run here:
            # todo https://github.com/qiime2/q2-dada2/blob/
            # todo b8daa524e4d91e1ae09c6e10e9889b20a9d7ae60/
            # todo q2_dada2/_denoise.py

            # try "retries" times to get sequence data
            while retries >= 0:
                result = run(cmd_fasterq, text=True, capture_output=True)

                if not (os.path.isfile(acc_fastq_single) |
                        os.path.isfile(acc_fastq_paired)):
                    retries -= 1
                    print('retrying {} times'.format(retries+1))
                else:
                    retries = -1

            # raise error if all three attempts failed else gzip files
            if not (os.path.isfile(acc_fastq_single) |
                    os.path.isfile(acc_fastq_paired)):
                raise ValueError('{} could not be downloaded with the '
                                 'following fasterq-dump error '
                                 'returned: {}'
                                 .format(acc, result.stderr))

            else:
                # gzip as cmd
                cmd_gzip = ["gzip",
                            "-r", tmpdirname]
                result_gzip = run(cmd_gzip, text=True, capture_output=True)

                # read fastq.gz and return
                # fh = gzip.open(filepath, 'rt')
                print(result_gzip)

        # once all acc were downloaded and zipped
        # ? somehow return them as qza file
        # os.rename(tmpdirname, str(result))
        return results
