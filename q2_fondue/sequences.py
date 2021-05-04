# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import os
import re
from subprocess import run
# from qiime2.plugin import model
from q2_types.per_sample_sequences import \
    (CasavaOneEightSingleLanePerSampleDirFmt)


# class SequencesDirFmt(model.DirectoryFormat):
#     # ? lacks attribute '__name__' - must be implemented wrongly..
#     sequences = model.FileCollection(
#         r'*\.fastq\.gz', format=FastqGzFormat)

#     @sequences.set_path_maker
#     def sequences_path_maker(self, acc):
#         return '%s.fastq.gz' % (acc)


def get_sequences(study_ids: list,
                  output_dir: str,
                  general_retries: int = 2,
                  threads:
                  int = 6) -> CasavaOneEightSingleLanePerSampleDirFmt():
    """
    Function to run SRA toolkit fasterq-dump to get sequences of accessions
    in `study_ids`. Supports mulitple tries (`general_retries`) and can use
    multiple `threads`.
    """
    # todo: make helper function out of below components
    results = CasavaOneEightSingleLanePerSampleDirFmt()

    # func: _download_sequences(tmpdirname, acc, general_retries, threads)
    # with tempfile.TemporaryDirectory() as tmpdirname:
    # get all study ids sequences + tmp files into tmpdirname
    for acc in study_ids:
        print("Downloading sequences of study: {}...".format(acc))
        retries = general_retries

        acc_fastq_single = os.path.join(output_dir,
                                        acc + '.fastq')
        acc_fastq_paired = os.path.join(output_dir,
                                        acc + '_1.fastq')

        cmd_fasterq = ["fasterq-dump",
                       "-O", output_dir,
                       "-e", str(threads),
                       acc]

        # try "retries" times to get sequence data
        while retries >= 0:
            result = run(cmd_fasterq, text=True, capture_output=True)

            if not (os.path.isfile(acc_fastq_single) |
                    os.path.isfile(acc_fastq_paired)):
                retries -= 1
                print('retrying {} times'.format(retries+1))
            else:
                retries = -1

        # raise error if all three attempts failed - else gzip files
        if not (os.path.isfile(acc_fastq_single) |
                os.path.isfile(acc_fastq_paired)):
            raise ValueError('{} could not be downloaded with the '
                             'following fasterq-dump error '
                             'returned: {}'
                             .format(acc, result.stderr))

        else:
            # gzip all files in folder
            cmd_gzip = ["gzip",
                        "-r", output_dir]
            run(cmd_gzip, text=True, capture_output=True)

            # rename all files in folder (depending on single or paired read)
            # to casava format
            # todo create helper func: _rename()
            for filename in os.listdir(output_dir):
                print(filename)
                acc = re.search(r'(.*)\.fastq\.gz$', filename).group(1)
                if acc.endswith('_1') or acc.endswith('_2'):
                    print('Paired end reads are not supported yet, '
                          'so {} will not be processed any further.'
                          .format(acc))
                    os.remove(os.path.join(output_dir, filename))
                else:
                    # convert to casava
                    new_name = '%s_00_L001_R%d_001.fastq.gz' % (acc, 1)
                    os.rename(os.path.join(output_dir, filename),
                              os.path.join(output_dir, new_name))

            # # read fastq.gz and return
            # fh = gzip.open(os.path.join(output_dir, new_name), 'rt')

        # try:
        #     artifact = sdk.Artifact.import_data(
        # 'SampleData[SequencesWithQuality]',
        #  output_dir,
        #  view_type='CasavaOneEightSingleLanePerSampleDirFmt')
        #     artifact.save(output_dir)
        # except Exception:
        #     print('Exception occurred')
        # once all acc were downloaded and zipped
        # ? somehow return them as qza file
        # os.rename(tmpdirname, str(result))
        # util.duplicate(output_dir, str(result))

    return results
