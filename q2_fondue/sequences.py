# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import os
import glob
import re
import gzip
import itertools
# import pathlib
# import pandas as pd
from subprocess import run
from q2_types.per_sample_sequences import \
    (CasavaOneEightSingleLanePerSampleDirFmt)  # , FastqManifestFormat, _util)


# todo move below functions starting with '_' to util.py
def _run_cmd_fasterq(acc: str, output_dir: str, threads: int,
                     general_retries: int):
    """
    Helper function running fasterq-dump 'general_retries' times
    """

    print("Downloading sequences of study: {}...".format(acc))

    retries = general_retries

    acc_fastq_single = os.path.join(output_dir,
                                    acc + '.fastq')
    acc_fastq_paired = os.path.join(output_dir,
                                    acc + '_1.fastq')
    # todo: add temp folder path as well
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

    return result


def _process_downloaded_sequences(output_dir):
    # todo: clean up function

    # remove paired sequence reads in output_dir
    ls_seq_paired_1 = glob.glob(os.path.join(
        output_dir, '*_1.fastq'), recursive=True)
    ls_seq_paired_2 = glob.glob(os.path.join(
        output_dir, '*_2.fastq'), recursive=True)

    for seq_file in (ls_seq_paired_1 + ls_seq_paired_2):
        print('Paired end reads are not supported yet, '
              'so {} will not be processed any further.'
              .format(seq_file))
        os.remove(seq_file)

    # gzip all remaining files in folder
    cmd_gzip = ["gzip",
                "-r", output_dir]
    run(cmd_gzip, text=True, capture_output=True)

    # rename all files to casava format
    for filename in os.listdir(output_dir):
        acc = re.search(r'(.*)\.fastq\.gz$', filename).group(1)

        # convert to casava
        new_name = '%s_00_L001_R%d_001.fastq.gz' % (acc, 1)
        os.rename(os.path.join(output_dir, filename),
                  os.path.join(output_dir, new_name))


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
    results = CasavaOneEightSingleLanePerSampleDirFmt()

    # get all study ids sequences + tmp files into tmpdirname
    # ? maybe add: with tempfile.TemporaryDirectory() as tmpdirname:
    # ? rename output_dir to tmpdirname
    for acc in study_ids:
        result = _run_cmd_fasterq(acc, output_dir, threads,
                                  general_retries)

        if len(os.listdir(output_dir)) == 0:
            # raise error if all general_retries attempts failed
            raise ValueError('{} could not be downloaded with the '
                             'following fasterq-dump error '
                             'returned: {}'
                             .format(acc, result.stderr))
        else:
            continue

    _process_downloaded_sequences(output_dir)

    # todo write to _write_sequences()
    def _read_fastq_seqs(filepath):
        # function copied from q2_demux._demux import _read_fastq_seqs
        # This function is adapted from @jairideout's SO post:
        # http://stackoverflow.com/a/39302117/3424666
        fh = gzip.open(filepath, 'rt')
        for seq_header, seq, qual_header, qual in itertools.zip_longest(*[fh] * 4):
            yield (seq_header.strip(), seq.strip(), qual_header.strip(),
                   qual.strip())

    for filename in os.listdir(output_dir):
        fwd_path_in = os.path.join(output_dir, filename)
        fwd_path_out = str(results.path / filename)

        with gzip.open(str(fwd_path_out), mode='w') as fwd:
            for fwd_rec in _read_fastq_seqs(fwd_path_in):
                fwd.write(('\n'.join(fwd_rec) + '\n').encode('utf-8'))

    # for _, fwd_path in manifest.itertuples():
    #     fwd_name = os.path.basename(fwd_path)
    #     fwd_path_in = str(sequences.path / fwd_name)
    #     fwd_path_out = str(results.path / fwd_name)
    #     with gzip.open(str(fwd_path_out), mode='w') as fwd:
    #         for fwd_rec in _read_fastq_seqs(fwd_path_in):
    #             if random.random() <= fraction:
    #                 fwd.write(('\n'.join(fwd_rec) + '\n').encode('utf-8'))

    # # Experiment: define manifest file propriately
    # str4manifest = 'sample-id,filename,direction\n'
    # in func: process_downloded_sequences extend string to:
    #   += '%s,%s,forward\n' % (acc, new_name)

    # tmp_manifest = FastqManifestFormat()
    # with tmp_manifest.open() as fh:
    #     fh.write(str4manifest)
    # manifest = tmp_manifest.view(pd.DataFrame)
    # manifest.to_csv(os.path.join(output_dir, 'MANIFEST'))
    # df_manifest = _util._manifest_to_df(tmp_manifest, output_dir)

    # # Experiment for manifest file
    # def munge_fn_closure(val):
    #     if val is not None:
    #         return pathlib.Path(val).name
    #     return val

    # for column in {'forward'}:
    #     manifest[column] = manifest[column].apply(munge_fn_closure)

    # # Experiment for reading artifact
    # try:
    #     artifact = sdk.Artifact.import_data(
    #         'SampleData[SequencesWithQuality]',
    #         output_dir,
    #         view_type='CasavaOneEightSingleLanePerSampleDirFmt')
    #     artifact.save(output_dir)
    # except Exception:
    #     print('Exception occurred')

    return results
