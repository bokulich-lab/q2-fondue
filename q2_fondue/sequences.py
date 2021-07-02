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
import tempfile
from subprocess import run
from q2_types.per_sample_sequences import \
    (CasavaOneEightSingleLanePerSampleDirFmt)

# todo move below functions starting with '_' to util.py
# todo rename study-ids to sample-ids


def _run_cmd_fasterq(acc: str, output_dir: str, threads: int,
                     general_retries: int):
    """
    Helper function running fasterq-dump `general_retries` times
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
    """
    Helper function that removes paired read sequences
    (not supported yet) and renames single read sequences
    according to casava file format
    """

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
        new_name = '%s_00_L001_R%d_001.fastq.gz' % (acc, 1)
        # todo: adjust to R2 if double-reads

        os.rename(os.path.join(output_dir, filename),
                  os.path.join(output_dir, new_name))


def _read_fastq_seqs(filepath):
    # function copied from q2_demux._demux import _read_fastq_seqs

    # Originally func is adapted from @jairideout's SO post:
    # http://stackoverflow.com/a/39302117/3424666
    fh = gzip.open(filepath, 'rt')
    for seq_header, seq, qual_header, qual in itertools.zip_longest(*[fh] * 4):
        yield (seq_header.strip(), seq.strip(), qual_header.strip(),
               qual.strip())


def _write2casava_dir(tmpdirname, casava_result_path):
    """
    Helper function that writes downloaded sequence files
    from tmpdirname to casava_result_path
    """
    for filename in os.listdir(tmpdirname):
        fwd_path_in = os.path.join(tmpdirname, filename)
        fwd_path_out = str(casava_result_path.path / filename)

        with gzip.open(str(fwd_path_out), mode='w') as fwd:
            for fwd_rec in _read_fastq_seqs(fwd_path_in):
                fwd.write(('\n'.join(fwd_rec) + '\n').encode('utf-8'))


def get_sequences(study_ids: list,
                  general_retries: int = 2,
                  threads:
                  int = 6) -> (
        CasavaOneEightSingleLanePerSampleDirFmt):
    """
    Function to run SRA toolkit fasterq-dump to get sequences of accessions
    in `study_ids`. Supports mulitple tries (`general_retries`) and can use
    multiple `threads`.
    """
    casava_out = CasavaOneEightSingleLanePerSampleDirFmt()

    with tempfile.TemporaryDirectory() as tmpdirname:
        # run fasterq-dump for all accessions
        for acc in study_ids:
            result = _run_cmd_fasterq(acc, tmpdirname, threads,
                                      general_retries)

            if len(os.listdir(tmpdirname)) == 0:
                # raise error if all general_retries attempts failed
                raise ValueError('{} could not be downloaded with the '
                                 'following fasterq-dump error '
                                 'returned: {}'
                                 .format(acc, result.stderr))
            else:
                continue

        # processing downloaded files
        _process_downloaded_sequences(tmpdirname)

        # write downloaded seqs from tmp to casava dir
        _write2casava_dir(tmpdirname, casava_out)

    return casava_out
