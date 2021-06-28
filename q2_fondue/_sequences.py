# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from subprocess import call


def get_seqs(ls_acc, general_retries=2, output_dir="output_seqs", threads=6):
    """
    Function to run SRA toolkit fasterq-dump to get sequences of accessions
    in `ls_accs`. Supports mulitple tries (nb_tries) and can use multiple
    threads.
    """

    for acc in ls_acc:
        print("Downloading sequences of study: {}...".format(acc))
        retries = general_retries

        while retries >= 0:
            # run fasterq-dump command
            cmd_fasterq = ["fasterq-dump", "-O",
                           output_dir, "-e", str(threads), acc]
            cmd_output = call(cmd_fasterq)

            # retry if needed
            if cmd_output == 0:
                print(f'Sequence {acc} successfully downloaded in '
                      f'folder {output_dir}')
                retries = -1
            elif cmd_output == 3:
                print(f'Sequence {acc} was already downloaded in '
                      f'folder {output_dir}')
                retries = -1
            else:
                print(f'Sequence {acc} download failed - will '
                      f'retry {retries} more times.')
                retries -= 1
