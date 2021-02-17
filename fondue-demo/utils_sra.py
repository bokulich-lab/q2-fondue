from subprocess import call


def get_seqs(ls_acc, general_retries=2, output_dir="output_seqs", threads=6):
    """
    Function to run SRA toolkit fasterq-dump to get sequences of accessions in `ls_accs`.
    Supports mulitple tries (nb_tries) and can use mulitple threads.
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
                print('Sequence {} successfully downloaded in folder {}'.format(
                    acc, output_dir))
                retries = -1
            elif cmd_output == 3:
                print('Sequence {} was already downloaded in folder {}'.format(
                    acc, output_dir))
                retries = -1
            else:
                print(
                    "Sequence {} download failed - will retry {} more times.".format(
                        acc, retries))
                retries -= 1
