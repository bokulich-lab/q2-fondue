# q2-fondue 
![CI](https://github.com/bokulich-lab/q2-fondue/actions/workflows/ci.yml/badge.svg)

## Installation

Before q2-fondue is available *via* conda, you can use the following instructions to install it on your machine:

```shell
conda create -y -n fondue \
   -c qiime2 -c conda-forge -c bioconda -c defaults \
  qiime2 q2cli q2-types "entrezpy>=2.1.2" "sra-tools==2.9.6" xmltodict
conda activate fondue

pip install git+https://github.com/bokulich-lab/q2-fondue.git

qiime dev refresh-cache
```

The current q2-fondue version supports QIIME 2 **v2021.4** or higher.

#### DEV-only note:
Until QIIME 2 2021.8 is officially released, replace `-c qiime2` in the command above with
`-c https://packages.qiime2.org/qiime2/2021.8/staged` to fetch the latest dev version instead.

## Usage
### Fetching metadata

To fetch metadata associated with a number of runs, execute the following command:

```shell
qiime fondue get-metadata \
              --p-sample-ids <id1> <id2> <id3> ... \
              --p-n-jobs 1 \
              --p-email your_email@somewhere.com \
              --o-metadata output_metadata.qza
```

where:
- `--p-sample-ids` is a list of accession numbers for all of the runs
- `--p-n-jobs` is a number of parallel download jobs (defaults to 1)
- `--p-email` is your email address (required by NCBI)
- `--o-metadata` is the output metadata artifact

The resulting artifact will contain a TSV file containing all the available metadata fields
for all of the requested runs.

### Fetching sequences
To get single-read and paired-end sequences associated with a number of runs, execute this command:
```shell
qiime fondue get-sequences \
              --p-sample-ids <id1> <id2> <id3> ... \
              --o-single-reads output_dir_single \
              --o-double-ends output_dir_double
```

where:
- `--p-sample-ids` is a list of accession numbers for all of the runs
- `--o-single-reads` is the output artifact containing single-read sequences
- `--o-double-ends` is the output artifact containing double-end sequences

The resulting artifact will contain the `fastq.gz` files of the sequences, `metadata.yml` and `MANIFEST` files. If the provided accession numbers only contain sequences of one type (e.g. single-read sequences) then the other artifact (e.g. artifact with paired-end sequences) contains empty sequence files with dummy ID starting with `xxx_`.
