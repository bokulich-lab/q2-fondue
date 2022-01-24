# q2-fondue 
![CI](https://github.com/bokulich-lab/q2-fondue/actions/workflows/ci.yml/badge.svg)

## Installation
Before q2-fondue is available *via* conda, you can use the following instructions to install it on your machine by creating a new conda environment:

* Create and activate a conda environment with the required dependencies:
```shell
conda create -y -n fondue \
   -c qiime2 -c conda-forge -c bioconda -c defaults \
  qiime2 q2cli q2-types "entrezpy>=2.1.2" "sra-tools>=2.11.0" \
  "tqdm>=4.62.3" xmltodict

conda activate fondue
```
* Install q2-fondue and refresh the QIIME 2 CLI cache. 
```shell
pip install git+https://github.com/bokulich-lab/q2-fondue.git

qiime dev refresh-cache
```

The current q2-fondue version supports QIIME 2 **v2021.4** or higher.


## Space requirements
Running q2-fondue requires space in the temporary (`TMPDIR`) and output directory. The space requirements for the output directory can be estimated by inserting the run or project IDs in the [SRA Run Selector](https://www.ncbi.nlm.nih.gov/Traces/study/). To estimate the space requirements for the temporary directory, multiply the output directory space requirement by a factor of 10. The current implementation of q2-fondue requires you to have a minimum of 2 GB of available space in your temporary directory.

To find out which temporary directory is used by Qiime 2, you can run `echo $TMPDIR` in your terminal. If this command returns an empty string, the assigned temporary directory equals the OS's default temporary directory (usually `/tmp`) . To re-assign your temporary directory to a location of choice, run `export TMPDIR=Location/of/choice`. 


## Usage
### Available actions
q2-fondue provides a couple of actions to fetch and manipulate SRA data. Below you will find a list of available actions and their short descriptions.

| Action           | Description                                                              |
|------------------|--------------------------------------------------------------------------|
| `get-sequences`  | Fetch sequences by run or BioProject IDs from the SRA repository.        |
| `get-metadata`   | Fetch metadata by run or BioProject IDs from the SRA repository.         |
| `get-all`        | Fetch sequences and metadata by run or BioProject IDs from the SRA repo. |
| `merge-metadata` | Merge several metadata files into a single metadata object.              |
| `combine-seqs`   | Combine sequences from multiple artifacts into a single artifact.        |


### Fetching metadata
To fetch metadata associated with a number of run or project IDs, execute the following command:

```shell
qiime fondue get-metadata \
              --m-accession-ids-file metadata_file.tsv \
              --p-n-jobs 1 \
              --p-email your_email@somewhere.com \
              --o-metadata output_metadata.qza
```

where:
- `--m-accession-ids-file` is a TSV containing run or project IDs
- `--p-n-jobs` is a number of parallel download jobs (defaults to 1)
- `--p-email` is your email address (required by NCBI)
- `--o-metadata` is the output metadata artifact

The resulting artifact will contain a TSV file with all the available metadata fields
for all of the requested runs.

### Fetching sequences
To get single-read and paired-end sequences associated with a number of run or project IDs, execute this command:
```shell
qiime fondue get-sequences \
              --m-accession-ids-file metadata_file.tsv \
              --p-email your_email@somewhere.com \
              --o-single-reads output_dir_single \
              --o-paired-reads output_dir_paired \
              --o-failed-runs output_failed_ids
```

where:
- `--m-accession-ids-file` is a TSV containing run or project IDs
- `--p-email` is your email address (required by NCBI)
- `--o-single-reads` is the output artifact containing single-read sequences
- `--o-paired-reads` is the output artifact containing paired-end sequences
- `--o-failed-runs` is the output artifact containing run IDs that failed to download

The resulting sequence artifacts (`--o-single-reads` and `--o-paired-reads`) will contain the `fastq.gz` files of the sequences, `metadata.yml` and `MANIFEST` files. 
If one of the provided IDs only contains sequences of one type (e.g. single-read sequences) then the other artifact 
(e.g. artifact with paired-end sequences) contains empty sequence files with dummy ID starting with `xxx_`. Similarly, 
if none of the requested sequences failed to download, the corresponding artifact will be empty.

If some run IDs failed to download they are returned in the `--o-failed-runs` artifact, which can be directly inputted as an `--m-accession-ids-file` to a subsequent `get-sequence` command. 

### Fetching metadata and sequences
To fetch both sequence-associated metadata and sequences associated with the provided run or project IDs, execute this command:

```shell
qiime fondue get-all \
              --m-accession-ids-file metadata_file.tsv \ 
              --p-email your_email@somewhere.com \
              --output-dir output-dir-name
```
where:
- `--m-accession-ids-file` is a TSV containing accession numbers for all the runs
- `--p-email` is your email address (required by NCBI)
- `--output-dir` directory where the downloaded metadata, sequences and IDs for failed downloads are stored as QIIME 2 artifacts

## License
q2-fondue is released under a BSD-3-Clause license. See LICENSE for more details.
