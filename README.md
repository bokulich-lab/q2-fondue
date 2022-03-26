# q2-fondue
![CI](https://github.com/bokulich-lab/q2-fondue/actions/workflows/ci.yml/badge.svg)

 <p align="left"><img src="logo.png" height="150" /></p>

`q2-fondue` is a [QIIME 2](https://qiime2.org/) plugin for programmatic access to sequences and metadata from NCBI Sequence Read Archive (SRA). It enables user-friendly acquisition, re-use, and management of public nucleotide sequence (meta)data while adhering to open data principles. 

## Installation
Before q2-fondue is available *via* conda, you can use the following instructions to install it on your machine by creating a new conda environment. The current q2-fondue version supports QIIME 2 **v2021.4** or higher.

* Create and activate a conda environment with the required dependencies:
```shell
conda create -y -n fondue \
   -c qiime2 -c conda-forge -c bioconda -c defaults \
  qiime2 q2cli q2-types "entrezpy>=2.1.2" "tqdm>=4.62.3" xmltodict pyzotero

conda activate fondue
```
* Install sra-tools using the script provided in this repo.
```shell
curl -sLH 'Accept: application/vnd.github.v3.raw' https://api.github.com/repos/bokulich-lab/q2-fondue/contents/install-sra-tools.sh > install-sra-tools.sh

chmod +x install-sra-tools.sh
bash install-sra-tools.sh

rm install-sra-tools.sh
```
* Install q2-fondue and refresh the QIIME 2 CLI cache.
```shell
pip install git+https://github.com/bokulich-lab/q2-fondue.git

qiime dev refresh-cache
```

* Configuration of the wrapped SRA Toolkit should be automatically performed by the installation script executed above. In case you need to configure a proxy server run:
```shell
vdb-config --proxy <your proxy URL> --proxy-disable no
```

## Space requirements
Running q2-fondue requires space in the temporary (`TMPDIR`) and output directory. The space requirements for the output directory can be estimated by inserting the run or project IDs in the [SRA Run Selector](https://www.ncbi.nlm.nih.gov/Traces/study/). To estimate the space requirements for the temporary directory, multiply the output directory space requirement by a factor of 10. The current implementation of q2-fondue requires you to have a minimum of 2 GB of available space in your temporary directory.

To find out which temporary directory is used by Qiime 2, you can run `echo $TMPDIR` in your terminal. If this command returns an empty string, the assigned temporary directory equals the OS's default temporary directory (usually `/tmp`) . To re-assign your temporary directory to a location of choice, run `export TMPDIR=Location/of/choice`.


## Usage
### Available actions
q2-fondue provides a couple of actions to fetch and manipulate nucleotide sequencing data and related metadata from SRA as well as an action to scrape run or BioProject IDs from a Zotero web library. Below you will find a list of available actions and their short descriptions.

| Action           | Description                                                              |
|------------------|--------------------------------------------------------------------------|
| `get-sequences`  | Fetch sequences by run or BioProject IDs from the SRA repository.        |
| `get-metadata`   | Fetch metadata by run or BioProject IDs from the SRA repository.         |
| `get-all`        | Fetch sequences and metadata by run or BioProject IDs from the SRA repo. |
| `merge-metadata` | Merge several metadata files into a single metadata object.              |
| `combine-seqs`   | Combine sequences from multiple artifacts into a single artifact.        |
| `scrape-collection`| Scrape Zotero collection for run and BioProject IDs.                   |


### Import run/BioProject accession IDs
All _q2-fondue_ actions which fetch data from SRA require the list of run or BioProject IDs to
be provided as a QIIME 2 artifact of `NCBIAccessionIDs` semantic type. You can either import an existing
list of IDs (1.) or scrape a Zotero web library collection to obtain these IDs (2.).

1) To import an existing list of IDs into a `NCBIAccessionIDs` artifact simply run:

```shell
qiime tools import \
              --type NCBIAccessionIDs \
              --input-path ids.tsv \
              --output-path ids.qza
```

where:
- `--input-path` is a path to the TSV file containing run or project IDs.
- `--output-path` is the output artifact.

__Note:__ the input TSV file needs to consist of a single column named "ID".

2) To scrape all run and BioProject IDs from an existing web Zotero library collection into a `NCBIAccessionIDs`
artifact run:
```shell
qiime fondue scrape-collection \
              --p-library-type user \
              --p-user-id user_id \
              --p-api-key my_key \
              --p-collection-name collection_name \
              --o-run-ids run_ids.qza \
              --o-bioproject-ids bioproject_ids.qza
```
where:
- `--p-library-type` is the Zotero API library type 'user' or 'group'.
- `--p-user-id` is a valid Zotero user ID. If `--p-library-type` is 'user' it can be retrieved from section 'your user_id for use in API calls' in https://www.zotero.org/settings/keys. If `--p-library-type` is 'group' it can be obtained by hovering over group name in https://www.zotero.org/groups/.
- `--p-api-key` is a valid Zotero API user key created at https://www.zotero.org/settings/keys/new (checking "Allow library access" and for 'group' library "Read/Write" permissions).
- `--p-collection-name` is the name of the collection to be scraped.
- `--o-run-ids` is the output artifact containing the scraped run IDs.
- `--o-bioproject-ids` is the output artifact containing the scraped BioProject IDs.

__Note:__ To retrieve all required IDs from Zotero, you must be logged in.

### Fetching metadata
To fetch metadata associated with a number of run or project IDs, execute the following command:

```shell
qiime fondue get-metadata \
              --i-accession-ids ids.qza \
              --p-n-jobs 1 \
              --p-email your_email@somewhere.com \
              --o-metadata output_metadata.qza \
              --o-failed-runs failed_IDs.qza
```

where:
- `--i-accession-ids` is an artifact containing run or project IDs
- `--p-n-jobs` is a number of parallel download jobs (defaults to 1)
- `--p-email` is your email address (required by NCBI)
- `--o-metadata` is the output metadata artifact
- `--o-failed-runs` is the list of all run IDs for which fetching metadata failed, with their corresponding error messages

The resulting artifact will contain a TSV file with all the available metadata fields
for all of the requested runs.

### Fetching sequences
To get single-read and paired-end sequences associated with a number of run or project IDs, execute this command:
```shell
qiime fondue get-sequences \
              --i-accession-ids ids.qza \
              --p-email your_email@somewhere.com \
              --o-single-reads output_dir_single \
              --o-paired-reads output_dir_paired \
              --o-failed-runs output_failed_ids
```

where:
- `--i-accession-ids` is an artifact containing run or project IDs
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
              --i-accession-ids ids.qza \
              --p-email your_email@somewhere.com \
              --output-dir output-dir-name
```
where:
- `--i-accession-ids` is an artifact containing run or project IDs
- `--p-email` is your email address (required by NCBI)
- `--output-dir` directory where the downloaded metadata, sequences and IDs for failed downloads are stored as QIIME 2 artifacts

## Citation

If you use `fondue` in your research, please cite the following:

Michal Ziemski, Anja Adamov, Lina Kim, Lena Flörl, Nicholas A. Bokulich. 2022. Reproducible acquisition, management, and meta-analysis of nucleotide sequence (meta)data using q2-fondue.
bioRxiv 2022.03.22.485322; doi: https://doi.org/10.1101/2022.03.22.485322


## License
q2-fondue is released under a BSD-3-Clause license. See LICENSE for more details.
