# q2-fondue
![CI](https://github.com/bokulich-lab/q2-fondue/actions/workflows/ci-dev.yaml/badge.svg)
[![codecov](https://codecov.io/gh/bokulich-lab/q2-fondue/branch/main/graph/badge.svg?token=UTM4W4B1KW)](https://codecov.io/gh/bokulich-lab/q2-fondue)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.6388476.svg)](https://doi.org/10.5281/zenodo.6388476)
[![DOI](http://img.shields.io/badge/DOI-10.1093/bioinformatics/btac639-B31B1B.svg)](https://doi.org/10.1093/bioinformatics/btac639)

<p>
<img src="logo.png" height="150" align="left"/>
<span>
<b>q2-fondue</b> is a <a href="https://qiime2.org/">QIIME 2</a> plugin for programmatic access to sequences and metadata from NCBI Sequence Read Archive (SRA). It enables user-friendly acquisition, re-use, and management of public nucleotide sequence (meta)data while adhering to open data principles.
</span>
</p><br>

## Installation
There are multiple options to install q2-fondue - each targeted towards different needs. You can install q2-fondue (**v2024.5** or higher):
* as part of the QIIME 2 metagenome distribution if you want to use it in the QIIME 2 metagenome universe (see [option 1](#option-1-install-q2-fondue-with-qiime-2-metagenome-distribution)).
* within a QIIME 2 amplicon environment if you want to use it in the QIIME 2 amplicon universe (see [option 2](#option-2-install-q2-fondue-within-a-qiime-2-amplicon-conda-environment)).
* in a conda environment of its own if you want to use it independent of other QIIME 2 tools (see [option 3](#option-3-minimal-fondue-environment)).

To install q2-fondue with a version <= 2023.7 see [this](#installing-q2-fondue-with-older-versions) section.

### Option 1: Install q2-fondue with QIIME 2 metagenome distribution
* Install q2-fondue together with the QIIME 2 metagenome distribution as outlined [here](https://docs.qiime2.org/2024.5/install/native/#qiime-2-metagenome-distribution).
* Now, don't forget to run **[the mandatory configuration step](#mandatory-configuration-for-all-three-options)**!

### Option 2: Install q2-fondue within a QIIME 2 amplicon conda environment
* Install the QIIME 2 amplicon distribution within a conda environment as described in [the official user documentation](https://docs.qiime2.org/2024.5/install/native/#qiime-2-amplicon-distribution). 
* Activate the QIIME 2 environment (v2024.5 or higher) and install q2-fondue within while making sure that the used conda channel matches the version of the QIIME 2 environment (replace below `{ENV_VERSION}` with the version number of your QIIME 2 environment):
```
conda activate qiime2-amplicon-{ENV_VERSION}
mamba install -y \
   -c https://packages.qiime2.org/qiime2/{ENV_VERSION}/metagenome/released/ \
   -c conda-forge -c bioconda -c defaults \
   q2-fondue
```
* Now, don't forget to run **[the mandatory configuration step](#mandatory-configuration-for-all-three-options)**!

### Option 3: Minimal fondue environment
* Start with installing [mamba](https://mamba.readthedocs.io/en/latest/index.html) in your base environment:
```shell
conda install mamba -n base -c conda-forge
```
* Create and activate a conda environment with the required dependencies:
```shell
mamba create -y -n fondue \
   -c https://packages.qiime2.org/qiime2/2024.5/metagenome/released/ \
   -c conda-forge -c bioconda -c defaults \
   q2cli q2-fondue

conda activate fondue
```
* Now, don't forget to run **[the mandatory configuration step](#mandatory-configuration-for-all-three-options)**!               

Note: You can replace the version number `2024.5` with later releases if they are already available. 

### Mandatory configuration for all three options
* Refresh the QIIME 2 CLI cache and see that everything worked:
```shell
qiime dev refresh-cache
qiime fondue --help
```
* Run the `vdb-config` tool to make sure the wrapped SRA Toolkit is configured on your system.
The command below will open the configuration interface - everything should be already configured, so you 
can directly exit by pressing **x** (this step is still required to ensure everything is working as expected). 
Feel free to adjust the configuration, if you need to change e.g. the cache location. 
For more information see [here](https://github.com/ncbi/sra-tools/wiki/05.-Toolkit-Configuration).
```shell
vdb-config -i
```
* In case you need to configure a proxy server, run the following command 
(this can also be done using the graphical interface described above):
```shell
vdb-config --proxy <your proxy URL> --proxy-disable no
```

### Installing q2-fondue with older versions
To install fondue with a version <= 2023.7 in a minimal environment run the following command inserting the respective version number `{ENV_VERSION}`:
```shell
mamba create -y -n fondue \
   -c https://packages.qiime2.org/qiime2/{ENV_VERSION}/tested/ \
   -c conda-forge -c bioconda -c defaults \
   q2cli q2-fondue

conda activate fondue
```

Alternatively, a minimal Docker image is available to run q2-fondue==v2023.7: 
* Install [Docker](https://docs.docker.com/engine/install/) with the linked instructions
* Pull the [q2-fondue Docker image](https://hub.docker.com/layers/linathekim/q2-fondue/2023.7/images/sha256-f5d26959ac035811a8f34e2a46f6cc381f9a4ce21b3604a196c1ee176ba708e7?context=repo):
```shell
docker pull linathekim/q2-fondue:2023.7
```
* Within the container, refresh the QIIME 2 CLI cache to see that everything worked:
```shell
qiime dev refresh-cache
qiime fondue --help
```
* If you need to configure a proxy server, run the following command:
```shell
vdb-config --proxy <your proxy URL> --proxy-disable no
```

## Space requirements
Running q2-fondue requires space in the temporary (`TMPDIR`) and output directory. The space requirements for the output directory can be estimated by inserting the run or project IDs in the [SRA Run Selector](https://www.ncbi.nlm.nih.gov/Traces/study/). To estimate the space requirements for the temporary directory, multiply the output directory space requirement by a factor of 10. The current implementation of q2-fondue requires you to have a minimum of 2 GB of available space in your temporary directory.

To find out which temporary directory is used by Qiime 2, you can run `echo $TMPDIR` in your terminal. If this command returns an empty string, the assigned temporary directory equals the OS's default temporary directory (usually `/tmp`) . To re-assign your temporary directory to a location of choice, run `export TMPDIR=Location/of/choice`.


## Usage
### Available actions
q2-fondue provides a couple of actions to fetch and manipulate nucleotide sequencing data and related metadata from SRA as well as an action to scrape run, study, BioProject, experiment and sample IDs from a Zotero web library. Below you will find a list of available actions and their short descriptions.

| Action               | Description                                                       |
|----------------------|-------------------------------------------------------------------|
| `get-sequences`      | Fetch sequences by IDs[*] from the SRA repository.                |
| `get-metadata`       | Fetch metadata by IDs[*] from the SRA repository.                 |
| `get-all`            | Fetch sequences and metadata by IDs[*] from the SRA repo.         |
| `get-ids-from-query` | Find SRA run accession IDs based on a search query.               |
| `merge-metadata`     | Merge several metadata files into a single metadata object.       |
| `combine-seqs`       | Combine sequences from multiple artifacts into a single artifact. |
| `scrape-collection`  | Scrape Zotero collection for IDs[*] and associated DOI names.     |

[*]: Supported IDs include run, study, BioProject, experiment and study IDs.

The next sections give a brief introduction to the most important actions in q2-fondue. More detailed instructions, background information and examples can be found in the associated [tutorial](https://github.com/bokulich-lab/q2-fondue/blob/main/tutorial/tutorial.md).

### Import accession IDs
All _q2-fondue_ actions which fetch data from SRA require the list of run, study, BioProject, experiment or study IDs to be provided as a QIIME 2 artifact of `NCBIAccessionIDs` semantic type. You can either import an existing list of IDs (1.) or scrape a Zotero web library collection to obtain these IDs (2.).

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

2) To scrape all run, study, BioProject, experiment and sample IDs from an existing web Zotero library collection into a `NCBIAccessionIDs` artifact run, you can use the `scrape-collection` method. Before running it, you have to set three environment variables linked to your Zotero account:
* `ZOTERO_TYPE` is the Zotero library type 'user' or 'group'.
* `ZOTERO_USERID` is a valid Zotero user ID. If `ZOTERO_TYPE` is 'user' it can be retrieved from section 'your user_id for use in API calls' in https://www.zotero.org/settings/keys. If `ZOTERO_TYPE` is 'group' it can be obtained by hovering over group name in https://www.zotero.org/groups/.
* `ZOTERO_APIKEY` is a valid Zotero API user key created at https://www.zotero.org/settings/keys/new (checking "Allow library access" and for 'group' library "Read/Write" permissions).

To set these environment variables run the following commands in your terminal for each of the three required variables: `export ZOTERO_TYPE=<your library type>` or create a `.env` file with the environment variable assignment. For the latter option, make sure to ignore this file in version control (add to `.gitignore`). 

__Note:__ To retrieve all required entries from Zotero, you must be logged in. Also, to allow for the `scrape-collection` action to work, make sure you enable file syncing on your Zotero account (see section "File Syncing" [here](https://www.zotero.org/support/sync)) and only attempt to use the action once all attachments were synchronized with your Web Library.

```shell
qiime fondue scrape-collection \
              --p-collection-name collection_name \
              --o-run-ids run_ids.qza \
              --o-study-ids study_ids.qza \
              --o-bioproject-ids bioproject_ids.qza \
              --o-experiment-ids experiment_ids.qza \
              --o-sample-ids sample_ids.qza --verbose
```
where:
- `--p-collection-name` is the name of the collection to be scraped.
- `--o-run-ids` is the output artifact containing the scraped run IDs.
- `--o-study-ids` is the output artifact containing the scraped study IDs.
- `--o-bioproject-ids` is the output artifact containing the scraped BioProject IDs.
- `--o-experiment-ids` is the output artifact containing the scraped experiment IDs.
- `--o-sample-ids` is the output artifact containing the scraped sample IDs.

3) To retrieve run accession IDs based on a text search query (performed on the BioSample database) you can use the `get-ids-from-query` method:
```shell
qiime fondue get-ids-from-query \
              --p-query "txid410656[Organism] AND \"public\"[Filter] AND (chicken OR poultry)" \
              --p-email your_email@somewhere.com \
              --p-n-jobs 2 \
              --o-ids run_ids.qza \
              --verbose
```
where:
- `--p-query` is the text search query to be executed on the BioSample database.
- `--p-email` is your email address (required by NCBI).
- `--p-n-jobs` is the number of parallel download jobs (defaults to 1).
- `--o-ids` is the output artifact containing the retrieved run IDs.

### Fetching metadata
To fetch metadata associated with a set of output IDs, execute the following command:

```shell
qiime fondue get-metadata \
              --i-accession-ids ids.qza \
              --p-n-jobs 1 \
              --p-email your_email@somewhere.com \
              --o-metadata output_metadata.qza \
              --o-failed-runs failed_IDs.qza
```

where:
- `--i-accession-ids` is an artifact containing run, study, BioProject, experiment or sample IDs
- `--p-n-jobs` is a number of parallel download jobs (defaults to 1)
- `--p-email` is your email address (required by NCBI)
- `--o-metadata` is the output metadata artifact
- `--o-failed-runs` is the list of all run IDs for which fetching metadata failed, with their corresponding error messages

The resulting artifact `--o-metadata ` will contain a TSV file with all the available metadata fields for all of the requested runs. If metadata for some run IDs failed to download they are returned in the `--o-failed-runs` artifact, which can be directly inputted as `--i-accession-ids` to a subsequent `get-metadata` command. To pass associated DOI names for the failed runs, provide the table of accession IDs with associated DOI names as `--o-linked-doi` to the `get-metadata` command.

### Fetching sequences
To get openly accessible single- and paired-end sequences associated with a number of IDs, execute this command:
```shell
qiime fondue get-sequences \
              --i-accession-ids ids.qza \
              --p-email your_email@somewhere.com \
              --o-single-reads output_dir_single \
              --o-paired-reads output_dir_paired \
              --o-failed-runs output_failed_ids
```

where:
- `--i-accession-ids` is an artifact containing run, study, BioProject, experiment or sample IDs
- `--p-email` is your email address (required by NCBI)
- `--o-single-reads` is the output artifact containing single-read sequences
- `--o-paired-reads` is the output artifact containing paired-end sequences
- `--o-failed-runs` is the output artifact containing run IDs that failed to download

The resulting sequence artifacts (`--o-single-reads` and `--o-paired-reads`) will contain the `fastq.gz` files of the sequences, `metadata.yml` and `MANIFEST` files.
If one of the provided IDs only contains sequences of one type (e.g. single-read sequences) then the other artifact
(e.g. artifact with paired-end sequences) contains empty sequence files with dummy ID starting with `xxx_`. Similarly,
if none of the requested sequences failed to download, the corresponding artifact will be empty.

If some run IDs failed to download they are returned in the `--o-failed-runs` artifact, which can be directly inputted as `--i-accession-ids` to a subsequent `get-sequence` command.

#### Special case: Fetching restricted access sequences with a dbGAP repository key
To get access to the respective dbGaP repository key users must first apply for approval and then retrieve the key from dbGAP (see prerequisites described [here](https://www.ncbi.nlm.nih.gov/sra/docs/sra-dbgap-download/)).    

To retrieve sequencing data using the acquired dbGAP repository key, without revealing the sensitive key, set the filepath to the stored key as an environment variable. You can either do this by running the following command in your terminal `export KEY_FILEPATH=<path to key>` or by adding the variable assignment to your `.env` file. For the latter option, make sure to ignore this file in version control (add to `.gitignore`).        
Having set the filepath of the key as an environment variable you can fetch the sequencing data by running `get-sequences` with the parameter `--p-restricted-access`:
```shell
qiime fondue get-sequences \
              --i-accession-ids ids.qza \
              --p-email your_email@somewhere.com \
              --p-restricted-access \
              --output-dir output_path
```
__Note:__ Fetching metadata with a dbGAP repository key is not supported. Hence, this flag is only available in the `get-sequences` action (and not in the `get-metadata` and `get-all` actions).

### Fetching metadata and sequences
To fetch both sequence-associated metadata and sequences associated with the provided IDs, execute this command:

```shell
qiime fondue get-all \
              --i-accession-ids ids.qza \
              --p-email your_email@somewhere.com \
              --output-dir output-dir-name
```
where:
- `--i-accession-ids` is an artifact containing run, study, BioProject, experiment or sample IDs
- `--p-email` is your email address (required by NCBI)
- `--output-dir` directory where the downloaded metadata, sequences and IDs for failed downloads are stored as QIIME 2 artifacts

## Downstream analysis in QIIME 2
For more information on how to use q2-fondue outputs within the QIIME 2 ecosystem see section [Downstream analysis in QIIME 2](./tutorial/tutorial.md#downstream-analysis-in-qiime-2) in the tutorial.     

## Exporting data for downstream analyses outside of QIIME 2
Some downstream analyses may need to rely on tools outside of QIIME 2. Since q2-fondue outputs can be transformed directly into FASTQ and other interoperable formats, there are no restrictions for users when using these tools. Note that the exported files will no longer contain integrated provenance information (which is unique to QIIME 2 Artifacts), but this metadata can be exported also and the original artifacts will retain the provenance data for traceability purposes.

To learn more on how to prepare q2-fondue outputs for further analysis outside of QIIME 2 see tutorial section [Prepare downstream analysis outside of QIIME 2](./tutorial/tutorial.md#prepare-downstream-analysis-outside-of-qiime-2). 

## Getting Help
Problem? Suggestion? Technical errors and user support requests can be filed on the [QIIME 2 Forum](https://forum.qiime2.org/).

## Citation

If you use `fondue` in your research, please cite the following:

Michal Ziemski, Anja Adamov, Lina Kim, Lena Flörl, Nicholas A. Bokulich. 2022. Reproducible acquisition, management, and meta-analysis of nucleotide sequence (meta)data using q2-fondue.
_Bioinformatics; doi: https://doi.org/10.1093/bioinformatics/btac639


## License
q2-fondue is released under a BSD-3-Clause license. See LICENSE for more details.
