# q2-fondue Tutorial

> q2-fondue (**F**unctions for reproducibly **O**btaining and **N**ormalizing **D**ata re-**U**sed from **E**lsewhere)

A QIIME 2 plugin enabling an easy download of high throughput sequencing data and the corresponding metadata. 

With this easy-to-use plugin you have plenty of deposited sequencing data at your fingertips! Want to set your own data in comparison to other published datasets or start off with a meta-analysis? _q2-fondue_ is here to help! :raised_hands:

Why use _q2-fondue_? 
* incorporated provenance tracking
* direct integration with QIIME 2 sequence analysis pipeline - see [Overview of QIIME 2 Plugin Workflows](https://docs.qiime2.org)
* support for multiple user interfaces
* no need to navigate online databases to retrieve data 

This tutorial will give you an insight into working with q2-fondue and how its output artifacts can further be used. 

## Some Background
There are numerous databases online, where researchers can deposit or retrieve high-throughput sequencing data. 
However, the three biggest and most important storage and sharing platforms certainly are the 
[**E**uropean **N**ucleotide **A**rchive](https://www.ebi.ac.uk/ena/browser/home) 
(ENA, by the European Bioinformatics Institute (EMBL-EBI)), the [**S**equence **R**ead **A**rchive](https://www.ncbi.nlm.nih.gov/sra/) 
(SRA, by the National Institutes of Health (NIH), a part of the U.S. Department of Health and Human Services) and the 
[**D**NA **D**ata **B**ank of **J**apan](https://www.ddbj.nig.ac.jp/index-e.html) 
(DDBJ, at the Research Organization of Information and System National Institute of Genetics(NIG)). 
Through the [International Nucleotide Sequence Database Collaboration](https://www.insdc.org) (INSDC), 
data submitted to any of these three organizations is immediately shared among them, enabling free and 
unrestricted access to deposited data from all three platforms.

Despite the effort to consolidate these colossal amounts of data, it can still be hard to navigate 
through variable nomenclature used by the different platforms and to actually fetch raw data from the diverse user interfaces. 

#### Overview to INSDCs accession number nomenclature:

| Accession Type | Accession Format        | Example    |
|----------------|-------------------------|------------|
| Projects       | PRJ(E\|D\|N)[A-Z][0-9]+ | PRJEB12345 |
| Studies        | (E\|D\|S)RP[0-9]{6,}    | ERP123456  |
| Experiments    | (E\|D\|S)RX[0-9]{6,}    | ERX123456  |
| Runs           | (E\|D\|S)RR[0-9]{6,}    | ERR123456  | 


Since the launch of BioProject IDs in 2011, this accession number is commonly referenced in most publications, allowing access to all raw sequencing data and corresponding metadata of the entire project<sup>1</sup>. The subordinate _Runs_, contain the actual sequencing data of individual samples. BioProject and Run ID are the two accession number types that can be used as input for _q2-fondue_. 

#### Database specific accession numbers:

| Prefix | Name                        | Platform       |
|--------|-----------------------------|----------------|
| PRJNA  | BioProject accession number | SRA (NCBI)     |
| PRJEB  | EBI Project accession       | ENA (EMBL-EBI) |
| PRJD   | DDBJ BioProject             | DDBJ           |
| SRR    | SRA run accession           | SRA (NCBI)     |
| ERR    | ERA run accession           | ENA (EMBL-EBI) |
| DRR    | DRA run accession           | DDBJ           |


Some microbiome datasets are also uploaded on [Qiita](https://qiita.ucsd.edu), an open-source microbial study management platform. While all data deposited on Qiita is automatically deposited into ENA-EBI, one can also use the QIIME 2 plugin [redbiome](https://forum.qiime2.org/t/querying-for-public-microbiome-data-in-qiita-using-redbiom/4653) to query and obtain data and metadata from Qiita. 

## Using q2-fondue 
After reading about regionally distinct microbial communities in vineyards in the publication by Bokulich et al. (2016)<sup>2</sup>, we are super curious to explore the dataset this study was based on. Luckily, with _q2-fondue_ retrieving all this data is a cakewalk! :cake:

## Installation
_q2-fondue_ will be installable as a conda package in the near future. 
For now, we install it with pip into an existing QIIME 2 environment. 

> _Note:_ the current q2-fondue version supports QIIME 2 **v2021.4** or higher - get the latest QIIME 2 release in the [Installation guide](docs.qiime2.org). 

First, activate your QIIME 2 environment and install relevant dependencies:
```shell
conda activate qiime2

conda install -c conda-forge -c bioconda -c defaults \
 "entrezpy>=2.1.2" "tqdm>=4.62.3" xmltodict pyzotero
```

Then, we are installing and configuring the wrapped SRA Toolkit with the script provided in this repo:
```shell
curl -sLH 'Accept: application/vnd.github.v3.raw' https://api.github.com/repos/bokulich-lab/q2-fondue/install-sra-tools.sh > install-sra-tools.sh

chmod +x install-sra-tools.sh
bash install-sra-tools.sh

rm install-sra-tools.sh
```

Finally, install _q2-fondue_ and update your QIIME 2 libraries:
```shell
pip install git+https://github.com/bokulich-lab/q2-fondue.git

qiime dev refresh-cache
```

Done! :muscle:
 
## Getting started

First, let's move to the tutorial directory. 

```shell
cd tutorial
```

To run _q2-fondue_ we need a TSV file containing the accession numbers of the desired Runs or BioProjects. 
This metadata file has to contain a header QIIME 2 can recognize and we can for example put *id* as the column name. 
To learn more about other options for identifiers used in QIIME 2 or learn about metadata in general, check out 
the [QIIME 2 metadata documentation](docs.qiime2.org).

Get some example metadata file with accession numbers of Bokulich et al. (2016) here: 
https://github.com/bokulich-lab/q2-fondue/tree/main/tutorial

The *metadata_file.tsv* contains the BioProject accession number (PRJEB14186) and the
*metadata_file_runs.tsv* the selected Run accession numbers (ERR1428207-ERR1428236). 

> *Tip*: one can of course also pass several BioProject accession numbers at once by having them all in the same metadata file!

## Fetching sequences and corresponding metadata together

We first have to convert our *metadata_file_runs.tsv* file to a QIIME2 artifact of semantic type `NCBIAccessionIDs`. 

```shell
qiime tools import \
      --type NCBIAccessionIDs \
      --input-path metadata_file_runs.tsv \
      --output-path metadata_file_runs.qza
```

Then, to download the raw sequencing data and associated metadata  of the entire project we simply pass the *metadata_file_runs.qza* to `qiime fondue get-all` and specify the output directory. 

To not overload their servers, NCBI recommends to avoid posting more than 3 URL requests per second 
and to run requests for very large jobs on the weekend (find more info on this in their 
[Usage Guidelines and Requirements](https://www.ncbi.nlm.nih.gov/books/NBK25497/)). 
Therefore NCBI requires a **valid email address**, enabling them to get in touch in case of an issue 
with downloading too much data. 

> *Tip:* We recommend always adding the `--verbose` flag when running _q2-fondue_. Depending on the amount of data we are retrieving, the download might take some time and it is easier to follow the process with the integrated download progress bar. Additionally, running with `--verbose` facilitates catching potential issues with internet connectivity. 

```shell
qiime fondue get-all \
      --i-accession-ids metadata_file_runs.qza \
      --p-email your_email@somewhere.com \
      --p-retries 3 \
      --verbose \
      --output-dir fondue-output
```

> *Note*: The optional parameter `--p-retries` specifies the number of times _q2-fondue_ is retrying to fetch the sequencing data and is set to 2 by default. If you notice that some of the desired data is not properly downloaded you might increase this number or try refetching manually (see [Troubleshooting](#manually-refetching-missing-sequencing-data)). 

Now let's have a look at the output files! 

In the `fondue-output` directory we can find four files:
* *metadata.qza* of semantic type `SRAMetadata`, containing the metadata
* *paired_reads.qza* of semantic type `SampleData[PairedEndSequencesWithQuality]` 
* *single_reads.qza* of semantic type `SampleData[SequencesWithQuality]`
* *failed_runs.qza* of semantic type `SRAFailedIDs`, containing the IDs of samples that failed to download (see [Troubleshooting](#manually-refetching-missing-sequencing-data))

It is important to know that _q2-fondue_ always generates **two** files of semantic type `SampleData`, one for paired end and one for single end reads, 
however most of the time only one of them contains the sequencing data we want (unless we are fetching sequencing data from various BioProjects at the same time).

How can we now find out which raw sequence file we should be using? These are your options:

⇨ read the methods section of the original publication to see whether they used paired or single end sequencing.

⇨ check out the metadata file (how to unpack this is shown below!) - the column *Library Layout* specifies SINGLE or PAIRED end sequencing.

⇨ in the `fondue-output` directory type `ls -lah` to show the file size in kilo- (K) or megabyte (M), one of the files will contain only a few kilobyte while the other has several MB of juicy raw data! 

⇨ when running `qiime fondue get-all`, add the `--verbose` flag to automatically get the `UserWarning: No paired-read sequences available for these sample IDs`.

In this case we will therefore [continue](#what-now) with the *single_reads.qza*! :fire:

## Other q2-fondue functionalities
### Fetching **only** metadata
We might just want to gain more insight into the metadata of a specific study. 
Also for this action we can start with TSV a file with accession number of BioProject or 
individual Runs. 

```shell
qiime tools import \
      --type NCBIAccessionIDs \
      --input-path metadata_file.tsv \
      --output-path metadata_file.qza

qiime fondue get-metadata \
      --i-accession-ids metadata_file.qza \
      --p-n-jobs 1 \
      --p-email your_email@somewhere.com \
      --o-metadata output_metadata.qza \
      --o-failed-runs failed_metadata.qza \
      --verbose
```
> *Note:* The parameter `--p-n-jobs` is the number of parallel download jobs and the default is 1. Since this specifies the number of threads, there are hardly any CPU limitations and the more is better until you run out of bandwidth. However, this action is fairly quick so feel free to stick to 1.

The output file *output_metadata.qza* now contains all the metadata for the requested IDs. And *failed_metadata.qza* list all IDs where fetching metadata failed, with their corresponding error messages. 


### Fetching **only** sequencing data

In contrast, to only get the raw sequences associated with a number of runs, execute these commands:
```shell
qiime tools import \
      --type NCBIAccessionIDs \
      --input-path metadata_file.tsv \
      --output-path metadata_file.qza

qiime fondue get-sequences \
      --i-accession-ids metadata_file.qza \
      --p-email your_email@somewhere.com \
      --o-single-reads single_reads.qza \
      --o-paired-reads paired_reads.qza \
      --o-failed-runs failed_ids.qza \
      --verbose
```

> *Note:* We can also add the `--p-n-jobs` and `--p-retries`  parameters in this command (see [`get-metadata`](#fetching-only-metadata) and [`get-all`](#fetching-sequences-and-corresponding-metadata-together) for more explanations).  

### Scraping run and BioProject IDs from a Zotero web library collection

For now we have assumed that a file exists with the accession IDs, for which we want to fetch the sequences and corresponding metadata, namely `metadata_file.qza`. If you want to scrape the Run and BioProject IDs from an existing Zotero web library collection, you can run the following command:
```shell
qiime fondue scrape-collection \
              --p-library-type user \
              --p-user-id user_id \
              --p-api-key my_key \
              --p-collection-name collection_name \
              --o-run-ids fondue-output/run_ids.qza \
              --o-bioproject-ids fondue-output/bioproject_ids.qza \
              --verbose
```
where:
- `--p-library-type` is the Zotero API library type 'user' or 'group'.
- `--p-user-id` is a valid Zotero user ID. If `--p-library-type` is 'user' it can be retrieved from section 'your user_id for use in API calls' in https://www.zotero.org/settings/keys. If `--p-library-type` is 'group' it can be obtained by hovering over group name in https://www.zotero.org/groups/.       
- `--p-api-key` is a valid Zotero API user key created at https://www.zotero.org/settings/keys/new (checking "Allow library access" and for 'group' library "Read/Write" permissions).
- `--p-collection-name` is the name of the collection to be scraped. 
- `--o-run-ids` is the output artifact containing the scraped run IDs.
- `--o-bioproject-ids` is the output artifact containing the scraped BioProject IDs.

To investigate which run and BioProject IDs were scraped from your collection, you can check out the respective output artifacts with the commands:
```shell
qiime metadata tabulate \
      --m-input-file fondue-output/run_ids.qza \
      --o-visualization fondue-output/run_ids.qzv

qiime tools view fondue-output/run_ids.qzv
```
> *Note:* make sure to have q2-metadata installed in your conda environment with `conda install -c qiime2 q2-metadata`.

## What now? 

Here we show how the artifacts fetched through q2-fondue enable an easy entry to the QIIME 2 analysis pipeline, which is also further described in [other QIIME 2 tutorials](https://docs.qiime2.org). :sparkles:

### Check out the metadata 
While the metadata files we use in QIIME 2 commonly are in the TSV format, 
the semantic type `SRAMetadata` that q2-fondue is creating can be used in the same way.

Let's have a look at the metadata by tabulating it and visualizing the .qzv file.
```shell
qiime metadata tabulate \
      --m-input-file fondue-output/metadata.qza \
      --o-visualization fondue-output/metadata.qzv

qiime tools view fondue-output/metadata.qzv
```

### Using the sequencing data 
Apart from avoiding the tedious search and manual downloading of these large piles of data, 
one of the biggest advantages of using q2-fondue is the fact that the output is already a QIIME 2 
artifact and we don't have to import it! 

The retrieved single_reads.qza file can therefore be summarized directly: 
```shell
qiime demux summarize \
      --i-data fondue-output/single_reads.qza \
      --o-visualization fondue-output/single_reads.qzv

qiime tools view fondue-output/single_reads.qzv
```
Have a look at the overall quality in the Interactive Quality Plot and inspect the sample and 
feature counts. Then, we can move on to denoising with DADA2 or Deblur. 

For example:
```shell
qiime dada2 denoise-single \
      --i-demultiplexed-seqs fondue-output/single_reads.qza \
      --p-trunc-len 120 \
      --o-table fondue-output/dada2_table.qza \
      --o-representative-sequences fondue-output/dada2_rep_set.qza \
      --o-denoising-stats fondue-output/dada2_stats.qza
```

As mentioned above, the *metadata.qza* file can be used directly in the following analysis! :muscle:

```shell
qiime feature-table summarize \
      --i-table fondue-output/dada2_table.qza \
      --m-sample-metadata-file fondue-output/metadata.qza \
      --o-visualization fondue-output/dada2_table.qzv

qiime tools view fondue-output/dada2_table.qzv
```

In summary, we showed how the artifacts fetched through q2-fondue enable an easy entry 
to the QIIME 2 analysis pipeline, which is further described in other tutorials. 

## Extracting the metadata or sequences artifacts 
All [QIIME 2 artifacts](docs.qiime2.org) fundamentally are zipped files, containing additional 
information on the artifact's provenance, type and format. All this information can be exposed using 
`qiime tools peek`. In case we want to work with the retrieved data outside of QIIME 2, 
it is possible to extract the artifacts.

#### Get a metadata TSV file 
```shell
qiime tools extract \
      --input-path metadata.qza \
      --output-path metadata
```
This creates a metadata directory with all information on provenance tracking, 
and in the folder *data* we find the *sra-metadata.tsv*.

#### Get FASTA files 
```shell
qiime tools extract \
      --input-path fondue-output/single_reads.qza \
      --output-path fondue-output/single_reads
```
Similarly, when extracting the sequencing data, we find the individual *fastq.gz* files of each Run as well as a *metadata.yml* and a *MANIFEST* file in the *data* directory. 

## Troubleshooting 
### Manually refetching missing sequencing data
Occasionally sequencing data is not completely downloaded due to, for example, server timeouts from NCBI. In this case one can simply use the generated *failed_runs.qza* file of semantic type `SRAFailedIDs` containing the IDs that failed to download the missing sequencing data. 

```shell
qiime fondue get-sequences \
      --i-accession-ids fondue-output/failed_ids.qza \
      --p-email your_email@somewhere.com \
      --o-single-reads refetched_single \
      --o-paired-reads refetched_paired \
      --o-failed-runs refetched_failed
```

### Combining sequence data from multiple into a single artifact 
Instead of working with multiple raw sequencing data (semantic type `SampleData[SequencesWithQuality]`), for example after refetching IDs that failed to download at first, we can use this powerful action to merge them into a single artifact. 

```shell
 qiime fondue combine-seqs \
      --i-seqs single_reads.qza refetched_single.qza \
      --o-combined-seqs combined_seqs.qza
```

### Merging metadata files 
With *q2-fondue* we can also easily merge multiple metadata files into a single one! 

```shell
qiime fondue merge-metadata \
      --i-metadata output_metadata_1.qza output_metadata_2.qza \
      --o-merged-metadata merged_metadata.qza
```

## References 

[1] Clark K, Pruitt K, Tatusova T, et al. **BioProject**. 2013 Apr 28 [Updated 2013 Nov 11]. In: The NCBI Handbook [Internet]. 2nd edition. Bethesda (MD): National Center for Biotechnology Information (US); 2013-. Available from: https://www.ncbi.nlm.nih.gov/books/NBK169438/?report=classic

[2] Bokulich N., et al. **Associations among Wine Grape Microbiome, Metabolome, and Fermentation Behavior Suggest Microbial Contribution to Regional Wine Characteristics**. 2016 Jun 14. In: ASM Journals / mBio / Vol. 7, No. 3. DOI: https://doi.org/10.1128/mBio.00631-16 
