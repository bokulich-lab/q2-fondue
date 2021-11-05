# q2-fondue Tutorial

> q2-fondue (**F**unctions for reproducibly **O**btaining and **N**ormalizing **D**ata re-**U**sed from **E**lsewhere)

A QIIME 2 plugin enabling the easy download of high throughput sequencing data and the corresponding metadata. 

With this easy to use plugin you have millions of deposited sequencing data at your fingertips! Want to set your own data in comparison to other published datasets or start off with a meta-analysis? q2-fondue is here to help! Get all the raw sequencing data and associated metadata of an entire BioProject or fetch the only the sequences or metadata or selected samples. 

## Some Background
There are numerous databases online, where researchers can deposit or retrieve high-throughput sequencing data. However the three biggest and most important storage and sharing platforms certainly are the **E**uropean **N**ucleotide **A**rchive (ENA, by the European Bioinformatics Institute (EMBL-EBI))<sup>1</sup>, the **S**equence **R**ead **A**rchive (SRA, by the National Institutes of Health (NIH), a part of the U.S. Department of Health and Human Services)<sup>2</sup> and the **D**NA **D**ata **B**ank of **J**apan (DDBJ, at the Research Organization of Information and System National Institute of Genetics(NIG))<sup>3</sup>. Through the International Nucleotide Sequence Database Collaboration (INSDC)<sup>4</sup>, data submitted to any of these three organizations is immediately shared among them, enabling free and unrestricted access to deposited data from all three platforms.

Despite the effort to consolidate these colossal amounts of data, it can still be hard to navigate the variable nomenclature used by the different platforms and to actually fetch raw data from the diverse user interfaces. 

### Overview to INSDCs accession number nomenclature 

| Accession Type | Accession Format        | Example    |
|----------------|-------------------------|------------|
| Projects       | PRJ(E\|D\|N)[A-Z][0-9]+ | PRJEB12345 |
| Studies        | (E\|D\|S)RP[0-9]{6,}    | ERP123456  |
| Experiments    | (E\|D\|S)RX[0-9]{6,}    | ERX123456  |
| Runs           | (E\|D\|S)RR[0-9]{6,}    | ERR123456  |

Since the launch of BioProject IDs in 2011, this accession number is commonly referenced in most publications, allowing access to all raw sequencing data and corresponding metadata of the entire project<sup>5</sup>. The subordinate Runs, contain the actual sequencing data of individual samples. These are the two accession number types allowed as input for q2-fondue. 

### Database specific accession numbers 

| Prefix | Name                        | Platform       |
|--------|-----------------------------|----------------|
| PRJNA  | BioProject accession number | SRA (NCBI)     |
| PRJEB  | EBI Project accession       | ENA (EMBL-EBI) |
| PRJD   | DDBJ BioProject             | DDBJ           |
| SRR    | SRA run accession           | SRA (NCBI)     |
| ERR    | ERA run accession           | ENA (EMBL-EBI) |
| DRR    | DRA run accession           | DDBJ           |


## Installation
q2-fondue will be installable as a conda package in the near future. For now, please install it in an existing QIIME 2 environment. 

_Note:_ the current q2-fondue version supports QIIME 2 **v2021.4** or higher - get the latest QIIME 2 release in the [Installation guide](https://docs.qiime2.org/2021.4/install/). 

First activate your QIIME 2 environment and install relevant dependencies:
```shell
conda activate qiime2-2021.8

conda install -c conda-forge -c bioconda -c defaults \
  qiime2 q2cli q2-types "entrezpy>=2.1.2" \
  "sra-tools==2.9.6" xmltodict "tzlocal==2.1"
```

Then install q2-fondue:
```shell
pip install git+https://github.com/bokulich-lab/q2-fondue.git
```

Finally, update your QIIME 2 libraries:
```shell
qiime dev refresh-cache
```

[//]: <> (I deleted the Developer Note - because QIIME 2 2021.8 has been officially released - but I saw that some e.g. q2-coords still have the same note in there?) 


## Using q2-fondue 

### Fetching the sequences and corresponding metadata at once

To download both, the raw sequencing data and associated metadata, altogether execute this command:

```shell
qiime fondue get-all \
              --m-accession-ids-file metadata_file.tsv \ 
              --p-email your_email@somewhere.com \
              --output-dir output-dir-name
```
where:
- `--m-accession-ids-file` is a TSV file containing the accession numbers for the desired Runs or BioProjects
- `--p-email` is your email address (required by NCBI)
- `--output-dir` directory where the downloaded metadata and sequences are stored as QIIME 2 artifacts

_Note:_ the metadata file provided has to contain a header that QIIME 2 can recognize! Simply put e.g. "id" as your column name. Check out other options for identifiers used in QIIME 2 or learn about metadata in general in the [QIIME 2 documentation](https://docs.qiime2.org/2019.10/tutorials/metadata/).

Example metadata files to download data from Bokulich et al. 2016<sup>6</sup>
* *metadata_file.tsv* contains a BioProject number 
* *metadata_file_runs.tsv* contains selected Runs  

[//]: <> (ADD LINKS!)

### Fetching only metadata
To get only the metadata from an entire BioProject or individual Runs, execute the following command:

```shell
qiime fondue get-metadata \
              --m-accession-ids-file metadata_file.tsv \
              --p-n-jobs 1 \
              --p-email your_email@somewhere.com \
              --o-metadata output_metadata.qza
```

where:
- `--m-accession-ids-file` is a TSV file containing the accession numbers for all of the Runs or BioProjects
- `--p-n-jobs` is a number of parallel download jobs (defaults to 1)
- `--p-email` is your email address (required by NCBI)
- `--o-metadata` is the output metadata artifact (.qza) which can directly be used in QIIME 2 

The resulting metadata artifact (.qza) can also be extracted with `qiime tools extract` to get a TSV file. 


### Fetching only sequencing data

To get single-read and paired-end sequences associated with a number of runs, execute this command:
```shell
qiime fondue get-sequences \
              --m-accession-ids-file metadata_file.tsv \
              --o-single-reads output_dir_single \
              --o-paired-reads output_dir_paired
```

where:
- `--m-accession-ids-file` is a TSV containing accession numbers for all of the runs
- `--o-single-reads` is the output artifact containing single-read sequences
- `--o-paired-reads` is the output artifact containing paired-end sequences

_Note:_ This action only accepts individual Run IDs and no BioProject accession numbers.

The resulting artifact will contain the `fastq.gz` files of the sequences, `metadata.yml` and `MANIFEST` files. If the provided accession numbers only contain sequences of one type (e.g. single-read sequences) then the other artifact (e.g. artifact with paired-end sequences) contains empty sequence files with dummy ID starting with `xxx_`.

## References 

[1] **European Nucleotide Archive (ENA)**, accessed 2021-11-05, https://www.ebi.ac.uk/ena/browser/home

[2] **Sequence Read Archive (SRA)**, accessed 2021-11-05, https://www.ncbi.nlm.nih.gov/sra/

[3] **DNA Data Bank of Japan (DDBJ)**, accessed 2021-11-05, https://www.ddbj.nig.ac.jp/index-e.html 

[4] **International Nucleotide Sequence Database Collaboration (INSDC)**, accessed 2021-11-05, https://www.insdc.org

[5] Clark K, Pruitt K, Tatusova T, et al. **BioProject**. 2013 Apr 28 [Updated 2013 Nov 11]. In: The NCBI Handbook [Internet]. 2nd edition. Bethesda (MD): National Center for Biotechnology Information (US); 2013-. Available from: https://www.ncbi.nlm.nih.gov/books/NBK169438/?report=classic

[6] Bokulich N., et al. **Associations among Wine Grape Microbiome, Metabolome, and Fermentation Behavior Suggest Microbial Contribution to Regional Wine Characteristics**. 2016 Jun 14. In: ASM Journals / mBio / Vol. 7, No. 3. DOI: https://doi.org/10.1128/mBio.00631-16 
