# q2-fondue ![CI](https://github.com/bokulich-lab/q2-fondue/actions/workflows/ci.yaml/badge.svg)

### Installation

Before q2-fondue is available *via* conda, you can use the following instructions to install it on your machine:

```shell
conda create -y -n fondue
conda activate fondue
conda install \
  -c conda-forge -c bioconda -c qiime2 -c defaults \
  qiime2 q2cli q2-types "entrezpy ~=2.1.2" "sra-tools ~=2.10.1" xmltodict
```

The current q2-fondue version supports QIIME 2 **v2021.4** or higher.

#### DEV note:
Until QIIME 2 2021.4 is officially released, replace `-c qiime2` in the command above with
`-c https://packages.qiime2.org/qiime2/2021.4/staged` to fetch the lastest dev version instead.

## Useful resources:
* List of all available NCBI databases: 
  - https://www.ncbi.nlm.nih.gov/search/
  - table1 in https://academic.oup.com/nar/article/45/D1/D12/2605705
* EntrezPy: https://entrezpy.readthedocs.io/en/master/
