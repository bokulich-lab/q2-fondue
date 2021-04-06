# q2-fondue plugin development
currently logging anything related to q2-fondue here with the goal of transforming this to a qiime2 plugin in the future

## Setup on MacOS
(currently required for `fondue-demo/demo.ipynb` that includes extraction of sequences & metadata)

````
conda create -n fondue python=3.8
conda activate fondue
conda install --file requirements.txt
pip install entrezpy
````

To get the sra toolkit run (more info available [here](https://github.com/ncbi/sra-tools/wiki/02.-Installing-SRA-Toolkit)): 
````
curl -OL http://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/current/sratoolkit.current-mac64.tar.gz
tar -vxzf sratoolkit.current-mac64.tar.gz
export PATH=$PATH:$PWD/sratoolkit.2.11.0-mac64/bin
````
or just add absolute location of `sratoolkit.2.11.0-mac64/bin` to .zshrc, as in:
`export PATH=$PATH:ABSOLUTE_LOC//sratoolkit.2.11.0-mac64/bin`

! beware to export the path with the correct sratoolkit version number installed on your local machine (above illustrated with `2.11.0`)
! also if you are using an IDE - best to restart it after updating the PATH variable


test if sra toolkit installation worked by:
````
which fasterq-dump
````
should return path exported above


## Funfacts: 

* NCBI maintains Entrez databases which can be access programmatically via
E-utilities &  Entrez-direct.
* Entrezpy = first Python library to offer the same functionalities as Entrez-direct, but as a Python library
* List of all available NCBI databases: https://www.ncbi.nlm.nih.gov/search/ and  
table1 in https://academic.oup.com/nar/article/45/D1/D12/2605705
* Further resources:
https://entrezpy.readthedocs.io/en/master/
https://anaconda.org/bioconda/entrezpy 
https://pypi.org/project/entrezpy/ 
