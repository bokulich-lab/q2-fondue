# q2-fondue plugin development
currently logging anything related to q2-fondue here with the goal of transforming this to a qiime2 plugin in the future

## Setup
(currently required for entrezpy-demo)

````
conda create -n entrezpy python=3.8
conda activate entrezpy
conda install --file requirements.txt
pip install entrezpy
````

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
