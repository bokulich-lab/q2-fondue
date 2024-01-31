.PHONY: all lint test test-cov install dev clean distclean

PYTHON ?= python

all: ;

lint:
	flake8

test: all
	py.test

test-cov: all
	coverage run -m pytest
	coverage xml

install: all
	bash install-sra-tools.sh
	maturin build --release -m fastq_writer/Cargo.toml
	$(PYTHON) setup.py install
	$(PYTHON) -m pip install --no-deps fastq_writer --find-links fastq_writer/target/wheels/

dev: all
	bash install-sra-tools.sh
	pip install pre-commit coverage parameterized maturin==0.10.3
	pip install -e .
	pre-commit install

prep-dev-container: all
	conda install mamba -qy -n base -c conda-forge
	mamba install -n qiime2-amplicon-2023.9 -qy -c conda-forge -c bioconda -c defaults --file requirements.txt flake8 coverage wget pytest-xdist autopep8
	/opt/conda/envs/qiime2-2023.2/bin/pip install -q https://github.com/qiime2/q2lint/archive/master.zip
	/opt/conda/envs/qiime2-2023.2/bin/pip install -e .

clean: distclean

distclean: ;
