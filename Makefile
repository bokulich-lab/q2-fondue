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
	pip install coverage parameterized
	pip install -e .

clean: distclean

distclean: ;
