.PHONY: all lint test test-cov install dev clean distclean

PYTHON ?= python

all: ;

lint:
	flake8

test: all
	py.test

test-cov: all
	py.test --cov=q2_fondue

install: all
	bash install-sra-tools.sh
	$(PYTHON) setup.py install

dev: all
	bash install-sra-tools.sh
	pip install -e .

clean: distclean

distclean: ;
