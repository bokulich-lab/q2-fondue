.PHONY: all lint test test-cov test-docker install dev clean distclean

PYTHON ?= python

all: ;

lint:
	flake8

test: all
	py.test

test-cov: all
	python -m pytest --cov=q2_fondue -n 4 && coverage xml -o coverage.xml

test-docker: all
	qiime info
	qiime fondue --help

install: all
	bash install-sra-tools.sh
	$(PYTHON) -m pip install -v .

dev: all
	bash install-sra-tools.sh
	pip install coverage parameterized
	pip install -e .

clean: distclean

distclean: ;
