.PHONY: all lint test test-cov test-docker install dev clean distclean

PYTHON ?= python

all: ;

lint:
	flake8

test: all
	py.test

test-cov: all
	python -m pytest --cov=q2_fondue --junitxml=junit.xml -o junit_family=legacy -n 4 && coverage xml -o coverage.xml

test-docker: all
	qiime info
	qiime fondue --help

install: all
	$(PYTHON) -m pip install -v .

dev: all
	pip install coverage parameterized
	pip install -e .

clean: distclean

distclean: ;
