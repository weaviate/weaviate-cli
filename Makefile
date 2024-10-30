.PHONY: format lint test install-dev

build:
	python -m build

build-check:
	python -m twine check dist/*

install-dev:
	pip install -r requirements-dev.txt
	pre-commit install

format:
	black cli.py weaviate_cli test

lint:
	black --check cli.py weaviate_cli test

test:
	pytest test/unittests

build-all: build build-check

all: format lint test build-all
