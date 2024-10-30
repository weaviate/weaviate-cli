# Contributing to Weaviate CLI

Thank you for looking into contributing to Weaviate CLI! We really appreciate that you are willing to spend some time and effort to make Weaviate better for everyone!

We have a detailed [contributor guide](https://weaviate.io/developers/contributor-guide/current/) as a part of our documentation. If you are new, we recommend reading through the [getting started guide for contributors](https://weaviate.io/developers/contributor-guide/current/getting-started/index.html) after reading this overview.

## Finding a Good First Issue
We use the `good-first-issue` labels on issues that we think are great to get started. These issues are typically isolated to a specific area of the CLI.

## Prerequisites
To contribute to Weaviate CLI you should have:
- Basic Python skills
- Experience using Weaviate CLI to understand the effects of changes
- Knowledge of writing tests (see our test files for examples)

## Development Setup
*Note: The Weaviate team uses Linux and Mac (darwin/arm64) machines exclusively. Development on Windows may lead to unexpected issues.*

1. Fork the repository
2. Clone your fork
3. Create a virtual environment:
    ```bash
    python -m venv .venv
    ```
4. Install development dependencies using Makefile:
    ```bash
    make install-dev
    ```

### Code Style
- We use Black for code formatting
- Install development dependencies and pre-commit hooks (if not already installed):
  ```bash
  make install-dev
  ```
- Before committing, ensure code is formatted:
  ```bash
  make format
  ```
- CI will check formatting using:
  ```bash
  make lint
  ```

## Tagging Your Commit
Please tag your commit(s) with the appropriate GH issue that your change refers to, e.g. `gh-9001 reduce memory allocations of ACME widget`. Please also include something in your PR description to indicate which issue it will close, e.g. `fixes #9001` or `closes #9001`.

## Pull Request
If you open an external pull request our CI pipeline will get started. This external run will not have access to secrets. This prevents people from submitting a malicious PR to steal secrets. As a result, the CI run will be slightly different from an internal one. For example, it will not automatically push a Docker image. If your PR is merged, a container with your changes will be built from the trunk.

## Agreements

### Code of Conduct
Please note that this project is released with a Contributor Code of Conduct. By participating in this project you agree to abide by its terms.
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](CODE_OF_CONDUCT.md)

### Contributor License Agreement
Contributions to Weaviate must be accompanied by a Contributor License Agreement. You (or your employer) retain the copyright to your contribution; this simply gives us permission to use and redistribute your contributions as part of Weaviate. Go to [this page](https://www.semi.technology/playbooks/misc/contributor-license-agreement.html) to read the current agreement.

The process works as follows:
- You contribute by opening a [pull request](#pull-request).
- If you have not contributed before, our bot will ask you to agree with the CLA.

## If in Doubt, Ask
The Weaviate team consists of some of the nicest people on this planet. If something is unclear or you'd like a second opinion, please don't hesitate to ask. We are glad that you want to help us, so naturally, we will also do our best to help you on this journey.

## Thanks for Contributing!
We really appreciate your effort in making Weaviate better for everyone!
