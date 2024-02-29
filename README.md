# ArmoniK Bench

Set of tools for benchmarking performance on ArmoniK.

[![License](https://img.shields.io/badge/license-Apache2-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://pypi.python.org/pypi/armonik_bench)
[![Docs](https://readthedocs.org/projects/armonik-bench/badge/?version=latest)](https://armonik-bench.readthedocs.io/en/latest/?version=latest)

## Overview

TODO

## Features

TODO

## Installation

### Prerequisites

- Python 3.10
- pip (Python package installer)

### Setting up a Virtual Environment

It's a good practice to use a virtual environment to isolate your project dependencies. Create a virtual environment using `venv`:

```bash
python3 -m venv .venv
```

Activate the virtual environment:

* On Windows:

```powershell
.\.venv\Scripts\activate
```

* On Unix or MacOS:

```bash
source .venv/bin/activate
```

### Install Apache Airflow

The project uses the [Apache Airflow](https://airflow.apache.org) system. To install it, please follow the [guide](https://airflow.apache.org/docs/apache-airflow/stable/start.html) provided on the Apache Airflow documentation.

Once installation is complete, edit `$AIRFLOW_HOME/airflow.cfg` file to enable Airflow to discover the workflows implemented in the `src/armonik_bench` project directory. To do so, replace the dag directory property as follow:

```bash
dags_folder = /path/to/project/src/armonik_bench
```

You can then finalize the project installation by following the instructions in the following sections.

### Installing the project using pip

Once the virtual environment is activated, you can install the project using pip.

```bash
pip install armonik_bench
```

This will install the project and its dependencies.

### Installing the project from sources

You can also intall the project from sources by cloning the repository.

```bash
git clone git@github.com:aneoconsulting/armonik_bench.git
```

Navigate to the project directory and run:

```bash
pip install .
```

For development, you might want to install additional packages for testing, linting, etc. Install the development dependencies using:

```bash
pip install -e .[dev,tests]
```

## Contributing

Contributions are always welcome!

See [CONTRIBUTING.md](CONTRIBUTING.md) for ways to get started.

## License

[Apache Software License 2.0](LICENSE)
