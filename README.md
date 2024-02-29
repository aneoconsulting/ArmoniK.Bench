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
