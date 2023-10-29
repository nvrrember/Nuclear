# nrc_reactor
## A python module for data exploration of NRC Nuclear Reactors for the last 365 days

Data retreived from https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/powerreactorstatusforlast365days.txt

### Quick Start

Create virtual environment in your Windows Linux Subsystem - if not installed, you will be prompted to install python3-venv
```sh
$ python3 -m venv .venv
```

Activate your virtual environment

```sh
$ source .venv/bin/activate
```

install the required packages to the venv

```sh
$ pip install -r requirements.txt
```

Run data_download.py to create database and nrc_reactors table

```sh
$ python3 nrc_reactor/data_download.py
```

For examples of functions, python3 nrc_reactor/reactors.py

```sh
$ python3 nrc_reactor/reactors.py
```

### Installing to another Python Env

Open command prompt, WSL, or Bash and cd to the root directory of the package
```sh
$ cd path/to/Nuclear
```

Run pip install with the python env you would like the package to be installed to
```sh
$ python3 -m pip install .
```

### Testing

Run tests
```sh
$ python3 -m pytest tests/
```



