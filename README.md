# data-management-python [![Build Status](https://api.travis-ci.com/imperial-genomics-facility/data-management-python.svg?branch=master)](https://app.travis-ci.com/github/imperial-genomics-facility/data-management-python)  [![Documentation Status](https://readthedocs.org/projects/data-management-python/badge/?version=master)](https://data-management-python.readthedocs.io/en/master/?badge=master)  [![Codacy Badge](https://app.codacy.com/project/badge/Grade/a6e13220d21b425da3bc4ef59d5cc439)](https://www.codacy.com/gh/imperial-genomics-facility/data-management-python/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=imperial-genomics-facility/data-management-python&amp;utm_campaign=Badge_Grade)

Sequencing data processing for Imperial Genomics Facility

https://data-management-python.readthedocs.io

This repository contains the core Python library developed and maintained by the NIHR Imperial BRC Genomics Facility for managing raw and processed genomic datasets efficiently.

## Key Features

**1. Metadata Management**
  * Utilizes an extended [ENA metadata model](https://ena-docs.readthedocs.io/en/latest/submit/general-guide/metadata.html) for managing information about:
    * Projects
    * Samples
    * Sequencing runs
    * Analysis
    * File paths and
    * Pipeline instances

**2. Genomic Sequencing Runs Processing**
  * Tracks ongoing sequencing runs and initiates processing upon completion.
  * Generates summary reports and sends email notifications to users.

**3. Analysis Pipelines**
  * Includes wrappers for both community-developed and vendor-provided data pipelines.
  * Automates:
    * Configuration generation
    * Input formatting
  * Executes external pipelines on HPC using bash script wrappers.
  * Manages post-processing, including:
    * Custom report generation
    * Analysis data validation

## Requirements
•	Python v3.10

## Installation
**1. Clone the Repository**
```bash
git clone https://github.com/imperial-genomics-facility/data-management-python.git
```

**2. Install Dependencies**
Install required Python libraries:

```bash
pip install -r requirements_2.6.2.txt  # For compatibility with Apache Airflow v2.6.2  
```

**3. Update PYTHONPATH**
Add the core library path to PYTHONPATH:
```bash
export PYTHONPATH=/PATH/data-management-python
```
## Update Airflow version
**1. Set env variables**
```bash
export AIRFLOW_VERSION=VERSION
export PYTHON_VERSION=VERSION
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

**2. Install core Airflow libraries**
```bash
pip install "apache-airflow[celery,postgres,redis,graphviz,pandas,apache-spark,airbyte,amazon,slack,singularity,ssh,sftp,smtp]==VERSION" --constraint ${CONSTRAINT_URL}
```

**3. Install additional libraries**
```bash
pip install asana gviz-api html5lib matplotlib PyMySQL  pytest pytest-cov tox slackclient --constraint ${CONSTRAINT_URL}
```

**4. List Python library versons in the requirements file**
```bash
pip freeze > requirements_vVERSION.txt
```

## License
This project is licensed under the **Apache-2.0 License**. See the [LICENSE](LICENSE) file for details.



