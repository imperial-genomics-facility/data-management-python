[![Build Status](https://api.travis-ci.com/imperial-genomics-facility/data-management-python.svg?branch=master)](https://app.travis-ci.com/github/imperial-genomics-facility/data-management-python)  [![Documentation Status](https://readthedocs.org/projects/data-management-python/badge/?version=master)](https://data-management-python.readthedocs.io/en/master/?badge=master)  [![Codacy Badge](https://app.codacy.com/project/badge/Grade/a6e13220d21b425da3bc4ef59d5cc439)](https://www.codacy.com/gh/imperial-genomics-facility/data-management-python/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=imperial-genomics-facility/data-management-python&amp;utm_campaign=Badge_Grade)

# Data Management Using Python Library

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
•	Python v3.9

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

## License
This project is licensed under the **Apache-2.0 License**. See the [LICENSE](LICENSE) file for details.



