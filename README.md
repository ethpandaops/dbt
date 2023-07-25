<p align="center">
  <img src="logo.svg" alt="dbt" />
</p>

Data Build Tool ([dbt](https://github.com/dbt-labs/dbt-core)) for the transformation layer of the [Xatu](https://docs.ethpandaops.io/xatu/intro) data pipeline.

-----
<p align="center" style="display: inline-block"> 
  <a target=”_blank” href="https://dbt.platform.ethpandaops.io">View generated docs</a>
</p>

-----

## Prerequisites

- [dbt](https://docs.getdbt.com/dbt-cli/installation)
- [Python 3.11+](https://www.python.org/downloads/)

## Installation

```bash
# Install dependencies
pip install -r requirements.txt
```

## Usage

```bash
# replace the 'REPLACE_ME' with the correct values
DBT_HOST=REPLACE_ME DBT_USER=REPLACE_ME DBT_PASSWORD=REPLACE_ME dbt run
```

## Linting

```bash
DBT_HOST=REPLACE_ME DBT_USER=REPLACE_ME DBT_PASSWORD=REPLACE_ME sqlfluff lint .
``````
