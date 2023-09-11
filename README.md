# Learning Spark

A local dev environment to follow the examples in ["Learning Spark" 2nd edition
by Damji et
al.](https://web.archive.org/web/20230114135611/https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)

The dev environment provides PySpark and Jupyter installations to work with the
code examples.  The Docker Compose file provides PostgreSQL and Kafka (with the
corresponding Web-UIs) to try out connecting to them from Spark.

# Requirements

- [asdf](https://github.com/asdf-community/asdf-python) (with the Python plugin)
- [Python](https://github.com/asdf-community/asdf-python) >=3.11
- Java >=11
- [Docker](https://docs.docker.com/engine/install/) >= 24.0.2
- [Poetry](https://python-poetry.org/docs/#installation) >= 1.5.1

To connect to PostgreSQL, the corresponding JDBC driver must be downloaded from
https://jdbc.postgresql.org/download/, renamed to `posgresql.jar`, and placed to
the `jars` directory.

# Setup

``` sh
asdf local python 3.11.2  # or whatever 3.11.x version you have installed
POETRY_VIRTUALENVS_PREFER_ACTIVE_PYTHON=true poetry install
```

# Usage

## PySpark and Jupyter

To start Jupyter Lab run

``` sh
poetry run jupyter-lab
```

Then open a notebook in the `notebooks` directory or create your own.

To stop Jupyter Lab press `Ctrl-C` twice.

To start PySpark shell run

``` sh
poetry run pyspark
```

or start a terminal session inside Jupyter Lab and run `pyspark`.

## PostgreSQL and Kafka

To bring up PostgreSQL and Kafka run

``` sh
docker compose up --detach
```

See `docker-compose.yml` for the corresponding ports and credentials of the services.

To stop PostgreSQL and Kafka run

```sh
docker compose down --remove-orphans --volumes
```
