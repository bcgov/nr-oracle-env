# Overview

A CLI tool to help understand relationships in the database.  You can feed the script
a seed table and it will identify all the objects in the database that have some kind
of relationship to that table:

- related tables through foreign key constraints
- views
- triggers
- procedurs
- functions

# Running the tool

## install relationships:

`uv

# Development

The cli tool is built using python.  Development was completed using python 3.13, but other versions
will likely work as well.

Project is using uv for dependency management:

Install uv:
`python3.13 -m pip install uv`

Create the initial project

`uv init data-query-tool`

add a new package dependency

`uv add requests`

install dependencies defined in lock file

`uv sync`

update/recreate the lock file from what is defined in pyproject.toml

`uv lock`

Install dev dependencies

`uv add ruff --dev`

Uv cache directory

`uv python dir`

Tool cache directory

`uv tool dir`

Activate env

`. .venv/bin/activate`

Use tool without installing

`uvx toolname`

Upgrade a specific dependency

`uv lock --upgrade-package requests`
