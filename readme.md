# miniHive

This repository provides a mini version of Hadoop Hive, which compiles SQL queries into MapReduce workflows

## Instructions

### Running miniHive locally

`src/miniHive.py` provides testing functions on data following the schema of TPC-H

Firstly copy source code of `dbgen` to the working directory, otherwise specify with `--dbgen <path-to-dbgen>`

```
python3 src/miniHive.py \
    --O --SF 0.01 --env LOCAL \
    "select distinct N_NAME from NATION"
```

The above command does the following things:

- Generate test data with a scale factor of 0.01
- Run the query locally, with the query optimization on

### Running a certain module

Compiling a query string into a workflow tree

```
python3 -m luigi \
    --module src.ra2mr SelectTask \
    --querystring "\select_{gender='female'} Person;" \
    --exec-environment LOCAL --local-scheduler
```
