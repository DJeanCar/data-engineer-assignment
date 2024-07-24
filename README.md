# Data Engineering Assignment

This repository holds the Data Engineering Assignment. The assignment represents what a data engineer can be expected to do daily at Verusen: receiving raw data from different sources, transforming it into a structured format, and loading it into a database.

The assignment is to create a **Python** script(s) that reads the data from the CSV files, transforms it into a structured format per the deliverable requirements, and loads it into a SQL (SQLLite, PSQL, MySQL, etc..) database.

## How to run it.

1. We need to run 2 dockers containers, one for apache spark and the another one for postgres.
  - docker run --name postgres -e POSTGRES_PASSWORD=mysecretpassword -d postgres
  - docker run -it apache/spark /opt/spark/bin/spark-shell
2. Create and run a virual env
  - python -m venv env (Python 3.3+)
  - source env/bin/activate
3. Install deps
  - pip install -r requirements.txt
4. Create .env file with the following variables
  - DB_NAME
  - DB_USER
  - DB_PASSWORD
  - DB_HOST
  - DB_PORT
5. Run script
  - python script.py
6. Run test
  - python -m unittest discover -s tests
