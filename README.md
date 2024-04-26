Overview
========
This is a data pipeline that retrieves data from a PostgreSQL database, and then loads that data into a Snowflake Database.

Built With
==========
1. Airflow
2. PostgreSQL
3. AWS S3
4. Snowflake
5. Astro CLI


Setup the Project Locally
===========================
1. Clone the repo using:
    git clone <<curr-repo.git>>

2. Build the docker image:
    docker build Dockerfile

3. Access the aiflow UI:
    http://localhost:8080

4. In te airflow UI enter connections for AWS, Snowflake and postgres
