# Project 3: Data Warehouse

Sparkify, a stream music startup, wants to move user data and song data into the cloud.
The technology platform for this reporting solution will be Amazon Web Services (AWS).
There are two sets of data available in an AWS S3 file storage: song files and song-play event records.

The objective of this solutions is to build a song-play reporting data warehouse using
ETL processes to read files from S3, populate staging tables and then populate fact and
dimension tables.

To support the anticipated future scale of this reporting solution the AWS RedShift clustered
database technology has been selected.


## Project Repository

The project repository is <https://github.com/jlauman/data_engineering_project_03.git>.

Perform `git clone` of the repository into a working folder.

The initial analysis for this project was performed using a local PostgreSQL database and the files
beginning with `pg_` are the result of that analysis. To use the PostgreSQL files the database must
be configured with a database name, user and password as they exist in the `pg_dwh.cfg` file.

The final solution of this project was executed on an AWS EC2 instance and in a Redshift cluster.
The files for AWS EC2/Redshift are prefixed with `rs_`.


## Jupyter Notebooks with Example Queries

For reference both the PostgreSQL prototype and the AWS Redshift solution notebooks are included.
Each notebook has data quality checks and example queries. Exports from each notebook are provided below.

Redshift solution example queries are in [rs_example_queries.md](./rs_example_queries.md).

PostgreSQL prototype example queries are in [pg_example_queries.md](./pg_example_queries.md).


## Data Examples

A single song record and a single song-play event record are included here for reference.
Note that in the S3 bucket there is one song per S3 object and multiple song-play events
per S3 object.


### Song Record Example

An example song file is shown below. The example is from file `workspace/song_data/A/A/A/TRAAAAK128F9318786.json`.
There is one JSON object per song file.

```json
{
  "artist_id": "AR73AIO1187B9AD57B",
  "artist_latitude": 37.77916,
  "artist_location": "San Francisco, CA",
  "artist_longitude": -122.42005,
  "artist_name": "Western Addiction",
  "duration": 118.07302,
  "num_songs": 1,
  "song_id": "SOQPWCR12A6D4FB2A3",
  "title": "A Poor Recipe For Civic Cohesion",
  "year": 2005
}
```

### Song-Play Event Record Example

An exapmle song-play event record is shown below. The example is from file `workspace/log_data/2018/11/2018-11-01-events.json`.
There are multiple song-play events per file, so the file will be parsed into a pandas dataframe.

```json
{
  "artist": "Infected Mushroom",
  "auth": "Logged In",
  "firstName": "Kaylee",
  "gender": "F",
  "itemInSession": 6,
  "lastName": "Summers",
  "length": 440.2673,
  "level": "free",
  "location": "Phoenix-Mesa-Scottsdale, AZ",
  "method": "PUT",
  "page": "NextSong",
  "registration": 1540344794796.0,
  "sessionId": 139,
  "song": "Becoming Insane",
  "status": 200,
  "ts": 1541107053796,
  "userAgent": "\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36\"",
  "userId": "8"
}
```


## Design

The design of this ETL process is illustrated in the diagram shown below.


```
+------------+     +------------+     +------------+
|            |     |            |     |            |
|            |     |  EC2 VM    |     |  RedShift  |
| S3 Buckets +---->+  for ETL   +---->+  Database  |
|            |     |            |     |            |
|            |     |            |     |            |
+------------+     +------------+     +------------+
```

The first step of the ETL pipeline copies and transformed song records and song-play event
records from S3 bucket objects into staging tables in AWS Redshift. After song records are
read, transformed and populated into an `s_song` staging table they will persist in the Redshift
database for the lifetime of the Sparkify reporting database. The expectation is that there
will be frequent small updates to the song records and the bulk of the load for song will
occur once. This is the same expecation for the song-play events - frequent small updates.

Based on these expectations the design is built around an ETL server that is capable of streaming
song records and song-play event records into the staging tables.

NOTE: What is missing from the staging tables is a flag to indicate that the staged record has already
been processed into either the dimension or fact tables. For long-term operation of the ETL server
this flag needs to be added to the `s_*` tables and existing records flagged as `true`.


### Database Design (AWS Redshift and PostgreSQL)

As part of the data investigation page of this project a prototype data warehouse was built using
PostgreSQL. The final project was executed in AWS Redshift and only the details for Redshift
are included here.

The naming convention for the reporting tables are:

* `d_` prefix for dimension tables
* `f_` prefix for fact tables
* `s_` prefix for staging tables

The database design exists of the following tables:

* `d_artist`: artist dimension table
* `d_song`: song dimension table
* `d_time` time dimension table
* `d_user`: Sparkify user dimension table
* `f_songplay`: Song-play fact table
* `s_song`: Song staging table
* `s_songplay_event`: Song-play event staging table

For AWS Redshift, the dimension tables use the `all` distribution method across slices. This is
an initial design decision and it may be determined that the `d_time` dimension table needs to be
distributed by the `start_time` key as it grows. The initial data management plan is to retain the
staging tables, so the staged records may be replayed into dimension and fact tables if the database
schema changes.

See the following documentation for a discussion on Redshift inserts.
https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-multi-row-inserts.html

One of the architectural differences between Redshift and PostgreSQL is how serial columns are incremented.
Find a discussion of this in the following documentation. https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

Rather than using serial/incremented numbers as song-play event identifiers a hashing mechanism is used.
See line 82 of `rs_etl.py` for the implementation.


## General Project Set Up

After cloning the git repository use the following section to set up the solution.

For the Redshift solution perform the `git clone` and general set up steps on an EC2 instance in the same
VPC security group as the Redshift cluster. See the `AWS EC2 Instance` instructions below.


### Set Up Visual Studio Code

For this solution `Visual Studio Code - Insiders` was used to take advantage of the `Remote Development`
extension. When the `Remote Development` extension is available for stable `Visual Studio Code` then
`Visual Studio Code` should be preferred.

Several `Visual Studio Code` extensions were configured for this solution.

* Remote Development
* Anaconda Extension Pack
* Bookmarks


### Set Up Anaconda

The Anaconda package manager is used in this solution.
Follow the installation instruction at <https://docs.anaconda.com/anaconda/install/>.
After the `conda` command is available the shell run the following.

    conda create --name dend_project_03 python=3.7
    conda activate dend_project_03
    conda install jupyterlab
    conda install psycopg2
    conda install pandas
    conda install git
    pip install ipython-sql

Also, set the Jupyter notebook password.

    jupyter notebook password

Use the `bin/run_jupyter.sh` script to start and verify the Jupyter environment functions.


### Set Up Name to Gender Data

Name and gender data was not provided as part of the S3 data. To complete the solution as
described gender information from [babynames](https://www.ssa.gov/oact/babynames/limits.html)
has been included in this project.

To set up the names perform the following:

    mkdir -p data/names
    unzip names.zip -d data/names


NOTE: After the staging table are populated the following queries may be used to examine
the breakdown of genders in the song-play events.

    select gender, count(gender) from s_songplay_event group by gender;

    select count(gender) as total from s_songplay_event;


## Amazon Web Services (AWS) Set Up

Amazon Web Services is the primary technology platform for this solution. Instructions for set up
and execution of the PostgreSQL prototype are included at the end of this document.


### AWS Redshift Cluster

Follow the normal Redshift cluster creation documentation available on AWS with the addition of using
an `Elastic IP` for the publically exposed IP address. This change helps with diagnosing
configuration problems. Also, ensure that port 5439 is open on the VPC Security Group that is assigned
to the cluster. With this configuration the Redshift cluster may accessed using a normal desktop
database client (like DbVisualizer).


### AWS EC2 Instance

Create an EC2 VM instance to be the ETL server for this solution. Run the following commands to
install the base packages required for this solution.

    sudo yum -y install git postgresql
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
    bash Miniconda3-latest-Linux-x86_64.sh

Exit shell and then `ssh` into the  EC2 instance to get clean environment. Ensure that the python version is 3.7.x.

    python --version


### AWS Project Configuration

Change the contents of the `rs_dwh.cfg` file to match the IP address of the cluster leader node. Also, change
the database name, username and password to match the configuration of the Redshift cluster.

Use the following command as a template to test the connection from the EC2 VM to the Redshift cluster.

    psql --host=172.31.9.254 --port=5439 --user=sparkify --dbname=sparkify

The S3 buckets containing song and song-play event records is accessible without authentication. See the
`boto3` client configuration on line 62 of `rs_etl.py`.


### AWS Execution

First, ensure the anaconda environment is `dend_project_03`.

To execute the ETL process, run the following commands:

    python rs_create_tables.py
    python rs_etl.py

After the ETL script executes the dimension and fact tables will be populated.


### AWS Run Jupyter Labs

In order to run and connect to a Jupyter notebook running on the EC2 VM a port must be opened
(which is not secure without SSL) or a port must be forwarded from the local box.

For this solution, `Visual Studio Code - Insider` as used to forward port 8888 to the EC2 VM.


## Local Set Up

The following instructions are for the PostgreSQL prototype set up.


### Local PostgreSQL Set Up

The local PostgreSQL prototype set up requires Docker.
Ensure that Docker CE is running and then run the following:

    bin/docker_pull_all.sh
    bin/run_postgres.sh


### Local Download of S3 Bucket Data

The local PostgreSQL prototype uses local data files. Install the AWS command line interface (cli)
tool for the local operating system.

On a Mac OS X/macOS box with homebrew installed do the following:

    brew install awscli

The URL for the S3 bucket data is: https://udacity-dend.s3.amazonaws.com/

Run the following command to download the S3 bucket data.

    aws s3 sync s3://udacity-dend/song_data ./data/song_data --no-sign-request
    aws s3 sync s3://udacity-dend/log_data ./data/log_data --no-sign-request
    aws s3 sync s3://udacity-dend/log_json_path.json ./data/log_json_path.json --no-sign-request


### Local Execution

    python pg_create_tables.py
    python pg_etl.py


### Run Jupyter Labs

    bin/run_jupyter.sh


## Extra: Copy Project's Jupyter Workspace

    zip -r workspace.zip workspace
    mv workspace.zip workspace_original.zip
    mv workspace_original.zip workspace


## Extra: Set Up Tex on Mac OS X/macOS

Mac OS X/macOS packages are required for exporting a notebook as PDF.

    brew cask install mactex

Then add `export PATH="$PATH:/Library/TeX/texbin"` to `~/.bash_profile` and open a new shell.
