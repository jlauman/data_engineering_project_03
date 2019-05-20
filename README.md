# Project 3: Data Warehouse

## Copy Jupyter Workspace

    zip -r workspace.zip workspace
    mv workspace.zip workspace_original.zip
    mv workspace_original.zip workspace


## Set Up Anaconda

Perform git clone of repository first.

    conda create --name dend_project_03 python=3.7
    conda activate dend_project_03
    conda install jupyterlab
    conda install psycopg2
    conda install pandas
    conda install git
    pip install ipython-sql

Set the Jupyter environment password.

    jupyter notebook password

Mac OS X packages require for exporting notebook as PDF.

    brew cask install mactex

Add `export PATH="$PATH:/Library/TeX/texbin"` to `~/.bash_profile`.


## Set Up VSCode

VisualStudio Code has the following plug-ins...
Anaconda Extension Pack
Remote Development
Bookmarks


## Set Up PostgreSQL

    bin/docker_pull_all.sh
    bin/run_postgres.sh


## Set Up Amazon S3 Bucket Data

    brew install awscli

https://udacity-dend.s3.amazonaws.com/

    aws s3 sync s3://udacity-dend/song_data ./data/song_data --no-sign-request
    aws s3 sync s3://udacity-dend/log_data ./data/log_data --no-sign-request
    aws s3 sync s3://udacity-dend/log_json_path.json ./data/log_json_path.json --no-sign-request


## Set Up Name to Gender Data

gender from https://www.ssa.gov/oact/babynames/limits.html

    mkdir -p data/names
    unzip names.zip -d data/names

select gender, count(gender) from s_songplay_event group by gender;
select count(gender) as total from s_songplay_event;



## Run Jupyter Labs

bin/run_jupyter.sh


## Amazon EC2 Instance

    sudo yum -y install git
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
    bash Miniconda3-latest-Linux-x86_64.sh

Exit shell and ssh into ec2 instance to get clean environment. Ensure that the python version is 3.7.x.

    python --version


## Amazon Redshift

https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-multi-row-inserts.html


## Song Data - Dictionary

An example from song file `workspace/song_data/A/A/A/TRAAAAK128F9318786.json`.

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

An example of an event record `workspace/log_data/2018/11/2018-11-01-events.json`

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



only 7 distinct minutes in songplay events
minute
1
5
2
4
0
6
3





-- what date range is in songplay data
select min(start_time), max(start_time)
from f_songplay f;



-- what are the top 10 most played artists
select
    row_number() over (order by count(f.artist_id) desc) as rank,
    d1.name as artist_name
from f_songplay f
join d_artist d1 on d1.artist_id = f.artist_id
where f.artist_id is not null
group by f.artist_id, d1.name
order by count(f.artist_id) desc
limit 10;



-- what are the top 10 most played artists by gender
with
female_top10_artists as (
    select
        row_number() over (order by count(f.artist_id) desc, d1.gender, d2.name) as rank,
        d1.gender,
        d2.name
    from f_songplay f
    join d_user d1 on d1.user_id = f.user_id
    join d_artist d2 on d2.artist_id = f.artist_id
    where f.artist_id is not null
    and d1.gender = 'F'
    group by d1.gender, d2.name
    order by count(f.artist_id) desc, d1.gender, d2.name
    limit 10
),
male_top10_artists as (
    select
        row_number() over (order by count(f.artist_id) desc, d1.gender, d2.name) as rank,
        d1.gender,
        d2.name
    from f_songplay f
    join d_user d1 on d1.user_id = f.user_id
    join d_artist d2 on d2.artist_id = f.artist_id
    where f.artist_id is not null
    and d1.gender = 'M'
    group by d1.gender, d2.name
    order by count(f.artist_id) desc, d1.gender, d2.name
    limit 10
)
select F.rank, F.name as female, M.name as male
from female_top10_artists as F
join male_top10_artists as M on F.rank = M.rank;



-- where are most Colplay song plays occuring?
select count(f.location), f.location
from f_songplay f
join d_artist d1 on d1.artist_id = f.artist_id
where d1.name = 'Coldplay'
group by f.location
order by count(f.location) desc;



-- where are most Kings of Leon song plays occuring?
select count(f.location), f.location
from f_songplay f
join d_artist d1 on d1.artist_id = f.artist_id
where d1.name = 'Kings Of Leon'
group by f.location
order by count(f.location) desc;



-- what are the free and paid user counts by location?
select f.location, f.level, count(level)
from f_songplay f
group by f.location, f.level
order by f.location, f.level;
