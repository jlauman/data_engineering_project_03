#!/usr/bin/env bash

sudo yum -y install git

# for ipython-sql
conda config --add channels conda-forge

conda install -y --name dend_project_03 jupyterlab
conda install -y --name dend_project_03 psycopg2
conda install -y --name dend_project_03 pandas
# conda install -y --name dend_project_03 git

conda install -y --name dend_project_03 ipython-sql
# pip install ipython-sql
