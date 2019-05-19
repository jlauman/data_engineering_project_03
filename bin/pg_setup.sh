
psql --dbname=template1 --command="create extension if not exists \"pgcrypto\";"

psql --dbname=postgres --command="create role sparkify superuser login encrypted password 'sparkify';"

psql --dbname=postgres --command="create database \"sparkify\" with owner sparkify;"
