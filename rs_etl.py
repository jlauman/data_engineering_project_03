import configparser, os, glob, csv, json, hashlib
import pandas as pd
import psycopg2
from pprint import pprint
from rs_sql_queries import staging_events_insert, staging_songs_insert
from rs_sql_queries import insert_table_queries

import boto3
from botocore import UNSIGNED
from botocore.config import Config


DEND_BUCKET='udacity-dend'

# global lookup table
NAME_TO_GENDER = {}


def load_gender_lookup():
    base_path = os.getcwd() + '/data/names'
    for root, dirs, files in os.walk(base_path):
        file_paths = glob.glob(os.path.join(root,'*.txt'))
        for file_path in file_paths:
            print('names: %s' % file_path)
            with open(file_path) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for row in csv_reader:
                    # pprint(row)
                    NAME_TO_GENDER[row[0]] = row[1]
    # pprint(NAME_TO_GENDER)
    True


def get_object_paths(s3, bucket, prefix):
    r1 = s3.list_objects(Bucket=DEND_BUCKET, Prefix=prefix)
    r2 = list(map(lambda obj: obj['Key'], r1['Contents']))
    r3 = list(filter(lambda str: str.endswith('.json'), r2))
    # s3 client does not need to be closed
    return r3


def load_staging_log_data(cur, conn):
    # import pdb; pdb.set_trace()
    # load log_data (events) into s_event table
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    file_paths = get_object_paths(s3, DEND_BUCKET, 'log_data')
    pprint(file_paths)
    for file_path in file_paths:
        sql = str(staging_events_insert)
        print('log_data: %s' % file_path)
        obj1 = s3.get_object(Bucket='udacity-dend', Key=file_path)
        str1 = obj1['Body'].read().decode('utf-8').strip()
        df = pd.read_json(str1, lines=True)
        df = df[df.page == 'NextSong']
        df['timestamp'] = pd.to_datetime(df['ts'], unit='ms')
        df['year'] = df['timestamp'].dt.year
        df['week'] = df['timestamp'].dt.weekofyear
        df['month'] = df['timestamp'].dt.month
        df['day'] = df['timestamp'].dt.day
        df['hour'] = df['timestamp'].dt.hour
        df['weekday'] = df['timestamp'].dt.weekday
        # pprint(df)
        for index, row in df.iterrows():
            # create a sha256 hash for event's unique id
            event_id = hashlib.sha256((str(row.userId) + ' ' + str(row.sessionId) + ' ' + row.timestamp.strftime('%Y%m%d%H%M') + ' ' + row.song).encode('utf-8')).hexdigest()
            str1 = ("(" +
                "'" + event_id + "', " +
                "'" + row.artist.replace("'", "''") + "', " +
                "'" + row.auth + "', " +
                "'" + row.firstName.replace("'", "''") + "', " +
                "" + str(row.itemInSession) + ", " +
                "'" + row.lastName.replace("'", "''") + "', " +
                "'" + NAME_TO_GENDER[row.firstName] + "', " +
                "" + str(row.length) + ", " +
                "'" + row.level + "', " +
                "'" + row.location.replace("'", "''") + "', " +
                "'" + row.method + "', " +
                "'" + row.page + "', " +
                "'" + str(row.registration) + "', " +
                "'" + str(row.sessionId) + "', " +
                "'" + row.song.replace("'", "''") + "', " +
                "'" + str(row.status) + "', " +
                "'" + row.timestamp.strftime('%Y-%m-%d %H') + "', " +
                "" + str(row.year) + ", " +
                "" + str(row.week) + ", " +
                "" + str(row.month) + ", " +
                "" + str(row.day) + ", " +
                "" + str(row.hour) + ", " +
                "" + str(row.weekday) + ", " +
                "'" + row.userAgent.replace("'", "''") + "', " +
                "'" + str(row.userId)  + "'" +
                "),\n")
            sql += str1
        sql = ''.join(sql).strip()[:-1] + ';'
        # print(sql)
        # import pdb; pdb.set_trace()
        cur.execute(sql)
        conn.commit()


def load_staging_song_data(cur, conn):
    # load songs into s_song table
    sql = str(staging_songs_insert)
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    file_paths = get_object_paths(s3, DEND_BUCKET, 'song_data')
    pprint(file_paths)
    for file_path in file_paths:
        print('song_data: %s' % file_path)
        obj1 = s3.get_object(Bucket='udacity-dend', Key=file_path)
        str1 = obj1['Body'].read().decode('utf-8').strip()
        data = json.loads(str1)
        if data['year'] == 0: data['year'] = None
        # fix link string...
        if str(data['artist_location']).startswith('<a'): data['artist_location'] = None
        # pprint(data)
        str2 = ("(" +
            "'" + data['artist_id'] + "', " +
            "" + (str(data['artist_latitude']) if not data['artist_latitude'] == None else 'null') + ", " +
            "'" + str(data['artist_location']).replace("'", "''") + "', " +
            "" + (str(data['artist_longitude']) if not data['artist_longitude'] == None else 'null') + ", " +
            "'" + str(data['artist_name']).replace("'", "''") + "', " +
            "" + str(data['duration']) + ", " +
            "" + str(data['num_songs']) + ", " +
            "'" + data['song_id'] + "', " +
            "'" + str(data['title']).replace("'", "''") + "', " +
            "" + (str(data['year']) if not data['year'] == None else 'null') + "" +
            "),\n")
        sql += str2
        # print(str2)
        if len(sql) > 4096:
            print('  4k insert...')
            sql = ''.join(sql).strip()[:-1] + ';'
            cur.execute(sql)
            conn.commit()
            sql = str(staging_songs_insert)
    print('last insert...')
    sql = ''.join(sql).strip()[:-1] + ';'
    # print(sql)
    # import pdb; pdb.set_trace()
    cur.execute(sql)
    conn.commit()


def load_staging_tables(cur, conn):
    load_staging_song_data(cur, conn)
    load_staging_log_data(cur, conn)


def insert_tables(cur, conn):
    for query in insert_table_queries:
        if query.strip() != "":
            pprint(query)
            cur.execute(query)
            conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('rs_dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # load_gender_lookup()
    # load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()