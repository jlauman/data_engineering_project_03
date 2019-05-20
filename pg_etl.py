import configparser, os, glob, csv, json, hashlib
import pandas as pd
import psycopg2
from pprint import pprint
from pg_sql_queries import staging_events_copy, staging_songs_copy
from pg_sql_queries import insert_table_queries


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


def load_staging_log_data(cur):
    # load log_data (events) into s_event table
    base_path = os.getcwd() + '/data/log_data'
    for root, dirs, files in os.walk(base_path):
        file_paths = glob.glob(os.path.join(root,'*.json'))
        # pprint(file_paths)
        for file_path in file_paths:
            print('log_data: %s' % file_path)
            df = pd.read_json(file_path, lines=True)
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
                cur.execute(staging_events_copy, [
                    event_id,
                    row.artist,
                    row.auth,
                    row.firstName,
                    int(row.itemInSession),
                    row.lastName,
                    NAME_TO_GENDER[row.firstName],
                    float(row.length),
                    row.level,
                    row.location,
                    row.method,
                    row.page ,
                    row.registration,
                    row.sessionId,
                    row.song,
                    int(row.status),
                    row.timestamp.strftime('%Y-%m-%d %H'),
                    row.year,
                    row.week,
                    row.month,
                    row.day,
                    row.hour,
                    row.weekday,
                    row.userAgent,
                    row.userId
                ])


def load_staging_song_data(cur):
    # load songs into s_song table
    base_path = os.getcwd() + '/data/song_data'
    for root, dirs, files in os.walk(base_path):
        file_paths = glob.glob(os.path.join(root,'*.json'))
        # pprint(file_paths)
        for file_path in file_paths:
            print('song_data: %s' % file_path)
            with open(file_path) as json_file:
                data = json.load(json_file)
                if data['year'] == 0: data['year'] = None
                if str(data['artist_location']).startswith('<a'): data['artist_location'] = None
                # pprint(data)
                cur.execute(staging_songs_copy, [
                    data['artist_id'],
                    data['artist_latitude'],
                    data['artist_location'],
                    data['artist_longitude'],
                    data['artist_name'],
                    data['duration'],
                    data['num_songs'],
                    data['song_id'],
                    data['title'],
                    data['year']
                ])



def load_staging_tables(cur, conn):
    load_staging_song_data(cur)
    load_staging_log_data(cur)
    conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        if query.strip() != "":
            pprint(query)
            cur.execute(query)
            conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('pg_dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_gender_lookup()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()