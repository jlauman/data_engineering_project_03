import configparser
import psycopg2
from pg_sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop sparkify database tables.
    Iterate through query list to drop tables.
    """
    for query in drop_table_queries:
        print("drop_tables={}".format(query))
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create sparkify database tables.
    Iterate through query list to create tables.
    """
    for query in create_table_queries:
        print("create_tables={}".format(query))
        cur.execute(query)
        conn.commit()


def main():
    """Set up sprkify database in postgresql.
    """
    config = configparser.ConfigParser()
    config.read('pg_dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()