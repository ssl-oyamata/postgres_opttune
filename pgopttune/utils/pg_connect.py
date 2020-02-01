import psycopg2
from psycopg2.extras import DictCursor


def get_pg_dsn(pghost='localhost',
               pgport=5432,
               pguser='postgres',
               pgpassword='postgres',
               pgdatabase='postgres'
               ):
    dsn = "postgresql://{}:{}@{}:{}/{}".format(pguser, pgpassword, pghost, pgport, pgdatabase)
    return dsn


def get_pg_connection(dsn='postgresql://postgres:password@localhost:5432/postgres'):
    return psycopg2.connect(dsn)


if __name__ == "__main__":
    with get_pg_connection(dsn=get_pg_dsn(pghost='localhost',
                                          pgport=5432,
                                          pguser='postgres',
                                          pgpassword='postgres12',
                                          pgdatabase='postgres'
                                          )) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute('SELECT id FROM test')
            for row in cur:
                print(dict(row))