import logging
import optuna
from pgopttune.utils.pg_connect import get_pg_dsn, get_pg_connection

logger = logging.getLogger(__name__)


class Workload:
    def __init__(self, host='localhost', port=5432, user='postgres', password='postgres12', database='postgres',
                 pgdata='/var/lib/pgsql/12/data', bin='/usr/pgsql-12/bin', pg_os_user='postgres'):
        self.pgdata = pgdata
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.bin = bin
        self.pg_os_user = pg_os_user

    def vacuum_database(self):
        """
        run vacuum analyze
        """
        vacuum_analyze_sql = "VACUUM ANALYZE"
        with get_pg_connection(dsn=get_pg_dsn(pghost=self.host,
                                              pgport=self.port,
                                              pguser=self.user,
                                              pgpassword=self.password,
                                              pgdatabase=self.database
                                              )) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(vacuum_analyze_sql)