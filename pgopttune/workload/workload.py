from logging import getLogger
from psycopg2.extras import DictCursor
from pgopttune.utils.pg_connect import get_pg_connection
from pgopttune.config.postgres_server_config import PostgresServerConfig

logger = getLogger(__name__)


class Workload:
    def __init__(self, postgres_server_config: PostgresServerConfig):
        self.postgres_server_config = postgres_server_config

    def vacuum_database(self):
        """
        run vacuum analyze
        """
        vacuum_analyze_sql = "VACUUM ANALYZE"
        with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(vacuum_analyze_sql)

    def execute_sql_file(self, sql_filepath):
        logger.debug("start execute {}".format(sql_filepath))
        with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(open(sql_filepath, "r").read())
        logger.debug("finish execute {}".format(sql_filepath))

    def get_number_of_xact_commit(self):
        get_number_of_xact_commit_sql = "SELECT xact_commit FROM pg_stat_database WHERE datname = %s"
        with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(get_number_of_xact_commit_sql, (self.postgres_server_config.database,))
                xact_commit = cur.fetchone()["xact_commit"]
        return xact_commit
