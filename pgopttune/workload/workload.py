import logging
import optuna
from pgopttune.utils.pg_connect import get_pg_dsn, get_pg_connection

logger = logging.getLogger(__name__)


class Workload:
    def __init__(self, postgres_server_config):
        self.postgres_server_config = postgres_server_config

    def vacuum_database(self):
        """
        run vacuum analyze
        """
        vacuum_analyze_sql = "VACUUM ANALYZE"
        with get_pg_connection(dsn=get_pg_dsn(pghost=self.postgres_server_config.host,
                                              pgport=self.postgres_server_config.port,
                                              pguser=self.postgres_server_config.user,
                                              pgpassword=self.postgres_server_config.password,
                                              pgdatabase=self.postgres_server_config.database
                                              )) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(vacuum_analyze_sql)

    def execute_sql_file(self, sql_filepath):
        logger.debug("start execute {}".format(sql_filepath))
        with get_pg_connection(dsn=get_pg_dsn(pghost=self.postgres_server_config.host,
                                              pgport=self.postgres_server_config.port,
                                              pguser=self.postgres_server_config.user,
                                              pgpassword=self.postgres_server_config.password,
                                              pgdatabase=self.postgres_server_config.database
                                              )) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(open(sql_filepath, "r").read())
        logger.debug("finish execute {}".format(sql_filepath))
