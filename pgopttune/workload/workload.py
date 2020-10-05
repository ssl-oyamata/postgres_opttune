from logging import getLogger
from retrying import retry
from psycopg2.extras import DictCursor
from pgopttune.utils.file_hash import get_file_hash
from pgopttune.utils.pg_connect import get_pg_connection
from pgopttune.config.postgres_server_config import PostgresServerConfig

logger = getLogger(__name__)


class Workload:
    def __init__(self, postgres_server_config: PostgresServerConfig):
        self.postgres_server_config = postgres_server_config
        self.backup_database_prefix = 'postgres_opttune_backup_'
        self.config_hash = get_file_hash(postgres_server_config.conf_path)

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

    def check_exist_backup_database(self):
        check_exist_backup_database_sql = "SELECT count(datname) FROM pg_database WHERE datname = %s"
        with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(check_exist_backup_database_sql, (self._get_backup_database_name(),))
                result = cur.fetchone()["count"]
        if result == 1:
            logger.debug("Backup database already exists. Database : {}"
                         .format(self._get_backup_database_name()))
            return True
        else:
            logger.debug("There is no backup database.")
            return False

    @retry(stop_max_attempt_number=5, wait_fixed=10000)
    def create_backup_database(self):
        logger.debug(
            "Start backing up the database. Database : {} ".format(self.postgres_server_config.database))

        create_database_backup_sql = "CREATE DATABASE {} TEMPLATE {}".format(self._get_backup_database_name(),
                                                                             self.postgres_server_config.database)
        with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(create_database_backup_sql)
        logger.debug(
            "The backup is complete. Database : {} ".format(self._get_backup_database_name()))

    def _get_backup_database_name(self):
        return self.backup_database_prefix + self.config_hash

    @retry(stop_max_attempt_number=5, wait_fixed=10000)
    def drop_database(self):
        drop_database_sql = "DROP DATABASE {} ".format(self.postgres_server_config.database)
        backup_database_dsn = self._get_backup_database_dsn()
        with get_pg_connection(dsn=backup_database_dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(drop_database_sql)
        logger.debug("The database has been deleted. Database : {}".format(self.postgres_server_config.database))

    @retry(stop_max_attempt_number=5, wait_fixed=10000)
    def create_database_use_backup_database(self):
        create_database_use_backup_sql = "CREATE DATABASE {} TEMPLATE {}".format(self.postgres_server_config.database,
                                                                                 self._get_backup_database_name())
        backup_database_dsn = self._get_backup_database_dsn()
        with get_pg_connection(dsn=backup_database_dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(create_database_use_backup_sql)
        logger.debug("Create a database using the backup database as a template. Database : {}, Backup Database : {}".
                     format(self.postgres_server_config.database,
                            self._get_backup_database_name()))

    def _get_backup_database_dsn(self):
        return "postgresql://{}:{}@{}:{}/{}".format(self.postgres_server_config.user,
                                                    self.postgres_server_config.password,
                                                    self.postgres_server_config.host,
                                                    self.postgres_server_config.port,
                                                    self._get_backup_database_name())
