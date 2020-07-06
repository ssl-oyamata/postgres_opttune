import os
import time
import logging
from psycopg2.extras import DictCursor
from pgopttune.utils.pg_connect import get_pg_connection
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.parameter.pg_parameter import PostgresParameter
from pgopttune.utils.remote_command import SSHCommandExecutor

logger = logging.getLogger(__name__)


class PostgresCsvLog:
    def __init__(self, postgres_server_config: PostgresServerConfig):
        self._postgres_server_config = postgres_server_config
        self._postgres_parameter = PostgresParameter(postgres_server_config)
        self._csv_log_table_name = "csv_log"
        self.start_csv_log_unix_time = None  # The time when the csv log started to be output(epoch time)
        self.end_csv_log_unix_time = None  # The time when the csv log output was finished(epoch time)
        self._csv_log_file_path = None
        self.csv_log_local_file_path = None  # File path when the csv log file is copied to localhost

        # checking logging_collector on
        if self._get_logging_collector_setting() != 'on':
            raise ValueError("You need to turn on the logging_collector for csv log file output.\
                                    The current setting is off; to turn it on, you need to restart PostgreSQL.")
        # get current settings
        self._current_log_destination = self._get_log_destination()
        self._current_log_min_duration_statement = self._get_log_min_duration_statement()

    def __del__(self):
        self.disable()

    def enable(self):
        # set log_destination = '[current_setting],csvlog'
        if 'csvlog' in self._current_log_destination:
            logger.info("The csvlog output is already enabled.\
            log_destination = {}".format(self._current_log_destination))
        else:
            log_destination_value = self._current_log_destination + ',csvlog'
            self._postgres_parameter.set_parameter(param_name="log_destination", param_value=log_destination_value)
            logger.debug("Start outputting PostgreSQL log message to CSV file.\n"
                         "log_destination = '{}'".format(log_destination_value))
        # set log_min_duration_statement = 0
        self._postgres_parameter.set_parameter(param_name="log_min_duration_statement", param_value=0, pg_reload=True)
        logger.debug("Changed setting to output all executed SQL to PostgreSQL log file.\n"
                     "log_min_duration_statement = '0'")
        self.start_csv_log_unix_time = time.time()
        self._csv_log_file_path = self._get_csv_log_file_path()

    def disable(self):
        # set log_destination = '[current_setting]'
        self._postgres_parameter.set_parameter(param_name="log_destination", param_value=self._current_log_destination)
        logger.debug("PostgreSQL log message output to CSV file is disabled.\n"
                     "log_destination = '{}'".format(self._current_log_min_duration_statement))
        # set log_min_duration_statement = current_setting
        self._postgres_parameter.set_parameter(param_name="log_min_duration_statement",
                                               param_value=self._current_log_min_duration_statement, pg_reload=True)
        logger.debug("The value of the log_min_duration_statement parameter has been restored to its original value.\n"
                     "log_min_duration_statement = '{}'".format(self._current_log_min_duration_statement))
        self.end_csv_log_unix_time = time.time()

    def load_csv_to_database(self, copy_dir="/tmp", dsn=None):
        self._copy_csv_logfile_to_local(copy_dir)  # copy logfile to directory(localhost)
        self._create_csv_log_table(dsn)
        self._truncate_csv_log_table(dsn)  # truncate csv log table
        with get_pg_connection(dsn=dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                with open(self.csv_log_local_file_path) as f:
                    # cur.copy_from(f, self.csv_log_table_name, sep=',')
                    cur.copy_expert("copy {} from stdin (format csv)".format(self._csv_log_table_name), f)

    def _copy_csv_logfile_to_local(self, copy_dir="/tmp"):
        file_name = os.path.basename(self._csv_log_file_path)
        self.csv_log_local_file_path = os.path.join(copy_dir, file_name)
        ssh = SSHCommandExecutor(user=self._postgres_server_config.os_user,
                                 password=self._postgres_server_config.ssh_password,
                                 hostname=self._postgres_server_config.host,
                                 port=self._postgres_server_config.ssh_port)
        ssh.get(self._csv_log_file_path, self.csv_log_local_file_path)
        os.chmod(self.csv_log_local_file_path, 0o644)

    def _get_logging_collector_setting(self):
        param_name = "logging_collector"
        return self._postgres_parameter.get_parameter_value(param_name)

    def _get_log_destination(self):
        param_name = "log_destination"
        return self._postgres_parameter.get_parameter_value(param_name)

    def _get_log_min_duration_statement(self):
        param_name = "log_min_duration_statement"
        return self._postgres_parameter.get_parameter_value(param_name)

    def _get_csv_log_file_path(self):
        get_parameter_value_sql = "SELECT pg_current_logfile('csvlog');"
        # use psycopg2
        with get_pg_connection(dsn=self._postgres_server_config.dsn) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(get_parameter_value_sql)
                row = cur.fetchone()
        csv_file_path = row['pg_current_logfile']
        if csv_file_path[0] != "/":
            csv_file_path = os.path.join(self._postgres_server_config.pgdata, csv_file_path)
        return csv_file_path

    def _create_csv_log_table(self, dsn):
        create_table_sql = "CREATE TABLE IF NOT EXISTS {} (" \
                           "log_time timestamp(3) with time zone," \
                           "user_name text," \
                           "database_name text," \
                           "process_id integer," \
                           "connection_from text," \
                           "session_id text," \
                           "session_line_num bigint," \
                           "command_tag text," \
                           "session_start_time timestamp with time zone," \
                           "virtual_transaction_id text," \
                           "transaction_id bigint," \
                           "error_severity text," \
                           "sql_state_code text," \
                           "message text," \
                           "detail text," \
                           "hint text," \
                           "internal_query text," \
                           "internal_query_pos integer," \
                           "context text," \
                           "query text," \
                           "query_pos integer," \
                           "location text," \
                           "application_name text," \
                           "PRIMARY KEY (session_id, session_line_num));".format(self._csv_log_table_name)
        with get_pg_connection(dsn=dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(create_table_sql)

    def _truncate_csv_log_table(self, dsn):
        truncate_table_sql = "TRUNCATE {}".format(self._csv_log_table_name)
        with get_pg_connection(dsn=dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(truncate_table_sql)


if __name__ == "__main__":
    from pgopttune.config.postgres_server_config import PostgresServerConfig

    logging.basicConfig(level=logging.DEBUG)
    conf_path = './conf/postgres_opttune.conf'
    postgres_server_config_test = PostgresServerConfig(conf_path)  # PostgreSQL Server config
    csv_log = PostgresCsvLog(postgres_server_config_test)
    csv_log.enable()
    # logging.debug(csv_log._csv_log_file_path)
    logging.debug("sleep...")
    time.sleep(60)
    csv_log.disable()
    csv_log.load_csv_to_database()
    logging.debug(csv_log.start_csv_log_unix_time)
    logging.debug(csv_log.end_csv_log_unix_time)
