import os
import sys
from logging import getLogger
import traceback
import subprocess
from retrying import retry
from xml.etree.ElementTree import parse
from psycopg2.extras import DictCursor
from .workload import Workload
from pgopttune.utils.file_hash import get_file_hash
from pgopttune.utils.pg_connect import get_pg_connection
from pgopttune.utils.command import run_command
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.oltpbench_config import OltpbenchConfig

logger = getLogger(__name__)


class Oltpbench(Workload):
    def __init__(self, postgres_server_config: PostgresServerConfig, oltpbench_config: OltpbenchConfig):
        super().__init__(postgres_server_config)
        self.oltpbench_config = oltpbench_config
        self.backup_database_prefix = 'oltpbench_backup_'

    def data_load(self):
        if self._check_exist_backup_database():
            # Recreate the database using the backed up database as a template
            self._drop_database()
            self._create_database_use_backup_database()
        else:  # First data load
            cwd = os.getcwd()
            config_path = os.path.join(cwd, self.oltpbench_config.oltpbench_config_path)
            data_load_cmd = "{}/oltpbenchmark -b {} -c {} --create=true --load=true".format(
                self.oltpbench_config.oltpbench_path,
                self.oltpbench_config.benchmark_kind,
                config_path)
            os.chdir(self.oltpbench_config.oltpbench_path)
            logger.debug('run oltpbench data load command : {}'.format(data_load_cmd))
            run_command(data_load_cmd)
            os.chdir(cwd)
            self._create_backup_database()  # backup database
        self.vacuum_database()  # vacuum analyze

    def _check_exist_backup_database(self):
        check_exist_backup_database_sql = "SELECT count(datname) FROM pg_database WHERE datname = %s"
        with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(check_exist_backup_database_sql, (self._get_backup_database_name(),))
                result = cur.fetchone()["count"]
        if result == 1:
            logger.debug("Oltpbench database for backup already exists. Database : {} ."
                         .format(self._get_backup_database_name()))
            return True
        else:
            logger.debug("There is no backup database.")
            return False

    @retry(stop_max_attempt_number=5, wait_fixed=10000)
    def _create_backup_database(self):
        create_database_backup_sql = "CREATE DATABASE {} TEMPLATE {}".format(self._get_backup_database_name(),
                                                                             self.postgres_server_config.database)
        with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(create_database_backup_sql)
        logger.debug(
            "The database for backup has been created. Database : {} .".format(self._get_backup_database_name()))

    def _get_oltpbench_config_hash(self):
        return get_file_hash(file_path=self.oltpbench_config.oltpbench_config_path, algorithm='sha1')

    def _get_backup_database_name(self):
        return self.backup_database_prefix + self._get_oltpbench_config_hash()

    @retry(stop_max_attempt_number=5, wait_fixed=10000)
    def _drop_database(self):
        drop_database_sql = "DROP DATABASE {} ".format(self.postgres_server_config.database)
        backup_database_dsn = self._get_backup_database_dsn()
        with get_pg_connection(dsn=backup_database_dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(drop_database_sql)
        logger.debug("The database has been deleted. Database : {} .".format(self.postgres_server_config.database))

    @retry(stop_max_attempt_number=5, wait_fixed=10000)
    def _create_database_use_backup_database(self):
        create_database_use_backup_sql = "CREATE DATABASE {} TEMPLATE {}".format(self.postgres_server_config.database,
                                                                                 self._get_backup_database_name())
        backup_database_dsn = self._get_backup_database_dsn()
        with get_pg_connection(dsn=backup_database_dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(create_database_use_backup_sql)
        logger.debug("Create a database using the backup database as a template. Database : {} . Backup Database : {}".
                     format(self.postgres_server_config.database,
                            self._get_backup_database_name()))

    def _get_backup_database_dsn(self):
        return "postgresql://{}:{}@{}:{}/{}".format(self.postgres_server_config.user,
                                                    self.postgres_server_config.password,
                                                    self.postgres_server_config.host,
                                                    self.postgres_server_config.port,
                                                    self._get_backup_database_name())

    def run(self, measurement_time_second: int = None):
        grep_string = "requests\/sec"
        cwd = os.getcwd()
        config_path = os.path.join(cwd, self.oltpbench_config.oltpbench_config_path)
        if measurement_time_second is not None:
            config_dir, config_filename = os.path.split(config_path)
            measurement_config_path = os.path.join(cwd, config_dir,
                                                   "measurement_test_" + str(measurement_time_second) + "s_"
                                                   + config_filename)
            tree = parse(config_path)  # read xml
            tree.find('works/work/time').text = str(measurement_time_second)
            tree.write(measurement_config_path, 'utf-8', True)
            config_path = measurement_config_path

        run_cmd_str = "{}/oltpbenchmark -b {} -c {} --execute=true -s 5". \
            format(self.oltpbench_config.oltpbench_path,
                   self.oltpbench_config.benchmark_kind,
                   config_path)
        os.chdir(self.oltpbench_config.oltpbench_path)
        run_cmd = run_cmd_str.split()
        grep_cmd = "grep {}".format(grep_string)
        grep_cmd = grep_cmd.split()
        cut_cmd = ["cut", "-d", " ", "-f", "12"]

        logger.debug('run oltpbench command : {}'.format(run_cmd_str))
        try:
            with open(os.devnull, 'w') as devnull:
                run_res = subprocess.Popen(run_cmd, stdout=subprocess.PIPE, stderr=devnull)
            grep_res = subprocess.Popen(grep_cmd, stdout=subprocess.PIPE, stdin=run_res.stdout)
            cut_res = subprocess.Popen(cut_cmd, stdout=subprocess.PIPE, stdin=grep_res.stdout)  # tps
            os.chdir(cwd)
            tps = float(cut_res.communicate()[0].decode('utf-8'))
        except ValueError:
            logger.critical(traceback.format_exc())
            logger.info('Failed Command: {} '.format(run_cmd_str))
            sys.exit(1)
        return tps
