import time
import logging
import psycopg2
from pgopttune.utils.remote_command import SSHCommandExecutor
from pgopttune.utils.command import run_command
from pgopttune.utils.pg_connect import get_pg_connection
from pgopttune.config.postgres_server_config import PostgresServerConfig

logger = logging.getLogger(__name__)


class PostgresParameter:
    def __init__(self, postgres_server_config: PostgresServerConfig):
        self.postgres_server_config = postgres_server_config

    def reset_param(self):
        """
        reset postgresql.auto.conf
        """
        # postgresql.auto.conf clear
        alter_system_sql = "ALTER SYSTEM RESET ALL"
        # use psycopg2
        with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(alter_system_sql)

        self.reset_database(is_free_cache=False)  # restart PostgreSQL for reset parameter

    def reset_database(self, is_free_cache=True):
        self._restart_database()
        self._wait_startup_database()
        if is_free_cache:
            self._free_cache()

    def _restart_database(self):
        restart_database_cmd = 'sudo -i -u {} {}/pg_ctl -D {} -w -t 600 restart'.format(
            self.postgres_server_config.os_user, self.postgres_server_config.pgbin,
            self.postgres_server_config.pgdata)
        # localhost PostgreSQL
        if self.postgres_server_config.host == '127.0.0.1' or self.postgres_server_config.host == 'localhost':
            run_command(restart_database_cmd, stdout_devnull=True)
        # remote PostgreSQL
        else:
            restart_database_cmd = '{}/pg_ctl -D {} -w -t 600 restart'.format(self.postgres_server_config.pgbin,
                                                                              self.postgres_server_config.pgdata)
            ssh = SSHCommandExecutor(user=self.postgres_server_config.os_user,
                                     password=self.postgres_server_config.ssh_password,
                                     hostname=self.postgres_server_config.host,
                                     port=self.postgres_server_config.ssh_port)
            ret = ssh.exec(restart_database_cmd)
            if not ret['retval'] == 0:
                raise ValueError('PostgreSQL restart failed.\n'
                                 'Restart command : {}'.format(restart_database_cmd))

    def _wait_startup_database(self, max_retry=300, sleep_time=2):
        is_in_recovery_sql = 'SELECT pg_is_in_recovery()'

        # use psycopg2
        for i in range(max_retry + 1):
            try:
                with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
                    with conn.cursor() as cur:
                        cur.execute(is_in_recovery_sql)
                        result = cur.fetchone()
                        if not result[0]:
                            logger.debug('PostgreSQL is already running.')
                            return
                        else:
                            logger.debug('PostgreSQL is in the process of recovery.')
                            time.sleep(sleep_time)
            except psycopg2.OperationalError:
                logger.debug('PostgreSQL is not running.')
                time.sleep(sleep_time)

        raise TimeoutError('PostgreSQL startup did not complete.')

    def _free_cache(self):
        free_cache_cmd = 'sudo bash -c "sync"; sudo bash -c "echo 1 > /proc/sys/vm/drop_caches"'

        # localhost PostgreSQL
        if self.postgres_server_config.host == '127.0.0.1' or self.postgres_server_config.host == 'localhost':
            run_command(free_cache_cmd)
        else:
            ssh = SSHCommandExecutor(user=self.postgres_server_config.os_user,
                                     password=self.postgres_server_config.ssh_password,
                                     hostname=self.postgres_server_config.host,
                                     port=self.postgres_server_config.ssh_port)
            ret = ssh.exec(free_cache_cmd)
            if not ret['retval'] == 0:
                raise ValueError('Free cache failed.\n'
                                 'Restart command : {}'.format(free_cache_cmd))

    @staticmethod
    def check_parameter_maxvalue_depend_memory_size(parameter_name):
        parameters_depend_memory_size = ['effective_cache_size', 'shared_buffers', 'temp_buffers']
        return parameter_name in parameters_depend_memory_size

    @staticmethod
    def check_parameter_maxvalue_depend_cpu(parameter_name):
        parameters_depend_cpu = ['max_worker_processes', 'max_parallel_workers',
                                 'max_parallel_workers_per_gather', 'max_parallel_maintenance_workers']
        return parameter_name in parameters_depend_cpu
