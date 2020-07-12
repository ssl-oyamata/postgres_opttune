import os
import sys
import math
import time
import random
import logging
import string
import traceback
import subprocess
import numpy as np
from typing import Union
from tqdm import tqdm
from psycopg2.extras import DictCursor
from sklearn import linear_model
from pgopttune.parameter.pg_parameter import PostgresParameter
from pgopttune.utils.pg_connect import get_pg_connection
from pgopttune.utils.remote_command import SSHCommandExecutor
from pgopttune.utils.command import run_command
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.workload.oltpbench import Oltpbench
from pgopttune.workload.pgbench import Pgbench

logger = logging.getLogger(__name__)


class Recovery(PostgresParameter):
    def __init__(self,
                 postgres_server_config: PostgresServerConfig,
                 workload: Union[Oltpbench, Pgbench],
                 required_recovery_time_second=300):
        super().__init__(postgres_server_config)
        self._test_table_name = "recovery_time_test"
        os.environ['LANG'] = 'C'
        self.workload = workload
        self.required_recovery_time_second = required_recovery_time_second
        self.x_recovery_time = np.array([])
        self.y_recovery_wal_size = np.array([])

    def estimate_max_wal_size(self):
        self._measurement_recovery_time()  # Measure WAL size and recovery time
        lr = linear_model.LinearRegression()  # Predict WAL size from recovery time using linear regression
        lr.fit(self.x_recovery_time.reshape(-1, 1).astype(np.float64),
               self.y_recovery_wal_size.reshape(-1, 1).astype(np.float64))
        required_recovery_time_second = np.array([[self.required_recovery_time_second]], dtype=np.float64)
        max_wal_size_estimate = lr.predict(required_recovery_time_second)[0][0]
        estimate_max_wal_size = (max_wal_size_estimate / 2)
        # Two to three checkpoints are performed within the WAL size specified in the max_wal_size parameter.
        # Therefore, we divide by 2 assuming the worst case.
        estimate_max_wal_size_mb = str(math.floor(estimate_max_wal_size / (1024 * 1024))) + 'MB'
        logger.info("The maximum value of max_wal_size estimated based on the measured values is {}.".format(
            estimate_max_wal_size_mb))
        return estimate_max_wal_size_mb

    def _measurement_recovery_time(self, measurement_second_scale=300, measurement_pattern=5):
        # measurement pattern
        measurement_second_array = np.arange(measurement_second_scale,
                                             measurement_second_scale * (measurement_pattern + 1),
                                             measurement_second_scale)
        sum_measurement_second = np.sum(measurement_second_array)
        logger.debug("Measurement of WAL size and recovery time pattern {} s".format(measurement_second_array))
        self._no_checkpoint_settings()
        self.workload.data_load()
        progress_bar = tqdm(total=100, ascii=True, desc="Measurement of WAL size and recovery time")
        for measurement_time_second in measurement_second_array:
            self.reset_database()
            self.workload.run(measurement_time_second=measurement_time_second)
            recovery_wal_size = self._get_recovery_wal_size()
            self._crash_database()
            self._free_cache()
            recovery_time = self._measurement_recovery_database_time()
            logger.info('The wal size written after the checkpoint is {}B. '
                        'And crash recovery time is {}s'.format(recovery_wal_size, recovery_time))
            self.x_recovery_time = np.append(self.x_recovery_time, recovery_time)
            self.y_recovery_wal_size = np.append(self.y_recovery_wal_size, recovery_wal_size)
            progress_bar.update(measurement_time_second / sum_measurement_second * 100)

        self.reset_param()
        progress_bar.close()
        np.set_printoptions(precision=3)
        logger.info("The wal size written after the checkpoint(Byte) : {}".format(self.y_recovery_wal_size))
        logger.info("PostgreSQL Recovery time(Sec) : {}".format(self.x_recovery_time))

    # def _create_test_table(self):
    #     create_table_sql = "CREATE TABLE IF NOT EXISTS " + self._test_table_name + "(id INT, test TEXT)"
    #     with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
    #         conn.set_session(autocommit=True)
    #         with conn.cursor() as cur:
    #             cur.execute(create_table_sql)
    #
    # def _truncate_test_table(self):
    #     create_table_sql = "TRUNCATE " + self._test_table_name
    #     with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
    #         conn.set_session(autocommit=True)
    #         with conn.cursor() as cur:
    #             cur.execute(create_table_sql)
    #
    # def _insert_test_data(self, row=1, data_size=1024):
    #     with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
    #         for i in range(1, row + 1):
    #             test_column_data = self.random_string(data_size)
    #             insert_sql = "INSERT INTO {} VALUES({}, '{}')".format(self._test_table_name, i, test_column_data)
    #             with conn.cursor() as cur:
    #                 cur.execute(insert_sql)
    #         conn.commit()

    def _no_checkpoint_settings(self):
        no_checkpoint_timeout_sql = "ALTER SYSTEM SET checkpoint_timeout TO '1d'"
        no_checkpoint_wal_size_sql = "ALTER SYSTEM set max_wal_size TO '10TB'"
        logger.debug("No checkpoint settings apply.")
        logger.debug("no_checkpoint_timeout_sql: {}".format(no_checkpoint_timeout_sql))
        logger.debug("no_checkpoint_wal_size_sql: {}".format(no_checkpoint_wal_size_sql))
        with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(no_checkpoint_timeout_sql)
                cur.execute(no_checkpoint_wal_size_sql)

    def _get_recovery_wal_size(self):
        latest_checkpoint_lsn = self._get_latest_checkpoint_lsn()
        get_recovery_wal_size_sql = "SELECT pg_current_wal_insert_lsn() - '{}'::pg_lsn AS wa_size".format(
            latest_checkpoint_lsn)
        with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(get_recovery_wal_size_sql)
                row = cur.fetchone()
                recovery_wal_size = row['wa_size']
        return float(recovery_wal_size)

    def _get_latest_checkpoint_lsn(self):
        if self.postgres_server_config.host == '127.0.0.1' or self.postgres_server_config.host == 'localhost':
            grep_string = "Latest checkpoint location"
            get_latest_checkpoint_cmd_str = "sudo -i -u {} {}/pg_controldata -D {}".format(
                self.postgres_server_config.os_user,
                self.postgres_server_config.pgbin,
                self.postgres_server_config.pgdata)
            get_latest_checkpoint_cmd = get_latest_checkpoint_cmd_str.split()
            grep_cmd = ["grep", grep_string]
            cut_cmd = ["cut", "-d", ":", "-f", "2"]
            logger.debug('run get latest checkpoint lsn command : {}'.format(get_latest_checkpoint_cmd))
            try:
                with open(os.devnull, 'w') as devnull:
                    run_res = subprocess.Popen(get_latest_checkpoint_cmd, stdout=subprocess.PIPE, stderr=devnull)
                grep_res = subprocess.Popen(grep_cmd, stdout=subprocess.PIPE, stdin=run_res.stdout)
                cut_res = subprocess.Popen(cut_cmd, stdout=subprocess.PIPE, stdin=grep_res.stdout)
                latest_checkpoint_lsn = cut_res.communicate()[0].decode('utf-8')
            except ValueError:
                logger.critical(traceback.format_exc())
                logger.info('Failed Command: {} '.format(get_latest_checkpoint_cmd_str))
                sys.exit(1)
        else:
            recovery_database_cmd = 'sudo -i -u {} {}/pg_controldata -D {} ' \
                                    '| grep "Latest checkpoint location" | ' \
                                    'cut -d ":" -f 2'.format(self.postgres_server_config.os_user,
                                                             self.postgres_server_config.pgbin,
                                                             self.postgres_server_config.pgdata)
            ssh = SSHCommandExecutor(user=self.postgres_server_config.os_user,
                                     password=self.postgres_server_config.ssh_password,
                                     hostname=self.postgres_server_config.host,
                                     port=self.postgres_server_config.ssh_port)
            ret = ssh.exec(recovery_database_cmd, only_retval=False, environment_dict={'LANG': 'C'})
            if not ret['retval'] == 0:
                raise ValueError('Get latest checkpoint lsn command failed.\n'
                                 'Failed command : {}'.format(recovery_database_cmd))
            latest_checkpoint_lsn = str(ret["stdout"][0])
        return latest_checkpoint_lsn.replace(' ', '').replace('\n', '')

    def _crash_database(self):
        # localhost PostgreSQL
        if self.postgres_server_config.host == '127.0.0.1' or self.postgres_server_config.host == 'localhost':
            crash_database_cmd = 'sudo -i -u {} {}/pg_ctl -D {} --mode=immediate stop'.format(
                self.postgres_server_config.os_user, self.postgres_server_config.pgbin,
                self.postgres_server_config.pgdata)
            run_command(crash_database_cmd, stdout_devnull=True)
        # remote PostgreSQL
        else:
            crash_database_cmd = '{}/pg_ctl -D {} --mode=immediate stop'.format(self.postgres_server_config.pgbin,
                                                                                self.postgres_server_config.pgdata)
            ssh = SSHCommandExecutor(user=self.postgres_server_config.os_user,
                                     password=self.postgres_server_config.ssh_password,
                                     hostname=self.postgres_server_config.host,
                                     port=self.postgres_server_config.ssh_port)
            ret = ssh.exec(crash_database_cmd)
            if not ret['retval'] == 0:
                raise ValueError('PostgreSQL crash failed.\n'
                                 'crash command : {}'.format(crash_database_cmd))

    def _measurement_recovery_database_time(self):
        # localhost PostgreSQL
        if self.postgres_server_config.host == '127.0.0.1' or self.postgres_server_config.host == 'localhost':
            recovery_database_cmd = 'sudo -i -u {} {}/pg_ctl -D {} start -w'.format(
                self.postgres_server_config.os_user, self.postgres_server_config.pgbin,
                self.postgres_server_config.pgdata)
            recovery_start = time.time()
            run_command(recovery_database_cmd, stdout_devnull=True)
            recovery_elapsed_time = time.time() - recovery_start
        # remote PostgreSQL
        else:
            recovery_database_cmd = '{}/pg_ctl -D {} start -w'.format(self.postgres_server_config.pgbin,
                                                                      self.postgres_server_config.pgdata)
            ssh = SSHCommandExecutor(user=self.postgres_server_config.os_user,
                                     password=self.postgres_server_config.ssh_password,
                                     hostname=self.postgres_server_config.host,
                                     port=self.postgres_server_config.ssh_port)
            recovery_start = time.time()
            ret = ssh.exec(recovery_database_cmd)
            recovery_elapsed_time = time.time() - recovery_start
            if not ret['retval'] == 0:
                raise ValueError('PostgreSQL start failed.\n'
                                 'start command : {}'.format(recovery_database_cmd))
        return float(recovery_elapsed_time)

    @staticmethod
    def random_string(length):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


if __name__ == "__main__":
    from pgopttune.config.postgres_server_config import PostgresServerConfig
    from pgopttune.config.oltpbench_config import OltpbenchConfig

    logging.basicConfig(level=logging.DEBUG)
    conf_path = './conf/postgres_opttune.conf'
    postgres_server_config_test = PostgresServerConfig(conf_path)  # PostgreSQL Server config
    oltpbench_config_test = OltpbenchConfig(conf_path)
    workload_test = Oltpbench(postgres_server_config=postgres_server_config_test,
                              oltpbench_config=oltpbench_config_test)
    recovery = Recovery(postgres_server_config_test, required_recovery_time_second=300, workload=workload_test)
    print(recovery.estimate_max_wal_size())
