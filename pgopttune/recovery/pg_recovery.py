import os
import sys
import math
import time
import random
import string
import traceback
import subprocess
import numpy as np
from logging import getLogger
from typing import Union
from tqdm import tqdm
from psycopg2.extras import DictCursor
from sklearn import linear_model
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import Pipeline
from pgopttune.parameter.pg_parameter import PostgresParameter
from pgopttune.utils.pg_connect import get_pg_connection
from pgopttune.utils.remote_command import SSHCommandExecutor
from pgopttune.utils.command import run_command
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.workload.oltpbench import Oltpbench
from pgopttune.workload.pgbench import Pgbench

logger = getLogger(__name__)


class Recovery(PostgresParameter):
    def __init__(self,
                 postgres_server_config: PostgresServerConfig,
                 workload: Union[Oltpbench, Pgbench],
                 required_recovery_time_second=300,
                 measurement_second_scale=300,
                 measurement_pattern=10):
        super().__init__(postgres_server_config)
        os.environ['LANG'] = 'C'
        self.workload = workload
        self.required_recovery_time_second = required_recovery_time_second
        self.x_recovery_time = np.array([])
        self.y_recovery_wal_size = np.array([])
        self.measurement_second_array = np.arange(measurement_second_scale,
                                                  measurement_second_scale * (measurement_pattern + 1),
                                                  measurement_second_scale)  # measurement pattern

    def estimate_check_point_parameters(self):
        self._measurement_recovery_time()  # Measure WAL size and recovery time
        # reshape
        self.x_recovery_time = self.x_recovery_time.reshape(-1, 1).astype(np.float64)
        self.y_recovery_wal_size = self.y_recovery_wal_size.reshape(-1, 1).astype(np.float64)
        self.measurement_second_array = self.measurement_second_array.reshape(-1, 1).astype(np.float64)
        self.required_recovery_time_second = np.array([[self.required_recovery_time_second]], dtype=np.float64)

        # estimate max_wal_size parameter
        max_wal_size_estimate = self.estimate_use_polynomial_regression(self.x_recovery_time,
                                                                        self.y_recovery_wal_size,
                                                                        self.required_recovery_time_second)
        estimate_max_wal_size = (max_wal_size_estimate * 3)
        # Two to three checkpoints are performed within the WAL size specified in the max_wal_size parameter.
        # We multiply the estimated WAL size by 3 to prevent the checkpoint from being triggered by the WAL size.
        estimate_max_wal_size_mb = str(math.floor(estimate_max_wal_size / (1024 * 1024))) + 'MB'
        logger.info("The value of max_wal_size estimated based on the measured values is {}.".format(
            estimate_max_wal_size_mb))

        # estimate checkpoint_timeout parameter
        checkpoint_timeout_estimate_second = self.estimate_use_polynomial_regression(self.x_recovery_time,
                                                                                     self.measurement_second_array,
                                                                                     self.required_recovery_time_second)
        checkpoint_timeout_estimate_min = str(math.floor(checkpoint_timeout_estimate_second / 60)) + 'min'
        logger.info("The value of checkpoint_timeout estimated based on the measured values is {}.".format(
            checkpoint_timeout_estimate_min))
        return estimate_max_wal_size_mb, checkpoint_timeout_estimate_min

    def _measurement_recovery_time(self):
        sum_measurement_second = np.sum(self.measurement_second_array)
        logger.debug("Measurement of WAL size and recovery time pattern {} s".format(self.measurement_second_array))
        self.no_checkpoint_settings()
        progress_bar = tqdm(total=100, ascii=True, desc="Measurement of WAL size and recovery time")
        for measurement_time_second in self.measurement_second_array:
            self.workload.data_load()
            self.reset_database()
            self.workload.run(measurement_time_second=measurement_time_second)
            recovery_wal_size = self.get_recovery_wal_size()
            self._crash_database()
            self._free_cache()
            recovery_time = self._measurement_recovery_database_time()
            logger.info('The wal size written after the checkpoint is {}B.'
                        'And crash recovery time is {}s'.format(recovery_wal_size, recovery_time))
            self.x_recovery_time = np.append(self.x_recovery_time, recovery_time)
            self.y_recovery_wal_size = np.append(self.y_recovery_wal_size, recovery_wal_size)
            progress_bar.update(measurement_time_second / sum_measurement_second * 100)

        self.reset_param()
        progress_bar.close()
        np.set_printoptions(precision=3)
        logger.info("The wal size written after the checkpoint(Byte) : {}".format(self.y_recovery_wal_size))
        logger.info("PostgreSQL Recovery time(Sec) : {}".format(self.x_recovery_time))

    def get_recovery_wal_size(self):
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

    def _measurement_recovery_database_time(self, wait_time=1000000):
        # localhost PostgreSQL
        if self.postgres_server_config.host == '127.0.0.1' or self.postgres_server_config.host == 'localhost':
            recovery_database_cmd = 'sudo -i -u {} {}/pg_ctl -D {} start -w -t {}'. \
                format(self.postgres_server_config.os_user,
                       self.postgres_server_config.pgbin,
                       self.postgres_server_config.pgdata,
                       wait_time)
            recovery_start = time.time()
            run_command(recovery_database_cmd, stdout_devnull=True)
            recovery_elapsed_time = time.time() - recovery_start
        # remote PostgreSQL
        else:
            recovery_database_cmd = '{}/pg_ctl -D {} start -w -t {}'.format(self.postgres_server_config.pgbin,
                                                                            self.postgres_server_config.pgdata,
                                                                            wait_time)
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

    def no_checkpoint_settings(self):
        self.set_parameter(param_name='checkpoint_timeout', param_value='1d')
        self.set_parameter(param_name='max_wal_size', param_value='10TB')

    @staticmethod
    def estimate_use_linear_regression(x_recovery_time_second, y, required_recovery_time_second):
        lr = linear_model.LinearRegression()
        # lr = linear_model.LinearRegression(fit_intercept=False)
        lr.fit(x_recovery_time_second, y)
        estimated_size = lr.predict(required_recovery_time_second)[0][0]
        return estimated_size

    @staticmethod
    def estimate_use_polynomial_regression(x_recovery_time_second, y, required_recovery_time_second):
        pr = Pipeline([('poly', PolynomialFeatures(degree=2)), ('linear', linear_model.LinearRegression())])
        pr.fit(x_recovery_time_second, y)
        estimated_size = pr.predict(required_recovery_time_second)[0][0]
        return estimated_size

    @staticmethod
    def random_string(length):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


if __name__ == "__main__":
    from pgopttune.config.postgres_server_config import PostgresServerConfig
    from pgopttune.config.oltpbench_config import OltpbenchConfig
    from logging import basicConfig, DEBUG

    basicConfig(level=DEBUG)
    conf_path = './conf/postgres_opttune.conf'
    postgres_server_config_test = PostgresServerConfig(conf_path)  # PostgreSQL Server config
    oltpbench_config_test = OltpbenchConfig(conf_path)
    workload_test = Oltpbench(postgres_server_config=postgres_server_config_test,
                              oltpbench_config=oltpbench_config_test)
    recovery = Recovery(postgres_server_config_test, required_recovery_time_second=300, workload=workload_test)
    logger.debug(recovery.estimate_check_point_parameters())
