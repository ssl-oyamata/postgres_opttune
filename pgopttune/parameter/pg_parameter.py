import os
import json
import time
import logging
import shutil
import psycopg2
from pgopttune.resource.hardware import HardwareResource
from pgopttune.utils.unit import get_param_raw, format_bytes_str, format_milliseconds_str
from pgopttune.utils.remote_command import SSHCommandExecutor
from pgopttune.utils.command import run_command
from pgopttune.utils.pg_connect import get_pg_dsn, get_pg_connection
from pgopttune.config.postgres_server_config import PostgresServerConfig

logger = logging.getLogger(__name__)


class Parameter:
    def __init__(self, postgres_server_config: PostgresServerConfig, params_json_dir='./conf'):
        self.postgres_server_config = postgres_server_config
        self.config_path = os.path.join(self.postgres_server_config.pgdata, 'postgresql.conf')
        self.tune_parameters_json_path = '{}/version-{}.json'.format(params_json_dir,
                                                                     postgres_server_config.major_version)
        # check file
        if (
                self.postgres_server_config.host == '127.0.0.1' or self.postgres_server_config.host == 'localhost') \
                and not os.path.exists(self.config_path):
            raise ValueError("postgresql.conf does not exist. path : {}".format(self.config_path))
        if not os.path.exists(self.tune_parameters_json_path):
            raise ValueError("tune paramer file does not exist. path : {}".format(self.tune_parameters_json_path))
        self.tune_parameters = self.raw_size_parameters()

    def raw_size_parameters(self):
        tune_parameters = self.load_json_parameters(self.tune_parameters_json_path)
        raw_size_parameters = []
        for tune_parameter in tune_parameters:
            raw_size_parameter = tune_parameter
            # get raw size
            raw_size_parameter['default'] = get_param_raw(tune_parameter['default'], tune_parameter['type'])
            if not (tune_parameter['type'] == 'enum' or tune_parameter['type'] == 'boolean'):
                raw_size_parameter['tuning_range']['minval'] = get_param_raw(tune_parameter['tuning_range']['minval'],
                                                                             tune_parameter['type'])
                raw_size_parameter['tuning_range']['maxval'] = get_param_raw(tune_parameter['tuning_range']['maxval'],
                                                                             tune_parameter['type'])
            raw_size_parameters.append(raw_size_parameter)
        return raw_size_parameters

    def reset_param(self):
        """
        reset postgresql.auto.conf
        """
        # postgresql.auto.conf clear
        alter_system_sql = "ALTER SYSTEM RESET ALL"
        # use psycopg2
        with get_pg_connection(dsn=get_pg_dsn(pghost=self.postgres_server_config.host,
                                              pgport=self.postgres_server_config.port,
                                              pguser=self.postgres_server_config.user,
                                              pgpassword=self.postgres_server_config.password,
                                              pgdatabase=self.postgres_server_config.database
                                              )) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(alter_system_sql)

        self.reset_database(is_free_cache=False)  # restart PostgreSQL for reset parameter

    def change_param_to_trial_values(self, params_trial=None):
        """
        change postgresql.auto.conf to trial values using ALTER SYSTEM
        """
        # clear postgresql.auto.conf
        self.reset_param()
        # setting trial values
        for param_trial in params_trial:
            param_name, param_trial_value = self._convert_trial_value_unit(param_trial)
            alter_system_sql = "ALTER SYSTEM SET {} = '{}'".format(param_name, param_trial_value)
            # use psycopg2
            with get_pg_connection(dsn=get_pg_dsn(pghost=self.postgres_server_config.host,
                                                  pgport=self.postgres_server_config.port,
                                                  pguser=self.postgres_server_config.user,
                                                  pgpassword=self.postgres_server_config.password,
                                                  pgdatabase=self.postgres_server_config.database
                                                  )) as conn:
                conn.set_session(autocommit=True)
                with conn.cursor() as cur:
                    cur.execute(alter_system_sql)

    def change_conf_to_trial_values(self, params_trial=None):
        """
        change tune.conf using trial values(not using)
        """
        tune_setting_path = os.path.join(self.postgres_server_config.pgdata, 'conf.d/tune.conf')
        signal = "# configurations recommended by optuna:\n"
        with open(tune_setting_path, mode='w') as f:
            f.write(signal)
        with open(tune_setting_path, mode='a') as f:
            for param_trial in params_trial:
                param_trial_name, param_trial_values = self._convert_trial_value_unit(param_trial)
                f.write('{} = {}\n'.format(param_trial_name, param_trial_values))

    def save_trial_values_as_conf(self, study_name=None, trial_number=None,
                                  params_trial=None, save_dir='./trial_conf/'):
        """
        save trial values as postgresql.conf
        """
        os.makedirs(save_dir, exist_ok=True)  # create directory
        trail_conf_filename = str(study_name) + '_#' + str(trial_number) + '_postgresql.conf'
        save_conf_path = os.path.join(save_dir, trail_conf_filename)
        signal = "# configurations recommended by optuna:\n"
        with open(save_conf_path, mode='w') as f:
            f.write(signal)
        with open(save_conf_path, mode='a') as f:
            for param_trial in params_trial:
                param_trial_name, param_trial_values = self._convert_trial_value_unit(param_trial)
                f.write('{} = \'{}\'\n'.format(param_trial_name, param_trial_values))
        return save_conf_path

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
                with get_pg_connection(dsn=get_pg_dsn(pghost=self.postgres_server_config.host,
                                                      pgport=self.postgres_server_config.port,
                                                      pguser=self.postgres_server_config.user,
                                                      pgpassword=self.postgres_server_config.password,
                                                      pgdatabase=self.postgres_server_config.database
                                                      )) as conn:
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
    def _convert_trial_value_unit(param_trial):
        """
        convert param trial value
        """
        param_name = param_trial['name']
        if param_trial['type'] == 'integer':
            param_values = int(param_trial['trial'])
        elif param_trial['type'] == 'float':
            param_values = float(param_trial['trial'])
        elif param_trial['type'] == 'bytes':
            param_values = format_bytes_str(param_trial['trial'])
        elif param_trial['type'] == 'time':
            param_values = format_milliseconds_str(param_trial['trial'])
        elif param_trial['type'] == 'boolean' or param_trial['type'] == 'enum':
            param_values = str(param_trial['trial'])
        else:
            raise Exception('parameter Type does not support')
        return param_name, param_values

    @staticmethod
    def load_json_parameters(tune_parameters_json_path):
        # load tuning parameters
        with open(tune_parameters_json_path, "r") as f:
            return json.load(f)

    @staticmethod
    def create_tune_parameter_json(host, major_version, params_json_dir='./conf', estimate_max_wal_size=None):
        hardware = HardwareResource(host=host)
        tune_parameter_json_path = '{}/version-{}.json'.format(params_json_dir, major_version)
        tune_parameter_json_backup_path = '{}/version-{}.json.org'.format(params_json_dir, major_version)
        tune_parameters = Parameter.load_json_parameters(tune_parameter_json_path)

        for index, tune_parameter in enumerate(tune_parameters):
            if Parameter.check_parameter_maxvalue_depend_memory_size(tune_parameter['name']):
                tune_parameters[index]['tuning_range']['maxval'] = format_bytes_str(hardware.memory_size * 0.75,
                                                                                    precision=0)
            elif Parameter.check_parameter_maxvalue_depend_cpu(tune_parameter['name']):
                tune_parameters[index]['tuning_range']['maxval'] = int(hardware.cpu_count)

            elif (estimate_max_wal_size is not None) and (tune_parameter['name'] == 'max_wal_size'):
                tune_parameters[index]['tuning_range']['minval'] = estimate_max_wal_size
                tune_parameters[index]['tuning_range']['maxval'] = estimate_max_wal_size

            elif (estimate_max_wal_size is not None) and (tune_parameter['name'] == 'checkpoint_timeout'):
                # Set the wal_max_size parameter to control check pointing,
                # so that check pointing due to timeouts should not occur.
                tune_parameters[index]['tuning_range']['minval'] = '1d'
                tune_parameters[index]['tuning_range']['maxval'] = '1d'

        shutil.copyfile(tune_parameter_json_path, tune_parameter_json_backup_path)  # backup
        with open(tune_parameter_json_path, 'w') as f:
            json.dump(tune_parameters, f, indent=2)

    @staticmethod
    def check_parameter_maxvalue_depend_memory_size(parameter_name):
        parameters_depend_memory_size = ['effective_cache_size', 'shared_buffers', 'temp_buffers']
        return parameter_name in parameters_depend_memory_size

    @staticmethod
    def check_parameter_maxvalue_depend_cpu(parameter_name):
        parameters_depend_cpu = ['max_worker_processes', 'max_parallel_workers',
                                 'max_parallel_workers_per_gather', 'max_parallel_maintenance_workers']
        return parameter_name in parameters_depend_cpu
