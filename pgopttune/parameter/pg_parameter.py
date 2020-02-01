import os
import json
import time
import logging
import psycopg2
from pgopttune.utils.unit import get_param_raw
from pgopttune.utils.remote_command import SSHCommandExecutor
from pgopttune.utils.command import run_command
from pgopttune.utils.pg_connect import get_pg_dsn, get_pg_connection

logger = logging.getLogger(__name__)


class Parameter:
    def __init__(self, major_version=12,
                 pgdata='/var/lib/pgsql/12/data/',
                 pghost='localhost',
                 pgport=5432,
                 pguser='postgres',
                 pgpassword='postges12',
                 pgdatabase='postgres',
                 pgbin='/usr/pgsql-12/bin',
                 pg_os_user='postgres',
                 ssh_port=22,
                 ssh_password='postgres',
                 params_json_dir='./conf'
                 ):
        self.pgdata = pgdata
        self.user = pguser
        self.password = pgpassword
        self.host = pghost
        self.port = pgport
        self.database = pgdatabase
        self.bin = pgbin
        self.ssh_port = ssh_port
        self.pg_os_user = pg_os_user
        self.ssh_password = ssh_password
        self.config_path = os.path.join(self.pgdata, 'postgresql.conf')
        self.tune_params_path = '{}/version-{}.json'.format(params_json_dir, major_version)
        # check file
        if (self.host == '127.0.0.1' or self.host == 'localhost') and not os.path.exists(self.config_path):
            raise ValueError("postgresql.conf does not exist. path : {}".format(self.config_path))
        if not os.path.exists(self.tune_params_path):
            raise ValueError("tune paramer file does not exist. path : {}".format(self.tune_params_path))
        self.tune_parameters = self.raw_size_parameters()

    def load_json_parameters(self):
        # load tuning parameters
        with open(self.tune_params_path, "r") as f:
            return json.load(f)

    def raw_size_parameters(self):
        tune_parameters = self.load_json_parameters()
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
        with get_pg_connection(dsn=get_pg_dsn(pghost=self.host,
                                              pgport=self.port,
                                              pguser=self.user,
                                              pgpassword=self.password,
                                              pgdatabase=self.database
                                              )) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(alter_system_sql)

        # use psql(old version)
        # alter_system_cmd = 'sudo -i -u {} {}/psql -h {} -U {} -d {} -Atqc "{}"' \
        #    .format(self.pg_os_user, self.bin, self.host, self.user, self.database, alter_system_sql)
        # run_command(alter_system_cmd)

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
            with get_pg_connection(dsn=get_pg_dsn(pghost=self.host,
                                                  pgport=self.port,
                                                  pguser=self.user,
                                                  pgpassword=self.password,
                                                  pgdatabase=self.database
                                                  )) as conn:
                conn.set_session(autocommit=True)
                with conn.cursor() as cur:
                    cur.execute(alter_system_sql)

            # use psql(old version)
            # alter_system_cmd = 'sudo -i -u {} {}/psql -h {} -U {} -d {} -Atqc "{}"' \
            #     .format(self.pg_os_user, self.bin, self.host, self.user, self.database, alter_system_sql)
            # run_command(alter_system_cmd)

    def change_conf_to_trial_values(self, params_trial=None):
        """
        change tune.conf using trial values(not using)
        """
        tune_setting_path = os.path.join(self.pgdata, 'conf.d/tune.conf')
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
                f.write('{} = {}\n'.format(param_trial_name, param_trial_values))
        return save_conf_path

    def reset_database(self, is_free_cache=True):
        self._restart_database()
        self._wait_startup_database()
        if is_free_cache:
            self._free_cache()

    def _restart_database(self):
        restart_database_cmd = 'sudo -i -u {} {}/pg_ctl -D {} -w -t 600 restart'.format(self.pg_os_user, self.bin,
                                                                                        self.pgdata)
        # localhost PostgreSQL
        if self.host == '127.0.0.1' or self.host == 'localhost':
            run_command(restart_database_cmd, stdout_devnull=True)
        # remote PostgreSQL
        else:
            restart_database_cmd = '{}/pg_ctl -D {} -w -t 600 restart'.format(self.bin, self.pgdata)
            ssh = SSHCommandExecutor(user=self.pg_os_user, password=self.ssh_password, hostname=self.host,
                                     port=self.ssh_port)
            ret = ssh.exec(restart_database_cmd)
            if not ret['retval'] == 0:
                raise ValueError('PostgreSQL restart failed.\n'
                                 'Restart command : {}'.format(restart_database_cmd))

    def _wait_startup_database(self, max_retry=300, sleep_time=2):
        is_in_recovery_sql = 'SELECT pg_is_in_recovery()'

        # use psycopg2
        for i in range(max_retry + 1):
            try:
                with get_pg_connection(dsn=get_pg_dsn(pghost=self.host,
                                                      pgport=self.port,
                                                      pguser=self.user,
                                                      pgpassword=self.password,
                                                      pgdatabase=self.database
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

        # use psql(old version)
        # startup_database_cmd = '{}/psql -h {} -U {} -d {} -Atqc "SELECT pg_is_in_recovery()"' \
        #     .format(self.bin, self.host, self.user, self.database)
        # res = run_command(startup_database_cmd)
        # while res.returncode != 0:
        #     time.sleep(20)
        #     res = run_command(startup_database_cmd)

    def _free_cache(self):
        free_cache_cmd = 'sudo bash -c "sync"; sudo bash -c "echo 1 > /proc/sys/vm/drop_caches"'

        # localhost PostgreSQL
        if self.host == '127.0.0.1' or self.host == 'localhost':
            run_command(free_cache_cmd)
        else:
            ssh = SSHCommandExecutor(user=self.pg_os_user, password=self.ssh_password, hostname=self.host,
                                     port=self.ssh_port)
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
            # FIXME : Unit setting according to value
            param_values = int(param_trial['trial'] / (1024 ** 2))
            param_values = str(param_values) + 'MB'
        elif param_trial['type'] == 'time':
            # FIXME : Unit setting according to value
            if param_trial['trial'] >= 1000:
                param_values = int(param_trial['trial'] / (1000 ** 1))
                param_values = str(param_values) + 's'
            else:
                param_values = int(param_trial['trial'])
                param_values = str(param_values) + 'ms'
        elif param_trial['type'] == 'boolean' or param_trial['type'] == 'enum':
            param_values = str(param_trial['trial'])
        else:
            raise Exception('parameter Type does not support')
        return param_name, param_values

    def add_include_dir(self):
        """
        add parameter setting (include_dir = 'conf.d')
        FIXME : Deleted at a later date because it is an unused function
        """
        add_param = "include_dir = 'conf.d'"
        with open(self.config_path, 'a') as f:
            print(add_param, file=f)

        # mkdir conf.d
        include_dir = os.path.join(self.pgdata, 'conf.d')
        os.makedirs(include_dir, exist_ok=True)

        # change owner
        change_owner_cmd = 'sudo chown {}:{} {}'.format(self.pg_os_user, self.pg_os_user, include_dir)
        run_command(change_owner_cmd)

        # restart postgreSQL
        self._restart_postgres(self.pgdata)

    def _restart_postgres(self, pgdata='/var/lib/pgsql/12/data'):
        """
        Restart PostgreSQL.
        FIXME : Deleted at a later date because it is an unused function
        """
        # localhost PostgreSQL
        if self.host == '127.0.0.1' or self.host == 'localhost':
            restart_database_cmd = 'sudo -i -u {} {}/pg_ctl -D {} -w restart'.format(self.pg_os_user, self.bin, pgdata)
            run_command(restart_database_cmd)
        else:
            raise NotImplementedError('This function does not support remote PostgreSQL.')
