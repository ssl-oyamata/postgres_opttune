import os
import json
import shutil
from logging import getLogger
from pgopttune.resource.hardware import HardwareResource
from pgopttune.utils.unit import get_param_raw, format_bytes_str, format_milliseconds_str
from pgopttune.utils.pg_connect import get_pg_connection
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.parameter.pg_parameter import PostgresParameter

logger = getLogger(__name__)


class PostgresTuneParameter(PostgresParameter):
    def __init__(self, postgres_server_config: PostgresServerConfig, params_json_dir='./conf'):
        super().__init__(postgres_server_config)
        self.config_path = os.path.join(self.postgres_server_config.pgdata, 'postgresql.conf')
        self.tune_parameters_json_path = '{}/version-{}.json'.format(params_json_dir,
                                                                     postgres_server_config.major_version)
        # check file
        if (self.postgres_server_config.host == '127.0.0.1' or self.postgres_server_config.host == 'localhost') \
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
            with get_pg_connection(dsn=self.postgres_server_config.dsn) as conn:
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
    def create_tune_parameter_json(host, major_version, params_json_dir='./conf',
                                   estimate_max_wal_size=None, estimate_checkpoint_timeout=None):
        hardware = HardwareResource(host=host)
        tune_parameter_json_path = '{}/version-{}.json'.format(params_json_dir, major_version)
        tune_parameter_json_backup_path = '{}/version-{}.json.org'.format(params_json_dir, major_version)
        tune_parameters = PostgresTuneParameter.load_json_parameters(tune_parameter_json_path)

        for index, tune_parameter in enumerate(tune_parameters):
            if PostgresParameter.check_parameter_maxvalue_depend_memory_size(tune_parameter['name']):
                tune_parameters[index]['tuning_range']['maxval'] = format_bytes_str(hardware.memory_size * 0.75,
                                                                                    precision=0)
            elif PostgresParameter.check_parameter_maxvalue_depend_cpu(tune_parameter['name']):
                tune_parameters[index]['tuning_range']['maxval'] = int(hardware.cpu_count)

            elif (estimate_max_wal_size is not None) and (tune_parameter['name'] == 'max_wal_size'):
                tune_parameters[index]['tuning_range']['minval'] = estimate_max_wal_size
                tune_parameters[index]['tuning_range']['maxval'] = estimate_max_wal_size

            elif (estimate_checkpoint_timeout is not None) and (tune_parameter['name'] == 'checkpoint_timeout'):
                tune_parameters[index]['tuning_range']['minval'] = estimate_checkpoint_timeout
                tune_parameters[index]['tuning_range']['maxval'] = estimate_checkpoint_timeout

        shutil.copyfile(tune_parameter_json_path, tune_parameter_json_backup_path)  # backup
        with open(tune_parameter_json_path, 'w') as f:
            json.dump(tune_parameters, f, indent=2)
