import logging
import optuna
from pgopttune.parameter.pg_parameter import Parameter

logger = logging.getLogger(__name__)


class Objective:
    def __init__(self,
                 pgdata='/var/lib/pgsql/12/data',
                 major_version=12,
                 pghost='localhost',
                 pgport=5432,
                 pguser='postgres',
                 pgpassword='postgres12',
                 pgdatabase='postgres',
                 pgbin='/usr/pgsql-12/bin',
                 pg_os_user='postgres',
                 ssh_port=22,
                 ssh_password='postgres',
                 params_json_dir='./conf',
                 data_load_interval=1
                 ):
        self.params = Parameter(pgdata=pgdata,  # turning parameter
                                pghost=pghost,
                                pgport=pgport,
                                pguser=pguser,
                                pgpassword=pgpassword,
                                pgdatabase=pgdatabase,
                                pgbin=pgbin,
                                pg_os_user=pg_os_user,
                                ssh_port=ssh_port,
                                ssh_password=ssh_password,
                                major_version=major_version,
                                params_json_dir=params_json_dir)
        self.data_load_interval = data_load_interval
        self.workload = None

    def __call__(self, trial):

        self.change_tune_param(trial)  # change parameter value(trial values)
        tps = self.run_workload(trial)  # run workload
        self.reset_param()  # reset parameter value

        return tps

    def change_tune_param(self, trial):
        trial_array = self.get_tune_param(trial)  # parameter tuning
        self.params.change_param_to_trial_values(trial_array)  # change parameters to trial values using ALTER SYSTEM
        trial_conf_path = self.params.save_trial_values_as_conf(study_name=trial.study.study_name,
                                                                trial_number=trial.number,
                                                                params_trial=trial_array, save_dir='./trial_conf/')
        # save trial param values as postgresql.conf
        logger.info('trail#{} conf saved : {}'.format(trial.number, trial_conf_path))

    def run_workload(self, trial):
        if (int(trial.number) == 0) or (int(trial.number) % self.data_load_interval == 0):
            self.workload.data_load()  # data load
        self.params.reset_database()  # cache free and database restart
        tps = self.workload.run()  # benchmark run
        return tps

    def reset_param(self):
        # reset parameter value(reset postgresql.auto.conf)
        self.params.reset_param()

    def get_tune_param(self, trial):
        trial_array = []  # trial tuning array
        for tune_parameter in self.params.tune_parameters:
            trial_dict = tune_parameter
            if tune_parameter['type'] == 'float':
                trial_dict['trial'] = trial.suggest_uniform(tune_parameter['name'],
                                                            tune_parameter['tuning_range']['minval'],
                                                            tune_parameter['tuning_range']['maxval'])
            elif tune_parameter['type'] == 'enum' or tune_parameter['type'] == 'boolean':
                trial_dict['trial'] = trial.suggest_categorical(tune_parameter['name'],
                                                                tune_parameter['tuning_range'])
            else:
                trial_dict['trial'] = trial.suggest_int(tune_parameter['name'],
                                                        tune_parameter['tuning_range']['minval'],
                                                        tune_parameter['tuning_range']['maxval'])
            trial_array.append(trial_dict)
        logging.debug('trial_array : {}'.format(trial_array))
        return trial_array
