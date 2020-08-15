from logging import getLogger
from pgopttune.parameter.pg_tune_parameter import PostgresTuneParameter
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.tune_config import TuneConfig

logger = getLogger(__name__)


class Objective:
    def __init__(self,
                 postgres_server_config: PostgresServerConfig,
                 tune_config: TuneConfig
                 ):
        self.params = PostgresTuneParameter(postgres_server_config,  # turning parameter
                                            params_json_dir=tune_config.parameter_json_dir)
        self.data_load_interval = tune_config.data_load_interval
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
        objective_value = self.workload.run()  # benchmark run
        return objective_value

    def reset_param(self):
        # reset parameter value(reset postgresql.auto.conf)
        self.params.reset_param()

    def get_tune_param(self, trial, precision=2):
        trial_array = []  # trial tuning array
        for tune_parameter in self.params.tune_parameters:
            trial_dict = tune_parameter
            if tune_parameter['type'] == 'float':
                trial_dict['trial'] = round(trial.suggest_uniform(tune_parameter['name'],
                                                                  tune_parameter['tuning_range']['minval'],
                                                                  tune_parameter['tuning_range']['maxval']), precision)
            elif tune_parameter['type'] == 'enum' or tune_parameter['type'] == 'boolean':
                trial_dict['trial'] = trial.suggest_categorical(tune_parameter['name'],
                                                                tune_parameter['tuning_range'])
            else:
                trial_dict['trial'] = trial.suggest_int(tune_parameter['name'],
                                                        tune_parameter['tuning_range']['minval'],
                                                        tune_parameter['tuning_range']['maxval'])
            trial_array.append(trial_dict)
        logger.debug('trial_array : {}'.format(trial_array))
        return trial_array
