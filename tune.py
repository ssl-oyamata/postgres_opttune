import os
import sys
import optuna
import logging
from logging import config
from distutils.util import strtobool
from pgopttune.utils.logger import logging_dict
from pgopttune.sampler.sampler import get_sampler
from pgopttune.study.study import create_study
from pgopttune.objective.objective_pgbench import ObjectivePgbench
from pgopttune.objective.objective_oltpbench import ObjectiveOltpbench
from pgopttune.objective.objective_star_schema_benchmark import ObjectiveStarSchemaBenchmark
from pgopttune.objective.objective_my_workload import ObjectiveMyWorkload
from pgopttune.parameter.reset import reset_postgres_param
from pgopttune.parameter.pg_tune_parameter import PostgresTuneParameter
from pgopttune.recovery.pg_recovery import Recovery
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.tune_config import TuneConfig
from pgopttune.config.pgbench_config import PgbenchConfig
from pgopttune.config.oltpbench_config import OltpbenchConfig
from pgopttune.config.star_schema_benchmark_config import StarSchemaBenchmarkConfig
from pgopttune.config.my_workload_config import MyWorkloadConfig


def main(
        conf_path='./conf/postgres_opttune.conf'
):
    # read setting parameters
    postgres_server_config = PostgresServerConfig(conf_path)  # PostgreSQL Server config
    tune_config = TuneConfig(conf_path)  # Tuning config

    # logging
    logging.config.dictConfig(logging_dict(debug=strtobool(tune_config.debug)))
    logger = logging.getLogger(__name__)
    optuna.logging.enable_propagation()  # Propagate logs to the root logger.
    optuna.logging.disable_default_handler()  # Stop showing logs in sys.stderr.

    # Estimate the wal_max_size based on the recovery time allowed.
    if int(tune_config.required_recovery_time_second) != 0:
        logger.info('Start to estimate the wal_max_size parameter. required_recovery_time_second = "{}s"'.format(
            tune_config.required_recovery_time_second))
        recovery = Recovery(postgres_server_config,
                            tune_config.required_recovery_time_second)
        estimate_max_wal_size_mb = recovery.estimate_max_wal_size()
    else:
        estimate_max_wal_size_mb = None

    # create tune parameter json
    # path : ./conf/version-<major-version>.json
    PostgresTuneParameter.create_tune_parameter_json(postgres_server_config.host,
                                                     postgres_server_config.major_version,
                                                     params_json_dir=tune_config.parameter_json_dir,
                                                     estimate_max_wal_size=estimate_max_wal_size_mb)

    logger.info('Run benchmark : {}'.format(tune_config.benchmark))
    # pgbench
    if tune_config.benchmark == 'pgbench':
        pgbench_config = PgbenchConfig(conf_path)  # pgbench config
        objective = ObjectivePgbench(postgres_server_config, tune_config, pgbench_config)
    # oltpbench
    elif tune_config.benchmark == 'oltpbench':
        oltpbench_config = OltpbenchConfig(conf_path)  # oltpbench config
        objective = ObjectiveOltpbench(postgres_server_config, tune_config, oltpbench_config)
    # star schema benchmark
    elif tune_config.benchmark == 'star_schema_benchmark':
        star_schema_benchmark_config = StarSchemaBenchmarkConfig(conf_path)  # star schema benchmark config
        objective = ObjectiveStarSchemaBenchmark(postgres_server_config, tune_config, star_schema_benchmark_config)
    # my workload (save using sampling_workload.py)
    elif tune_config.benchmark == 'my_workload':
        my_workload_config = MyWorkloadConfig(conf_path)  # my worklod config config
        objective = ObjectiveMyWorkload(postgres_server_config, tune_config, my_workload_config)
    else:
        raise NotImplementedError('This benchmark tool is not supported at this time.')

    cwd = os.getcwd()  # save current directory
    # tuning using optuna
    try:
        sampler = get_sampler(tune_config.sample_mode)  # sampler setting
        if tune_config.benchmark == 'my_workload':
            logger.info("The purpose of optimization is to minimize the total SQL execution time")
            study = create_study(study_name=tune_config.study_name,  # create study
                                 sampler=sampler,
                                 save_study_history=strtobool(tune_config.save_study_history),
                                 load_study_history=strtobool(tune_config.load_study_history),
                                 direction='minimize',
                                 history_database_url=tune_config.history_database_url)
        else:
            logger.info("The purpose of optimization is to maximize TPS")
            study = create_study(study_name=tune_config.study_name,  # create study
                                 sampler=sampler,
                                 save_study_history=strtobool(tune_config.save_study_history),
                                 load_study_history=strtobool(tune_config.load_study_history),
                                 direction='maximize',
                                 history_database_url=tune_config.history_database_url)

        study.optimize(objective, n_trials=int(tune_config.number_trail))  # optimize
    except KeyboardInterrupt:
        logger.critical('Keyboard Interrupt.')
        os.chdir(cwd)
        # Resetting parameters and restart
        reset_postgres_param(postgres_server_config)
        logger.info('Resetting PostgreSQL parameters, PostgreSQL restart completed.')
        sys.exit(1)
    logger.info('best trial : #{} \n'
                'best param : {}'.format(study.best_trial.number, study.best_params))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='PostgreSQL parameter tuning using machine learning.',
                                     usage='%(prog)s [options]')
    parser.add_argument('-f', '--config_path', type=str, default='./conf/postgres_opttune.conf',
                        help='postgres opttune conf file path')
    args = parser.parse_args()
    main(conf_path=args.config_path)
