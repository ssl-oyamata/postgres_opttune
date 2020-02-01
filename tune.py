import os
import sys
import errno
import configparser
import optuna
import logging
from logging import config
from distutils.util import strtobool
from pgopttune.utils.logger import logging_dict
from pgopttune.sampler.sampler import get_sampler
from pgopttune.study.study import create_study
from pgopttune.objective.objective_pgbench import ObjectivePgbench
from pgopttune.objective.objective_oltpbench import ObjectiveOltpbench
from pgopttune.parameter.reset import reset_postgres_param


def main(
        conf_path='./conf/postgres_opttune.conf'
):
    # read setting parameters
    param_config = configparser.ConfigParser(inline_comment_prefixes=('#'))
    if not os.path.exists(conf_path):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), conf_path)
    else:
        param_config.read(conf_path, encoding='utf-8')

    # logging
    logging.config.dictConfig(logging_dict(debug=strtobool(param_config['turning']['debug'])))
    logger = logging.getLogger(__name__)
    optuna.logging.enable_propagation()  # Propagate logs to the root logger.

    # check parameter
    # if not config['PostgreSQL']['pghost'] == 'localhost':
    #     raise NotImplementedError('At the moment PostgreSQL needs to run on localhost.')

    logger.info('Run benchmark : {}'.format(param_config['turning']['benchmark']))
    if param_config['turning']['benchmark'] == 'pgbench':
        objective = ObjectivePgbench(
            pgdata=param_config['PostgreSQL']['pgdata'],
            major_version=int(param_config['PostgreSQL']['major_version']),
            pghost=param_config['PostgreSQL']['pghost'],
            pgport=int(param_config['PostgreSQL']['pgport']),
            pguser=param_config['PostgreSQL']['pguser'],
            pgpassword=param_config['PostgreSQL']['pgpassword'],
            pgbin=param_config['PostgreSQL']['pgbin'],
            pgdatabase=param_config['PostgreSQL']['pgdatabase'],
            pg_os_user=param_config['PostgreSQL']['pg_os_user'],
            ssh_port=param_config['PostgreSQL']['ssh_port'],
            ssh_password=param_config['PostgreSQL']['ssh_password'],
            params_json_dir=param_config['turning']['parameter_json_dir'],
            benchmark_scale_factor=int(param_config['pgbench']['scale_factor']),
            benchmark_evaluation_time=int(param_config['pgbench']['evaluation_time']),
            benchmark_clients=int(param_config['pgbench']['clients']),
            data_load_interval=int(param_config['turning']['data_load_interval'])
        )
    elif param_config['turning']['benchmark'] == 'oltpbench':
        objective = ObjectiveOltpbench(
            pgdata=param_config['PostgreSQL']['pgdata'],
            major_version=int(param_config['PostgreSQL']['major_version']),
            pghost=param_config['PostgreSQL']['pghost'],
            pgport=int(param_config['PostgreSQL']['pgport']),
            pguser=param_config['PostgreSQL']['pguser'],
            pgdatabase=param_config['PostgreSQL']['pgdatabase'],
            pgbin=param_config['PostgreSQL']['pgbin'],
            pg_os_user=param_config['PostgreSQL']['pg_os_user'],
            pgpassword=param_config['PostgreSQL']['pgpassword'],
            ssh_port=param_config['PostgreSQL']['ssh_port'],
            ssh_password=param_config['PostgreSQL']['ssh_password'],
            oltppath=param_config['oltpbench']['oltpbench_path'],
            params_json_dir=param_config['turning']['parameter_json_dir'],
            benchmark_kind=param_config['oltpbench']['benchmark_kind'],
            oltpbench_config_path=param_config['oltpbench']['oltpbench_config_path'],
            data_load_interval=int(param_config['turning']['data_load_interval'])
        )
    else:
        raise NotImplementedError('This benchmark tool is not supported at this time.')
    cwd = os.getcwd()  # save current directory
    try:
        sampler = get_sampler(param_config['turning']['sample_mode'])  # sampler setting
        study = create_study(study_name=param_config['turning']['study_name'],  # create study
                             sampler=sampler,
                             save_study_history=strtobool(param_config['turning']['save_study_history']),
                             load_study_history=strtobool(param_config['turning']['load_study_history']),
                             history_database_url=param_config['turning']['history_database_url'])
        study.optimize(objective, n_trials=int(param_config['turning']['number_trail']))  # optimize
    except KeyboardInterrupt:
        logger.critical('Keyboard Interrupt.')
        os.chdir(cwd)
        reset_postgres_param(param_config)  # Resetting parameters and restart
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
