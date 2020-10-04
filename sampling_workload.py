import logging
from logging import config
from distutils.util import strtobool
from pgopttune.utils.logger import logging_dict
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.tune_config import TuneConfig
from pgopttune.config.workload_sampling_config import WorkloadSamplingConfig
from pgopttune.workload.workload_sampler import WorkloadSampler


def main(
        conf_path='./conf/postgres_opttune.conf'
):
    # read setting parameters
    postgres_server_config = PostgresServerConfig(conf_path)  # PostgreSQL Server config
    tune_config = TuneConfig(conf_path)  # Tuning config(only use debug parameter)
    workload_sampling_config_test = WorkloadSamplingConfig(conf_path)  # Workload Sampling Config

    # logging
    logging.config.dictConfig(logging_dict(debug=strtobool(tune_config.debug)))
    logger = logging.getLogger(__name__)

    # workload sampling
    workload_sampler = WorkloadSampler(postgres_server_config, workload_sampling_config_test)
    workload_save_file_path = workload_sampler.save()
    logger.info("Workload sampling is complete.\n"
                "Workload save file: {}".format(workload_save_file_path))
    logger.info(
        "You can automatically tune the saved workload by setting the following in'./conf/postgres_opttune.conf'.\n"
        "[turning]\n"
        "benchmark = sampled_workload \n"
        ":\n"
        "[sampled-workload]\n"
        "sampled_workload_save_file = {}".format(workload_save_file_path))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Sample workload to PostgreSQL database and save to file.',
                                     usage='%(prog)s [options]')
    parser.add_argument('-f', '--config_path', type=str, default='./conf/postgres_opttune.conf',
                        help='postgres opttune conf file path')
    args = parser.parse_args()
    main(conf_path=args.config_path)
