import time
import logging
import datetime
from pgopttune.log.pg_csv_log import PostgresCsvLog
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.workload_sampling_config import WorkloadSamplingConfig
from pgopttune.workload.my_workload import MyWorkload

logger = logging.getLogger(__name__)


class WorkloadSampler:
    def __init__(self,
                 postgres_server_config: PostgresServerConfig,
                 workload_sampling_config: WorkloadSamplingConfig):
        self.postgres_server_config = postgres_server_config
        self.workload_sampling_config = workload_sampling_config
        self.csv_log = PostgresCsvLog(postgres_server_config)

    def save(self):
        logger.info("Start recording the executed SQL in the log file.")
        logger.info("Sampling time : {} s".format(self.workload_sampling_config.workload_sampling_time_second))
        self.csv_log.enable()   # enable csv log
        csv_log_start_time = time.time()
        logger.info("Sampling start time : {}".format(datetime.datetime.fromtimestamp(csv_log_start_time)))
        csv_log_end_time = time.time() + self.workload_sampling_config.workload_sampling_time_second
        time.sleep(self.workload_sampling_config.workload_sampling_time_second)
        logger.info("Sampling Stop time : {}".format(datetime.datetime.fromtimestamp(csv_log_end_time)))
        logger.info("Stop recording the executed SQL in the log file.")
        self.csv_log.disable()  # end csv log
        logger.debug("Start importing the CSV file(saved executed SQL) into the table.")
        self.csv_log.load_csv_to_database(copy_dir=self.workload_sampling_config.my_workload_save_dir,
                                          dsn=self.workload_sampling_config.dsn)
        my_workload = MyWorkload(start_unix_time=csv_log_start_time, end_unix_time=csv_log_end_time,
                                 postgres_server_config=self.postgres_server_config,
                                 workload_sampling_config=self.workload_sampling_config)
        save_file = my_workload.save_my_workload()
        logger.info("The workload has been recorded in '{}'".format(save_file))
        return save_file


if __name__ == "__main__":
    from pgopttune.config.postgres_server_config import PostgresServerConfig

    logging.basicConfig(level=logging.DEBUG)
    conf_path = './conf/postgres_opttune.conf'
    postgres_server_config_test = PostgresServerConfig(conf_path)  # PostgreSQL Server config
    workload_sampling_config_test = WorkloadSamplingConfig(conf_path)
    workload_sampler_test = WorkloadSampler(postgres_server_config_test, workload_sampling_config_test)
    workload_sampler_test.save()
