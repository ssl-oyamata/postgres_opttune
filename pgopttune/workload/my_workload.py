import os
import time
from logging import getLogger
from .workload import Workload
from pgopttune.utils.command import run_command
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.my_workload_config import MyWorkloadConfig

logger = getLogger(__name__)


class MyWorkLoad(Workload):
    def __init__(self, postgres_server_config: PostgresServerConfig, my_workload_config: MyWorkloadConfig):
        super().__init__(postgres_server_config)
        self.my_workload_config = my_workload_config
        self.backup_database_prefix = 'my_workload_backup_'
        os.environ['PGHOST'] = postgres_server_config.host
        os.environ['PGPORT'] = postgres_server_config.port
        os.environ['PGDATABASE'] = postgres_server_config.database
        os.environ['PGUSER'] = postgres_server_config.user
        os.environ['PGPASSWORD'] = postgres_server_config.password

    def data_load(self):
        data_load_cmd = self.my_workload_config.data_load_command
        logger.debug('run my workload data load command : {}'.format(data_load_cmd))
        run_command(data_load_cmd)

    def run(self):
        run_workload_command = self.my_workload_config.run_workload_command
        start_number_of_xact_commit = self.get_number_of_xact_commit()
        workload_start_time = time.time()  # start measurement time
        run_command(run_workload_command)  # run workload
        # command_result = run_command(run_workload_command)  # run workload
        # logger.info(command_result.stdout.decode("utf8"))
        workload_elapsed_times = time.time() - workload_start_time
        # logger.info(workload_elapsed_times)
        time.sleep(1)  # default PGSTAT_STAT_INTERVAL(500ms)
        workload_number_of_xact_commit = self.get_number_of_xact_commit() - start_number_of_xact_commit
        # logger.info(workload_number_of_xact_commit)
        tps = self.calculate_transaction_per_second(workload_number_of_xact_commit, workload_elapsed_times)
        # logger.info("tps : {}".format(tps))
        return tps

    @staticmethod
    def calculate_transaction_per_second(number_of_xact_commit, elapsed_seconds):
        return round(number_of_xact_commit / elapsed_seconds, 6)
