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
        self.workload_elapsed_time = 0
        os.environ['PGHOST'] = postgres_server_config.host
        os.environ['PGPORT'] = postgres_server_config.port
        os.environ['PGDATABASE'] = postgres_server_config.database
        os.environ['PGUSER'] = postgres_server_config.user
        os.environ['PGPASSWORD'] = postgres_server_config.password

    def data_load(self):
        cwd = os.getcwd()
        if self.my_workload_config.work_directory != "current_directory":
            os.chdir(self.my_workload_config.work_directory)
        data_load_cmd = self.my_workload_config.data_load_command
        logger.debug('run my workload data load command : {}'.format(data_load_cmd))
        run_command(data_load_cmd)
        if self.my_workload_config.work_directory != "current_directory":
            os.chdir(cwd)

    def run(self, measurement_time_second: int = None):
        cwd = os.getcwd()
        if self.my_workload_config.work_directory != "current_directory":
            os.chdir(self.my_workload_config.work_directory)
        run_workload_command = self.my_workload_config.run_workload_command
        start_number_of_xact_commit = self.get_number_of_xact_commit()
        workload_start_time = time.time()  # start measurement time
        self.workload_elapsed_time = 0

        if measurement_time_second is not None:
            workload_load_count = 0
            while measurement_time_second > self.workload_elapsed_time:
                run_command(run_workload_command)  # run workload
                self.workload_elapsed_time = time.time() - workload_start_time
                workload_load_count += 1
                logger.debug("workload_load_count: {}, workload_elapsed_time : {}s, ".
                             format(workload_load_count, round(self.workload_elapsed_time, 2)))
        else:
            run_command(run_workload_command)  # run workload
            self.workload_elapsed_time = time.time() - workload_start_time
        if self.my_workload_config.work_directory != "current_directory":
            os.chdir(cwd)
        time.sleep(1)  # default PGSTAT_STAT_INTERVAL(500ms)
        workload_number_of_xact_commit = self.get_number_of_xact_commit() - start_number_of_xact_commit
        # logger.info(workload_number_of_xact_commit)
        tps = self.calculate_transaction_per_second(workload_number_of_xact_commit, self.workload_elapsed_time)
        return tps

    @staticmethod
    def calculate_transaction_per_second(number_of_xact_commit, elapsed_seconds):
        return round(number_of_xact_commit / elapsed_seconds, 6)
