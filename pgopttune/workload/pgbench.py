import os
import sys
from logging import getLogger
import traceback
import subprocess
from .workload import Workload
from pgopttune.utils.command import run_command
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.pgbench_config import PgbenchConfig

logger = getLogger(__name__)


class Pgbench(Workload):
    def __init__(self, postgres_server_config: PostgresServerConfig, pgbench_config: PgbenchConfig):
        super().__init__(postgres_server_config)
        self.pgbench_config = pgbench_config
        self.backup_database_prefix = 'pgbench_backup_'
        os.environ['PGPASSWORD'] = postgres_server_config.password

    def data_load(self):
        data_load_cmd = "{}/pgbench -h {} -p {} -U {} {} -i -s {}".format(self.postgres_server_config.pgbin,
                                                                          self.postgres_server_config.host,
                                                                          self.postgres_server_config.port,
                                                                          self.postgres_server_config.user,
                                                                          self.postgres_server_config.database,
                                                                          self.pgbench_config.scale_factor)
        logger.debug('run pgbench data load command : {}'.format(data_load_cmd))
        run_command(data_load_cmd)

    def run(self, measurement_time_second: int = None):
        grep_string = "excluding"
        if measurement_time_second is None:
            measurement_time_second = self.pgbench_config.evaluation_time

        run_cmd_str = "{}/pgbench -h {} -p {} -U {} {} -c {} -T {}".format(self.postgres_server_config.pgbin,
                                                                           self.postgres_server_config.host,
                                                                           self.postgres_server_config.port,
                                                                           self.postgres_server_config.user,
                                                                           self.postgres_server_config.database,
                                                                           self.pgbench_config.clients,
                                                                           measurement_time_second)
        run_cmd = run_cmd_str.split()
        grep_cmd = "grep {}".format(grep_string)
        grep_cmd = grep_cmd.split()
        cut_cmd = ["cut", "-d", " ", "-f", "3"]
        logger.debug('run pgbench command : {}'.format(run_cmd_str))

        try:
            with open(os.devnull, 'w') as devnull:
                run_res = subprocess.Popen(run_cmd, stdout=subprocess.PIPE, stderr=devnull)
            grep_res = subprocess.Popen(grep_cmd, stdout=subprocess.PIPE, stdin=run_res.stdout)
            cut_res = subprocess.Popen(cut_cmd, stdout=subprocess.PIPE, stdin=grep_res.stdout)
            tps = float(cut_res.communicate()[0].decode('utf-8'))
        except ValueError:
            logger.critical(traceback.format_exc())
            logger.info('Failed Command: {} '.format(run_cmd_str))
            sys.exit(1)
        return tps
