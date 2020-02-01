import os
import sys
import logging
import traceback
import subprocess
from .workload import Workload
from pgopttune.utils.command import run_command

logger = logging.getLogger(__name__)


class Pgbench(Workload):
    def __init__(self,
                 host='localhost',
                 port=5432,
                 user='postgres',
                 password='postgres12',
                 database='postgres',
                 pgdata='/var/lib/pgsql/12/data',
                 bin='/usr/pgsql-12/bin',
                 pg_os_user='postgres',
                 benchmark_scale_factor=10,
                 benchmark_evaluation_time=30,
                 benchmark_clients=75,
                 ):
        super().__init__(host=host, port=port, user=user, password=password, database=database,
                         pgdata=pgdata, bin=bin, pg_os_user=pg_os_user)
        self.scale_factor = benchmark_scale_factor
        self.evaluation_time = benchmark_evaluation_time
        self.clients = benchmark_clients
        os.environ['PGPASSWORD'] = password

    def data_load(self):
        data_load_cmd = "{}/pgbench -h {} -p {} -U {} {} -i -s {}".format(self.bin, self.host, self.port, self.user,
                                                                          self.database, self.scale_factor)
        logger.debug('run pgbench data load command : {}'.format(data_load_cmd))
        run_command(data_load_cmd)
        self.vacuum_database()  # vacuum analyze

    def run(self):
        grep_string = "excluding"
        run_cmd_str = "{}/pgbench -h {} -p {} -U {} {} -c {} -T {}".format(self.bin, self.host, self.port, self.user,
                                                                           self.database, self.clients,
                                                                           self.evaluation_time)
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
            logging.critical(traceback.format_exc())
            logging.info('Failed Command: {} '.format(run_cmd_str))
            sys.exit(1)
        return tps
