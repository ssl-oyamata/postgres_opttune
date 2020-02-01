import os
import sys
import logging
import traceback
import subprocess
from .workload import Workload
from pgopttune.utils.command import run_command

logger = logging.getLogger(__name__)


class Oltpbench(Workload):
    def __init__(self, host='localhost', port=5432, user='postgres', password='postgres12', database='postgres',
                 pgdata='/var/lib/pgsql/12/data', bin='/usr/pgsql-12/bin', pg_os_user='postgres',
                 oltppath='/root/oltpbench', bench='tpcc', oltpbench_config_path='./conf/tpcc_config_postgres.xml'):
        super().__init__(host=host, port=port, user=user, password=password, database=database,
                         pgdata=pgdata, bin=bin, pg_os_user=pg_os_user)
        self.oltppath = oltppath
        self.bench = bench
        self.oltpbench_config_path = oltpbench_config_path

    def data_load(self):
        cwd = os.getcwd()
        config_path = os.path.join(cwd, self.oltpbench_config_path)
        data_load_cmd = "{}/oltpbenchmark -b {} -c {} --create=true --load=true".format(self.oltppath,
                                                                                        self.bench,
                                                                                        config_path)
        os.chdir(self.oltppath)
        logger.debug('run oltpbench data load command : {}'.format(data_load_cmd))
        run_command(data_load_cmd)
        os.chdir(cwd)
        self.vacuum_database()  # vacuum analyze

    def run(self):
        grep_string = "requests\/sec"
        cwd = os.getcwd()
        config_path = os.path.join(cwd, self.oltpbench_config_path)
        run_cmd_str = "{}/oltpbenchmark -b {} -c {} --execute=true -s 5".format(self.oltppath, self.bench,
                                                                                config_path)
        os.chdir(self.oltppath)
        run_cmd = run_cmd_str.split()
        grep_cmd = "grep {}".format(grep_string)
        grep_cmd = grep_cmd.split()
        cut_cmd = ["cut", "-d", " ", "-f", "12"]

        logger.debug('run oltpbench command : {}'.format(run_cmd_str))
        try:
            with open(os.devnull, 'w') as devnull:
                run_res = subprocess.Popen(run_cmd, stdout=subprocess.PIPE, stderr=devnull)
            grep_res = subprocess.Popen(grep_cmd, stdout=subprocess.PIPE, stdin=run_res.stdout)
            cut_res = subprocess.Popen(cut_cmd, stdout=subprocess.PIPE, stdin=grep_res.stdout)  # tps
            os.chdir(cwd)
            tps = float(cut_res.communicate()[0].decode('utf-8'))
        except ValueError:
            logging.critical(traceback.format_exc())
            logging.info('Failed Command: {} '.format(run_cmd_str))
            sys.exit(1)
        return tps
