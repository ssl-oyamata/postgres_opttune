import os
import sys
import logging
import traceback
import subprocess
from xml.etree.ElementTree import parse
from .workload import Workload
from pgopttune.utils.command import run_command
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.oltpbench_config import OltpbenchConfig

logger = logging.getLogger(__name__)


class Oltpbench(Workload):
    def __init__(self, postgres_server_config: PostgresServerConfig, oltpbench_config: OltpbenchConfig):
        super().__init__(postgres_server_config)
        self.oltpbench_config = oltpbench_config

    def data_load(self):
        cwd = os.getcwd()
        config_path = os.path.join(cwd, self.oltpbench_config.oltpbench_config_path)
        data_load_cmd = "{}/oltpbenchmark -b {} -c {} --create=true --load=true".format(
            self.oltpbench_config.oltpbench_path,
            self.oltpbench_config.benchmark_kind,
            config_path)
        os.chdir(self.oltpbench_config.oltpbench_path)
        logger.debug('run oltpbench data load command : {}'.format(data_load_cmd))
        run_command(data_load_cmd)
        os.chdir(cwd)
        self.vacuum_database()  # vacuum analyze

    def run(self, measurement_time_second: int = None):
        grep_string = "requests\/sec"
        cwd = os.getcwd()
        config_path = os.path.join(cwd, self.oltpbench_config.oltpbench_config_path)
        if measurement_time_second is not None:
            config_dir, config_filename = os.path.split(config_path)
            measurement_config_path = os.path.join(cwd, config_dir,
                                                   "measurement_test_" + str(measurement_time_second) + "s_"
                                                   + config_filename)
            tree = parse(config_path)  # read xml
            tree.find('works/work/time').text = str(measurement_time_second)
            tree.write(measurement_config_path, 'utf-8', True)
            config_path = measurement_config_path

        run_cmd_str = "{}/oltpbenchmark -b {} -c {} --execute=true -s 5". \
            format(self.oltpbench_config.oltpbench_path,
                   self.oltpbench_config.benchmark_kind,
                   config_path)
        os.chdir(self.oltpbench_config.oltpbench_path)
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
