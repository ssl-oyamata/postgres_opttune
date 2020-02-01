import logging
from pgopttune.workload.oltpbench import Oltpbench
from pgopttune.objective.objective import Objective

logger = logging.getLogger(__name__)


class ObjectiveOltpbench(Objective):

    def __init__(
            self,
            pgdata='/var/lib/pgsql/12/data',
            major_version=12,
            pghost='localhost',
            pgport=5432,
            pguser='postgres',
            pgdatabase='postgres',
            pgpassword='postgres12',
            pgbin='/usr/pgsql-12/bin',
            pg_os_user='postgres',
            ssh_port=22,
            ssh_password='postgres',
            oltppath='/root/oltpbench',
            params_json_dir='./conf',
            benchmark_kind='tpcc',
            oltpbench_config_path='./conf/tpcc_config_postgres.xml',
            data_load_interval=1
    ):
        super().__init__(pgdata=pgdata,
                         major_version=major_version,
                         pghost=pghost,
                         pgport=pgport,
                         pguser=pguser,
                         pgpassword=pgpassword,
                         pgdatabase=pgdatabase,
                         pgbin=pgbin,
                         pg_os_user=pg_os_user,
                         ssh_port=ssh_port,
                         ssh_password=ssh_password,
                         params_json_dir=params_json_dir,
                         data_load_interval=data_load_interval)

        self.workload = Oltpbench(host=pghost,
                                  port=pgport,
                                  user=pguser,
                                  password=pgpassword,
                                  database=pgdatabase,
                                  pgdata=pgdata,
                                  bin=pgbin,
                                  pg_os_user=pg_os_user,
                                  oltppath=oltppath,
                                  bench=benchmark_kind,
                                  oltpbench_config_path=oltpbench_config_path)
