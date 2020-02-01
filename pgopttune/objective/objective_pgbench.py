import logging
from pgopttune.workload.pgbench import Pgbench
from pgopttune.objective.objective import Objective

logger = logging.getLogger(__name__)


class ObjectivePgbench(Objective):

    def __init__(
            self,
            pgdata='/var/lib/pgsql/12/data',
            major_version=12,
            pghost='localhost',
            pgport=5432,
            pguser='postgres',
            pgpassword='postgres12',
            pgdatabase='postgres',
            pgbin='/usr/pgsql-12/bin',
            pg_os_user='postgres',
            ssh_port=22,
            ssh_password='postgres',
            params_json_dir='./conf',
            benchmark_scale_factor=10,
            benchmark_evaluation_time=30,
            benchmark_clients=75,
            data_load_interval=1,
    ):
        super().__init__(pgdata=pgdata,
                         major_version=major_version,
                         pghost=pghost,
                         pgport=pgport,
                         pguser=pguser,
                         pgdatabase=pgdatabase,
                         pgbin=pgbin,
                         pg_os_user=pg_os_user,
                         pgpassword=pgpassword,
                         ssh_port=ssh_port,
                         ssh_password=ssh_password,
                         params_json_dir=params_json_dir,
                         data_load_interval=data_load_interval)

        self.workload = Pgbench(host=pghost,
                                port=pgport,
                                user=pguser,
                                password=pgpassword,
                                database=pgdatabase,
                                pgdata=pgdata,
                                bin=pgbin,
                                pg_os_user=pg_os_user,
                                benchmark_scale_factor=benchmark_scale_factor,
                                benchmark_evaluation_time=benchmark_evaluation_time,
                                benchmark_clients=benchmark_clients,
                                )
