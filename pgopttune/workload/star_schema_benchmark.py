import os
import time
import logging
from multiprocessing import Pool
from pgopttune.workload.workload import Workload
from pgopttune.utils.command import run_command
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.star_schema_benchmark_config import StarSchemaBenchmarkConfig
from pgopttune.utils.pg_connect import get_pg_dsn, get_pg_connection

logger = logging.getLogger(__name__)


class StarSchemaBenchmark(Workload):
    def __init__(self, postgres_server_config: PostgresServerConfig, ssb_config: StarSchemaBenchmarkConfig):
        super().__init__(postgres_server_config)
        self.ssb_config = ssb_config

    def data_load(self):
        self._create_table()  # create table
        self._truncate_table()  # truncate table
        self._data_file_generate()  # create data file
        self._load_data()  # data load
        self.vacuum_database()  # vacuum database

    def _create_table(self):
        for table_sql_filepath in self.ssb_config.table_sql_filepath_list:
            self.execute_sql_file(table_sql_filepath)

    def _truncate_table(self):
        self.execute_sql_file(self.ssb_config.truncate_sql_filepath)

    def _data_file_generate(self):
        cwd = os.getcwd()
        os.chdir(self.ssb_config.ssb_dbgen_path)
        run_cmd_str = "./dbgen -s {} -T a".format(self.ssb_config.scale_factor)
        run_command(run_cmd_str, stdout_devnull=True)
        os.chdir(cwd)
        self._change_data_file_permission()
        self._create_data_file_symlink()

    def _change_data_file_permission(self):
        for table in self.ssb_config.table_list:
            data_filename = table + ".tbl"
            data_filepath = os.path.join(self.ssb_config.ssb_dbgen_path, data_filename)
            run_cmd_str = "chown -h {}:{} {}".format(self.postgres_server_config.os_user,
                                                     self.postgres_server_config.os_user, data_filepath)
            run_command(run_cmd_str, stdout_devnull=True)

    def _create_data_file_symlink(self):
        dest_dir = "/tmp"
        for table in self.ssb_config.table_list:
            data_filename = table + ".tbl"
            data_filepath = os.path.join(self.ssb_config.ssb_dbgen_path, data_filename)
            data_symlink_path = os.path.join(dest_dir, data_filename)
            if os.path.islink(data_symlink_path):
                os.unlink(data_symlink_path)
            os.symlink(data_filepath, data_symlink_path)
            run_cmd_str = "chown {}:{} {}".format(self.postgres_server_config.os_user,
                                                  self.postgres_server_config.os_user, data_symlink_path)
            run_command(run_cmd_str, stdout_devnull=True)

    def _load_data(self):
        if self.postgres_server_config.host == '127.0.0.1' or self.postgres_server_config.host == 'localhost':
            load_sql_filepath = os.path.join(self.ssb_config.sql_file_path, "load.sql")
            self.execute_sql_file(load_sql_filepath)
        else:
            for table_name in self.ssb_config.table_list:
                data_filepath = os.path.join("/tmp", table_name + ".tbl")
                with get_pg_connection(dsn=get_pg_dsn(pghost=self.postgres_server_config.host,
                                                      pgport=self.postgres_server_config.port,
                                                      pguser=self.postgres_server_config.user,
                                                      pgpassword=self.postgres_server_config.password,
                                                      pgdatabase=self.postgres_server_config.database
                                                      )) as conn:
                    conn.set_session(autocommit=True)
                    with conn.cursor() as cur:
                        with open(data_filepath) as f:
                            cur.copy_from(f, table_name, sep='|')

    def run(self):
        transaction_tasks = []
        # cache warm up
        logger.debug("Start SSB cache warm up.")
        self._run_transaction(self.ssb_config.workload_sql_filepath_list)
        logger.debug("Finish SSB cache warm up.")
        for _ in range(self.ssb_config.clients):
            transaction_tasks.append(self.ssb_config.workload_sql_filepath_list)
        start = time.time()
        with Pool(processes=self.ssb_config.clients) as pool:
            pool.map(self._run_transaction, transaction_tasks)
        elapsed_time = time.time() - start
        # Calculate the TPS.
        num_transaction = len(self.ssb_config.workload_sql_filepath_list) * self.ssb_config.clients
        tps = round(float(num_transaction) / float(elapsed_time), 5)
        return tps

    def _run_transaction(self, workload_sql_filepath_list):
        for sql_file_path in workload_sql_filepath_list:
            self.execute_sql_file(sql_file_path)  # run transaction


if __name__ == "__main__":
    from pgopttune.config.postgres_server_config import PostgresServerConfig
    from pgopttune.config.star_schema_benchmark_config import StarSchemaBenchmarkConfig

    conf_path = '../../conf/postgres_opttune.conf'
    postgres_server_config_test = PostgresServerConfig(conf_path)  # PostgreSQL Server config
    ssb_config_test = StarSchemaBenchmarkConfig(conf_path)
    ssb = StarSchemaBenchmark(postgres_server_config_test, ssb_config_test)
    ssb.data_load()
    print(ssb.run())
