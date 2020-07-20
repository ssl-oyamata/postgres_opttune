import time
import logging
from typing import Union
from multiprocessing import Process
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.oltpbench_config import OltpbenchConfig
from pgopttune.recovery.pg_recovery import Recovery
from pgopttune.workload.oltpbench import Oltpbench
from pgopttune.workload.pgbench import Pgbench

logger = logging.getLogger(__name__)


def check_relation_checkpoint_wal_size(
        postgres_server_config: PostgresServerConfig,
        workload: Union[Oltpbench, Pgbench],
        measurement_time_second=3600 * 5):
    recovery = Recovery(postgres_server_config, workload)

    # change checkpoint settings
    recovery.set_parameter(param_name='log_checkpoints', param_value='on')
    recovery.set_parameter(param_name='max_wal_size', param_value='13526MB')
    recovery.set_parameter(param_name='checkpoint_timeout', param_value='107min')
    recovery.set_parameter(param_name='max_connections', param_value='200')
    recovery.reset_database()  # restart
    # workload
    # workload.data_load()
    recovery.reset_database()  # restart
    p = Process(target=workload.run, args=(measurement_time_second,))
    p.start()

    # check wal_size from checkpoint
    recovery = Recovery(postgres_server_config, workload)
    start_time = time.time()
    elapsed_time = 0
    while measurement_time_second > elapsed_time:
        recovery_wal_size = recovery.get_recovery_wal_size()
        logger.info(
            "recovery_wal_size : {:.2f} B ({:.2f} MB)".format(recovery_wal_size, recovery_wal_size / 1024 / 1024))
        time.sleep(1)
        elapsed_time = time.time() - start_time
    p.join()


if __name__ == "__main__":
    fmt = "%(asctime)s %(levelname)s %(name)s :%(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt)
    conf_path = './conf/postgres_opttune.conf'
    postgres_server_config_test1 = PostgresServerConfig(conf_path)  # PostgreSQL Server config
    oltpbench_config_test1 = OltpbenchConfig(conf_path)
    workload_test = Oltpbench(postgres_server_config=postgres_server_config_test1,
                              oltpbench_config=oltpbench_config_test1)
    check_relation_checkpoint_wal_size(postgres_server_config_test1, workload_test)
