from logging import getLogger
from pgopttune.workload.pgbench import Pgbench
from pgopttune.objective.objective import Objective
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.tune_config import TuneConfig
from pgopttune.config.pgbench_config import PgbenchConfig

logger = getLogger(__name__)


class ObjectivePgbench(Objective):

    def __init__(self,
                 postgres_server_config: PostgresServerConfig,
                 tune_config: TuneConfig,
                 pgbench_config: PgbenchConfig):
        super().__init__(postgres_server_config, tune_config)
        self.workload = Pgbench(postgres_server_config, pgbench_config)
