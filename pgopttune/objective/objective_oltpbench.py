from logging import getLogger
from pgopttune.workload.oltpbench import Oltpbench
from pgopttune.objective.objective import Objective
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.tune_config import TuneConfig
from pgopttune.config.oltpbench_config import OltpbenchConfig

logger = getLogger(__name__)


class ObjectiveOltpbench(Objective):

    def __init__(self,
                 postgres_server_config: PostgresServerConfig,
                 tune_config: TuneConfig,
                 oltpbench_config: OltpbenchConfig):
        super().__init__(postgres_server_config, tune_config)
        self.workload = Oltpbench(postgres_server_config, oltpbench_config)
