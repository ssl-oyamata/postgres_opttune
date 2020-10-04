from logging import getLogger
from pgopttune.workload.sampled_workload import SampledWorkload
from pgopttune.objective.objective import Objective
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.tune_config import TuneConfig
from pgopttune.config.sampled_workload_config import SampledWorkloadConfig

logger = getLogger(__name__)


class ObjectiveSampledWorkload(Objective):

    def __init__(self,
                 postgres_server_config: PostgresServerConfig,
                 tune_config: TuneConfig,
                 sampled_workload_config: SampledWorkloadConfig):
        super().__init__(postgres_server_config, tune_config)
        self.workload = SampledWorkload.load_sampled_workload(sampled_workload_config.my_workload_save_file,
                                                              postgres_server_config=postgres_server_config)
