from logging import getLogger
from pgopttune.workload.my_workload import MyWorkload
from pgopttune.objective.objective import Objective
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.tune_config import TuneConfig
from pgopttune.config.my_workload_config import MyWorkloadConfig

logger = getLogger(__name__)


class ObjectiveMyWorkload(Objective):

    def __init__(self,
                 postgres_server_config: PostgresServerConfig,
                 tune_config: TuneConfig,
                 my_workload_config: MyWorkloadConfig):
        super().__init__(postgres_server_config, tune_config)
        self.workload = MyWorkload.load_my_workload(my_workload_config.my_workload_save_file,
                                                    postgres_server_config=postgres_server_config)
