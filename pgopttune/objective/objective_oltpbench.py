import logging
from pgopttune.workload.oltpbench import Oltpbench
from pgopttune.objective.objective import Objective

logger = logging.getLogger(__name__)


class ObjectiveOltpbench(Objective):

    def __init__(self, postgres_server_config, tune_config, oltpbench_config):
        super().__init__(postgres_server_config, tune_config)
        self.workload = Oltpbench(postgres_server_config, oltpbench_config)
