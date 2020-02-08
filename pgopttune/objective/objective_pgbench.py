import logging
from pgopttune.workload.pgbench import Pgbench
from pgopttune.objective.objective import Objective

logger = logging.getLogger(__name__)


class ObjectivePgbench(Objective):

    def __init__(self, postgres_server_config, tune_config, pgbench_config):
        super().__init__(postgres_server_config, tune_config)
        self.workload = Pgbench(postgres_server_config, pgbench_config)
