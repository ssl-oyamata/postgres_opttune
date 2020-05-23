import logging
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.tune_config import TuneConfig
from pgopttune.config.star_schema_benchmark_config import StarSchemaBenchmarkConfig
from pgopttune.workload.star_schema_benchmark import StarSchemaBenchmark
from pgopttune.objective.objective import Objective

logger = logging.getLogger(__name__)


class ObjectiveStarSchemaBenchmark(Objective):

    def __init__(self,
                 postgres_server_config: PostgresServerConfig,
                 tune_config: TuneConfig,
                 ssb_config: StarSchemaBenchmarkConfig):
        super().__init__(postgres_server_config, tune_config)
        self.workload = StarSchemaBenchmark(postgres_server_config, ssb_config)
