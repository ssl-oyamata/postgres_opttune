from logging import getLogger
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.parameter.pg_parameter import PostgresParameter

logger = getLogger(__name__)


def reset_postgres_param(postgres_server_config: PostgresServerConfig):
    """
    When KeyboardInterrupt occurs,
    Reset PostgreSQL parameters and restart.
    """
    params = PostgresParameter(postgres_server_config)
    params.reset_database(is_free_cache=False)  # restart postgres
    params.reset_param()
    params.reset_database(is_free_cache=False)
