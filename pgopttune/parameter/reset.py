import logging
from distutils.util import strtobool
from pgopttune.parameter.pg_parameter import Parameter

logger = logging.getLogger(__name__)


def reset_postgres_param(postgres_server_config):
    """
    When KeyboardInterrupt occurs,
    Reset PostgreSQL parameters and restart.
    """
    params = Parameter(postgres_server_config)
    params.reset_database(is_free_cache=False)  # restart postgres
    params.reset_param()
    params.reset_database(is_free_cache=False)
