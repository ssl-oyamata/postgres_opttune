import logging
from distutils.util import strtobool
from pgopttune.parameter.pg_parameter import Parameter

logger = logging.getLogger(__name__)


def reset_postgres_param(config):
    """
    When KeyboardInterrupt occurs,
    Reset PostgreSQL parameters and restart.
    """
    params = Parameter(
        pgdata=config['PostgreSQL']['pgdata'],
        major_version=int(config['PostgreSQL']['major_version']),
        pghost=config['PostgreSQL']['pghost'],
        pgport=int(config['PostgreSQL']['pgport']),
        pguser=config['PostgreSQL']['pguser'],
        pgpassword=config['PostgreSQL']['pgpassword'],
        pgdatabase=config['PostgreSQL']['pgdatabase'],
        pgbin=config['PostgreSQL']['pgbin'],
        pg_os_user=config['PostgreSQL']['pg_os_user'],
        ssh_port=config['PostgreSQL']['ssh_port'],
        ssh_password=config['PostgreSQL']['ssh_password'],
        params_json_dir=config['turning']['parameter_json_dir']
    )
    params.reset_database(is_free_cache=False)  # restart postgres
    params.reset_param()
    params.reset_database(is_free_cache=False)