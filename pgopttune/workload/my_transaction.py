import time
import logging
from pgopttune.utils.pg_connect import get_pg_connection
from pgopttune.config.postgres_server_config import PostgresServerConfig

logger = logging.getLogger(__name__)


class MyTransaction:
    def __init__(self, session_id: str, query_start_time: list, statement: list):
        self.session_id = session_id
        self.query_start_time = query_start_time
        self.statement = statement

    def run(self, postgres_server_config: PostgresServerConfig):
        start_time = time.time()
        elapsed_times = 0

        # sleep until first statement start
        self._sleep_until_statement_start_time(start_time, self.query_start_time[0])

        with get_pg_connection(dsn=postgres_server_config.dsn) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                for index in range(len(self.query_start_time)):
                    self._sleep_until_statement_start_time(start_time, self.query_start_time[index])
                    query_start_time = time.time()
                    if "vacuum" in self.statement[index].lower():
                        cur.execute("END;")
                    cur.execute(self.statement[index])
                    # logger.info("Execute Statement : {}".format(self.statement[index]))
                    elapsed_times += (time.time() - query_start_time)
        return elapsed_times

    @staticmethod
    def _sleep_until_statement_start_time(start_time, query_start_time):
        while (time.time() - start_time) < query_start_time.total_seconds():
            sleep_time = query_start_time.total_seconds() - (time.time() - start_time)
            # logger.debug("sleep: {0:.4f}".format(sleep_time))
            if sleep_time > 1:
                time.sleep(sleep_time)
            else:
                time.sleep(0.1) # FIXME
