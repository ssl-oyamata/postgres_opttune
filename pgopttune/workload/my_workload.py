import os
from logging import getLogger
import datetime
import pickle
import multiprocessing
from psycopg2.extras import DictCursor
from pgopttune.workload.workload import Workload
from pgopttune.utils.pg_connect import get_pg_connection
from pgopttune.config.postgres_server_config import PostgresServerConfig
from pgopttune.config.workload_sampling_config import WorkloadSamplingConfig
from pgopttune.workload.my_transaction import MyTransaction

logger = getLogger(__name__)


class MyWorkload(Workload):
    def __init__(self,
                 postgres_server_config: PostgresServerConfig,
                 workload_sampling_config: WorkloadSamplingConfig,
                 start_unix_time, end_unix_time, my_transactions: list = None):
        super().__init__(postgres_server_config)
        self.workload_sampling_config = workload_sampling_config
        self.start_unix_time = start_unix_time
        self.end_unix_time = end_unix_time
        if my_transactions is None:
            self.my_transactions = []
            self.extract_workload()
        else:
            self.my_transactions = my_transactions

    def extract_workload(self):
        extract_workload_sql = '''
        SELECT
             -- log_time,
             -- query_stat_time = log_time - duration - start_unix_time
             (log_time::timestamp(3) with time zone - substring(message from '(?<=duration: ).*ms')::interval 
             - to_timestamp(%s)) AS query_stat_time,
             -- database_name,
             session_id,
             -- substring(message from '(?<=duration: ).*(?= ms)') AS duration,
             substring(message from '(?<=statement: ).*') AS statement
        FROM
             csv_log
        WHERE
             log_time > to_timestamp(%s) AND
             log_time <=  to_timestamp(%s) AND
             database_name = %s AND
             message LIKE '%%duration%%'
        ORDER BY session_id,
                 session_line_num;
                 -- log_time;
        '''
        with get_pg_connection(dsn=self.workload_sampling_config.dsn) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(extract_workload_sql, (self.start_unix_time, self.start_unix_time, self.end_unix_time,
                                                   self.postgres_server_config.database))
                workload_rows = cur.fetchall()
        # logger.debug("workload_rows : {}".format(workload_rows))
        self._create_transactions(workload_rows)

    def _create_transactions(self, workload_rows):
        query_stat_time = []
        session_id = ''
        statement = []
        for index, row in enumerate(workload_rows):
            if session_id == row[1] or index == 0:  # same session statement
                query_stat_time.append(row[0])
                session_id = row[1]
                statement.append(row[2])
            else:
                my_transaction = MyTransaction(session_id, query_stat_time, statement)
                self.my_transactions.append(my_transaction)
                query_stat_time = [row[0]]
                session_id = row[1]
                statement = [row[2]]

    def save_my_workload(self):
        save_file_name = datetime.datetime.fromtimestamp(self.start_unix_time).strftime("%Y-%m-%d_%H%M%S.%f") + \
                         "-" \
                         + datetime.datetime.fromtimestamp(self.end_unix_time).strftime("%Y-%m-%d_%H%M%S.%f") + ".pkl"
        save_file_path = os.path.join("workload_data", save_file_name)
        with open(save_file_path, 'wb') as f:
            pickle.dump(self, f)
        return save_file_path

    def run(self):
        session_num = len(self.my_transactions)  # number of session
        logger.debug("Number of session : {} ".format(session_num))
        with multiprocessing.Pool(session_num) as p:
            args = range(session_num)
            elapsed_times = p.map(self._run_transaction, args)
        logger.debug("Transactions elapsed times : {} ".format(elapsed_times))
        elapsed_time = sum(elapsed_times)
        #  single process execute #
        # for index, my_transaction in enumerate(self.my_transactions):
        #     logger.debug("Transaction's statement : {}".format(my_transaction.statement))
        #     transaction_elapsed_time = my_transaction.run(self._postgres_server_config)
        #     logger.debug("elapsed time : {0:.4f} s".format(transaction_elapsed_time))
        #     elapsed_time += transaction_elapsed_time
        # logger.debug("Transactions elapsed time(sum) : {0:.4f} s".format(elapsed_time))
        logger.debug("Transactions elapsed time(sum) : {0:.4f} s".format(elapsed_time))
        return elapsed_time

    @classmethod
    def load_my_workload(cls, load_file_path, postgres_server_config: PostgresServerConfig = None):
        with open(load_file_path, 'rb') as f:
            workload = pickle.load(f)
        if postgres_server_config is not None:
            workload.postgres_server_config = postgres_server_config
        return workload

    @staticmethod
    def data_load():
        # TODO:
        logger.warning("At this time, the data loading function to the sampled database is not implemented.")

    def _run_transaction(self, transaction_index=0):
        # logger.debug("Transaction's statement : {}".format(self.my_transactions[transaction_index].statement))
        transaction_elapsed_time = self.my_transactions[transaction_index].run(self.postgres_server_config)
        # logger.debug("elapsed time : {0:.4f} s".format(transaction_elapsed_time))
        return transaction_elapsed_time


if __name__ == "__main__":
    from pgopttune.config.postgres_server_config import PostgresServerConfig

    from logging import basicConfig, DEBUG

    basicConfig(level=DEBUG)
    conf_path = './conf/postgres_opttune.conf'
    postgres_server_config_test = PostgresServerConfig(conf_path)  # PostgreSQL Server config
    workload_sampling_config_test = WorkloadSamplingConfig(conf_path)
    my_workload = MyWorkload(start_unix_time=1593093506.9530554, end_unix_time=1593093567.088895,
                             workload_sampling_config=workload_sampling_config_test,
                             postgres_server_config=postgres_server_config_test)
    save_file = my_workload.save_my_workload()
    logger.debug("run transactions ")
    my_workload_elapsed_time = my_workload.run()
    logger.debug(my_workload_elapsed_time)
    load_workload = MyWorkload.load_my_workload(save_file, postgres_server_config=postgres_server_config_test)
    logger.debug("run transactions using saved file")
    load_workload_elapsed_time = load_workload.run()
    logger.debug(load_workload_elapsed_time)
    logger.debug("finised...")
    logger.debug(my_workload_elapsed_time)
    logger.debug(load_workload_elapsed_time)

    # my_workload.extract_workload()
