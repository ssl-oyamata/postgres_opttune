import os
from pgopttune.config.config import Config


class StarSchemaBenchmarkConfig(Config):
    def __init__(self, conf_path, section='star-schema-benchmark'):
        super().__init__(conf_path)
        self.config_dict = dict(self.config.items(section))
        self.table_list = ["customer", "date", "part", "supplier", "lineorder"]
        sql_key_list = self.sql_key.replace(' ', '').split(',')
        self.check_sql_key_length(sql_key_list)
        self.table_sql_filepath_list = self._create_table_sql_file()
        self.truncate_sql_filepath = self._truncate_table_sql_file()
        self.load_sql_filepath = self._create_load_sql_file()
        self.workload_sql_filepath_list = self._create_workload_sql_file_list(sql_key_list)

    def _create_table_sql_file(self):
        create_table_file_list = []
        for table in self.table_list:
            sql_filename = "create_" + table + "_table.sql"
            sql_filepath = os.path.join(self.sql_file_path, sql_filename)
            if not os.path.exists(sql_filepath):
                raise ValueError("File ({}) does not exist.Check the file.".format(sql_filepath))
            create_table_file_list.append(sql_filepath)
        return create_table_file_list

    def _create_load_sql_file(self):
        return os.path.join(self.sql_file_path, "load.sql")

    def _truncate_table_sql_file(self):
        return os.path.join(self.sql_file_path, "truncate.sql")

    def _create_workload_sql_file_list(self, sql_key_list):
        workload_sql_file_list = []
        for sql_key in sql_key_list:
            sql_filename = sql_key + ".sql"
            sql_file_path = os.path.join(self.sql_file_path, sql_filename)
            if not os.path.exists(sql_file_path):
                raise ValueError("File ({}) does not exist."
                                 "Check the sql_key parameter in postgres_opttune.conf.".format(sql_file_path))
            workload_sql_file_list.append(sql_file_path)
        return workload_sql_file_list

    @property
    def ssb_dbgen_path(self):
        return self.get_parameter_value('ssb_dbgen_path')

    @property
    def scale_factor(self):
        return int(self.get_parameter_value('scale_factor'))

    @property
    def clients(self):
        return int(self.get_parameter_value('clients'))

    @property
    def sql_file_path(self):
        return self.get_parameter_value('sql_file_path')

    @property
    def sql_key(self):
        return self.get_parameter_value('sql_key')

    @staticmethod
    def check_sql_key_length(sql_key_list):
        if len(sql_key_list) == 0:
            raise ValueError("sql_key parameter is not specified."
                             "Check the sql_key parameter in postgres_opttune.conf.")
