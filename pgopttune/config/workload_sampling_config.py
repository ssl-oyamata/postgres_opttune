from pgopttune.config.config import Config
from pgopttune.utils.pg_connect import get_pg_dsn


class WorkloadSamplingConfig(Config):
    def __init__(self, conf_path, section='workload-sampling'):
        super().__init__(conf_path)
        self.config_dict = dict(self.config.items(section))

    @property
    def workload_sampling_time_second(self):
        return float(self.get_parameter_value('workload_sampling_time_second'))

    @property
    def my_workload_save_dir(self):
        return self.get_parameter_value('my_workload_save_dir')

    @property
    def dsn(self):
        return get_pg_dsn(pghost=self.get_parameter_value('pghost'),
                          pgport=self.get_parameter_value('pgport'),
                          pgdatabase=self.get_parameter_value('pgdatabase'),
                          pguser=self.get_parameter_value('pguser'),
                          pgpassword=self.get_parameter_value('pgpassword'))
