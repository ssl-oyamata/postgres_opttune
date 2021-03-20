from pgopttune.config.config import Config


class MyWorkloadConfig(Config):
    def __init__(self, conf_path, section='my-workload'):
        super().__init__(conf_path)
        self.config_dict = dict(self.config.items(section))

    @property
    def work_directory(self):
        return self.get_parameter_value('work_directory')

    @property
    def data_load_command(self):
        return self.get_parameter_value('data_load_command')

    @property
    def warm_up_command(self):
        return self.get_parameter_value('warm_up_command')

    @property
    def run_workload_command(self):
        return self.get_parameter_value('run_workload_command')
