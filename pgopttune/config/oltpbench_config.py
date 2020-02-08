from pgopttune.config.config import Config


class OltpbenchConfig(Config):
    def __init__(self, conf_path, section='oltpbench'):
        super().__init__(conf_path)
        self.config_dict = dict(self.config.items(section))

    @property
    def oltpbench_path(self):
        return self.get_parameter_value('oltpbench_path')

    @property
    def benchmark_kind(self):
        return self.get_parameter_value('benchmark_kind')

    @property
    def oltpbench_config_path(self):
        return self.get_parameter_value('oltpbench_config_path')
