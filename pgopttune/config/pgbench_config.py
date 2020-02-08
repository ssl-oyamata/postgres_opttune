from pgopttune.config.config import Config


class PgbenchConfig(Config):
    def __init__(self, conf_path, section='pgbench'):
        super().__init__(conf_path)
        self.config_dict = dict(self.config.items(section))

    @property
    def scale_factor(self):
        return self.get_parameter_value('scale_factor')

    @property
    def clients(self):
        return self.get_parameter_value('clients')

    @property
    def evaluation_time(self):
        return self.get_parameter_value('evaluation_time')
