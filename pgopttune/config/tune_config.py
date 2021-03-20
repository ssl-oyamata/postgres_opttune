from pgopttune.config.config import Config


class TuneConfig(Config):
    def __init__(self, conf_path, section='turning'):
        super().__init__(conf_path)
        self.config_dict = dict(self.config.items(section))

    @property
    def study_name(self):
        return self.get_parameter_value('study_name')

    @property
    def required_recovery_time_second(self):
        return self.get_parameter_value('required_recovery_time_second')

    @property
    def benchmark(self):
        return self.get_parameter_value('benchmark')

    @property
    def parameter_json_dir(self):
        return self.get_parameter_value('parameter_json_dir')

    @property
    def number_trail(self):
        return int(self.get_parameter_value('number_trail'))

    @property
    def data_load_interval(self):
        return int(self.get_parameter_value('data_load_interval'))

    @property
    def warm_up_interval(self):
        return int(self.get_parameter_value('warm_up_interval'))

    @property
    def sample_mode(self):
        return self.get_parameter_value('sample_mode')

    @property
    def debug(self):
        return self.get_parameter_value('debug')

    @property
    def save_study_history(self):
        return self.get_parameter_value('save_study_history')

    @property
    def load_study_history(self):
        return self.get_parameter_value('load_study_history')

    @property
    def history_database_url(self):
        return self.get_parameter_value('history_database_url')
