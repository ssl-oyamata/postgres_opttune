import os
from pgopttune.config.config import Config


class SampledWorkloadConfig(Config):
    def __init__(self, conf_path, section='sampled_workload'):
        super().__init__(conf_path)
        self.conf_path = conf_path
        self.config_dict = dict(self.config.items(section))
        self._check_is_exist_sampled_workload_save_file()

    def _check_is_exist_sampled_workload_save_file(self):
        if not os.path.exists(self.get_parameter_value('sampled_workload_save_file')):
            raise ValueError("{} does not exist."
                             "Check the sampled_workload_save_file parameter in {}."
                             .format(self.get_parameter_value('sampled_workload_save_file'), self.conf_path))

    @property
    def my_workload_save_file(self):
        return self.get_parameter_value('sampled_workload_save_file')
