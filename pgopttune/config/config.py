import os
import errno
import configparser


class Config:
    def __init__(self, conf_path, comment_prefixes=('#', ';'), encoding='utf-8'):
        # read parameters
        config = configparser.ConfigParser(inline_comment_prefixes=comment_prefixes)
        if not os.path.exists(conf_path):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), conf_path)
        else:
            config.read(conf_path, encoding=encoding)
        self.config = config
        self.config_dict = dict(config.items('DEFAULT'))

    def get_parameter_value(self, parameter_name):
        return self.config_dict.get(parameter_name)