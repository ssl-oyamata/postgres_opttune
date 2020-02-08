from pgopttune.config.config import Config


class PostgresServerConfig(Config):
    def __init__(self, conf_path, section='PostgreSQL'):
        super().__init__(conf_path)
        self.config_dict = dict(self.config.items(section))

    @property
    def pgdata(self):
        return self.get_parameter_value('pgdata')

    @property
    def pgbin(self):
        return self.get_parameter_value('pgbin')

    @property
    def major_version(self):
        return self.get_parameter_value('major_version')

    @property
    def host(self):
        return self.get_parameter_value('pghost')

    @property
    def port(self):
        return self.get_parameter_value('pgport')

    @property
    def user(self):
        return self.get_parameter_value('pguser')

    @property
    def password(self):
        return self.get_parameter_value('pgpassword')

    @property
    def database(self):
        return self.get_parameter_value('pgdatabase')

    @property
    def os_user(self):
        return self.get_parameter_value('pg_os_user')

    @property
    def ssh_port(self):
        return self.get_parameter_value('ssh_port')

    @property
    def ssh_password(self):
        return self.get_parameter_value('ssh_password')
