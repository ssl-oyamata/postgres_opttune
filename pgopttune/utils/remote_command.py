import paramiko
import scp

class SSHCommandExecutor:
    def __init__(self, user, password='postgres', hostname='localhost', port=22, timeout=15.0):
        self.hostname = hostname
        self.port = port
        self.user = user
        self.password = password
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(self.hostname, username=self.user,
                            password=self.password, port=self.port, timeout=timeout, look_for_keys=False)

    def __del__(self):
        self.client.close()

    def exec(self, command, only_retval=True, environment_dict=None):
        _, stdout, stderr = self.client.exec_command(command, environment=environment_dict)
        if only_retval:
            return {'stdout': None,
                    'stderr': None,
                    'retval': stdout.channel.recv_exit_status()}
        else:
            # Depending on the command, the following problems may occur:
            # hanging on stdout.readlines()
            # https://github.com/paramiko/paramiko/issues/109
            return {'stdout': stdout.readlines(),
                    'stderr': stderr.readlines(),
                    'retval': stdout.channel.recv_exit_status()}

    def get(self, remote_path=None, local_path=None):
        with scp.SCPClient(self.client.get_transport()) as scpc:
            scpc.get(remote_path, local_path)


if __name__ == "__main__":
    ssh = SSHCommandExecutor(hostname='127.0.0.1', user='postgres', password='postgres')
    # ret = ssh.exec('ls /tmp ; ls /tmp')
    ret = ssh.exec('/usr/pgsql-12/bin/pg_ctl -D /var/lib/pgsql/12/data/ restart')
    if not ret['retval'] == 0:
        raise Exception('command failed')
    print('stdout : {}'.format(ret["stdout"]))
    print('stderr : {}'.format(ret["stderr"]))
    print('retval : {}'.format(ret["retval"]))
