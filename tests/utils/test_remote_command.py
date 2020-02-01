import pytest
import os

from pgopttune.utils.remote_command import SSHCommandExecutor

'''
@pytest.fixture()
def ssh_client():
    ssh_client = SSHCommandExecutor(hostname='localhost', user='postgres', password='postgres')
    yield ssh_client
    del ssh_client


@pytest.fixture()
def make_test_file():
    with open('/tmp/test_ssh_command.txt', 'a') as f:
        f.write('hoge\n')
    yield
    os.remove('/tmp/test_ssh_command.txt')


class TestSSHCommandExecutor:
    def test_exec_ls(self, ssh_client, make_test_file):
        ret = ssh_client.exec('ls /tmp/test_ssh_command.txt', only_retval=False)
        assert ret["stdout"][0] == '/tmp/test_ssh_command.txt\n'
        assert len(ret["stderr"]) == 0
        assert ret["retval"] == 0

    def test_exec_ls_only_retval(self, ssh_client, make_test_file):
        ret = ssh_client.exec('ls /tmp/test_ssh_command.txt', only_retval=True)
        assert ret["stdout"] is None
        assert ret["stderr"] is None
        assert ret["retval"] == 0

    def test_failed_command(self, ssh_client):
        ret = ssh_client.exec('aaaaaaaaaaaaa', only_retval=False)
        assert ret["retval"] != 1
'''