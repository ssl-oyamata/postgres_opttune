import psutil
import logging

from pgopttune.utils.unit import format_bytes_str
from pgopttune.utils.remote_command import SSHCommandExecutor

logger = logging.getLogger(__name__)


class HardwareResource:
    def __init__(self, host='127.0.0.1', user='postgres', password='postgres'):
        self.host = host
        if self.host == '127.0.0.1' or self.host == 'localhost':
            self.cpu_count = psutil.cpu_count()
            mem = psutil.virtual_memory()
            self.memory_size = mem.total
            swap = psutil.swap_memory()
            self.swap_size = swap.total
        else:
            self.user = user
            self.password = password
            self.cpu_count = self._get_remote_cpu_count()
            self.memory_size = self._get_remote_memory_size()
            self.swap_size = self._get_remote_swap_size()

    def _get_remote_cpu_count(self):
        cpu_count_cmd = 'cat /proc/cpuinfo | grep processor | wc -l'
        ssh_client = self._get_ssh_client()
        cpu_count_result = ssh_client.exec(cpu_count_cmd, only_retval=False)
        if not cpu_count_result['retval'] == 0:
            raise ValueError('Get number of cpu failed.\n'
                             'stderr : {}\n'
                             'command : {}'.format(cpu_count_result.get('stderr'), cpu_count_cmd))
        return int(cpu_count_result.get('stdout')[0])

    def _get_remote_memory_size(self):
        memory_size_cmd = "free -b | grep Mem | awk '{print $2}'"
        ssh_client = self._get_ssh_client()
        memory_size_result = ssh_client.exec(memory_size_cmd, only_retval=False)
        if not memory_size_result['retval'] == 0:
            raise ValueError('Get memory size failed.\n'
                             'stderr : {}\n'
                             'command : {}'.format(memory_size_result.get('stderr'), memory_size_cmd))
        return int(memory_size_result.get('stdout')[0])

    def _get_remote_swap_size(self):
        swap_size_cmd = "free -b | grep Swap | awk '{print $2}'"
        ssh_client = self._get_ssh_client()
        swap_size_result = ssh_client.exec(swap_size_cmd, only_retval=False)
        if not swap_size_result['retval'] == 0:
            raise ValueError('Get swap size failed.\n'
                             'stderr : {}\n'
                             'command : {}'.format(swap_size_result.get('stderr'), swap_size_cmd))
        return int(swap_size_result.get('stdout')[0])

    def _get_ssh_client(self):
        return SSHCommandExecutor(hostname=self.host, user=self.user, password=self.password)

    def print_resource(self):
        logger.info('host: {} \n'
                    'cpu count :  {} \n'
                    'memory size : {} ({}) \n'
                    'swap size : {} ({})) \n'
                    .format(self.host,
                            self.cpu_count,
                            self.memory_size, format_bytes_str(self.memory_size),
                            self.swap_size, format_bytes_str(self.swap_size)))


if __name__ == "__main__":
    hardware = HardwareResource()
    hardware.print_resource()
    hardware = HardwareResource(host='192.168.0.20')
    hardware.print_resource()