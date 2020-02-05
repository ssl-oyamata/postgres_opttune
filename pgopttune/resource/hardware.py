import psutil
import logging

from pgopttune.utils.unit import format_bytes_str

logger = logging.getLogger(__name__)


class HardwareResource:
    def __init__(self):
        self.cpu_count = psutil.cpu_count()
        mem = psutil.virtual_memory()
        self.memory_size = mem.total
        swap = psutil.swap_memory()
        self.swap_size = swap.total

    def print_resource(self):
        logger.info('cpu count :  {} \n'
                    'memory size : {} ({}) \n'
                    'swap size : {} ({})) \n'
                    .format(self.cpu_count,
                            self.memory_size, format_bytes_str(self.memory_size),
                            self.swap_size, format_bytes_str(self.swap_size)))


if __name__ == "__main__":
    hardware = HardwareResource()
    hardware.print_resource()