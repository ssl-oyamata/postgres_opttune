import pytest
import multiprocessing
from pgopttune.resource.hardware import HardwareResource


@pytest.fixture()
def resource():
    resource = HardwareResource()
    yield resource
    del resource


@pytest.fixture()
def mem_info():
    mem_info_filepath = '/proc/meminfo'
    mem_info = dict((i.split()[0].rstrip(':'), int(i.split()[1]))
                    for i in open(mem_info_filepath).readlines())
    yield mem_info
    del mem_info


class TestHardwareResource:
    def test_cpu_count(self, resource):
        assert resource.cpu_count == multiprocessing.cpu_count()

    def test_memory_size(self, resource, mem_info):
        assert resource.memory_size == mem_info['MemTotal'] * 1024

    def test_swap_size(self, resource, mem_info):
        assert resource.swap_size == mem_info['SwapTotal'] * 1024
