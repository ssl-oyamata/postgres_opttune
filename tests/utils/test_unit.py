import pytest

from pgopttune.utils.unit import get_param_raw


class TestUnit:
    @pytest.mark.parametrize('value, param_type, excepted_result', [
        (0, 'integer', 0),
        (1, 'integer', 1),
        (-1, 'integer', -1),
        (0, 'float', 0),
        (0.1, 'float', 0.1),
        ('2PB', 'bytes', 1024 ** 5 * 2),
        ('2TB', 'bytes', 1024 ** 4 * 2),
        ('2GB', 'bytes', 1024 ** 3 * 2),
        ('2MB', 'bytes', 1024 ** 2 * 2),
        ('2kB', 'bytes', 1024 ** 1 * 2),
        ('2B', 'bytes', 2),
        ('2d', 'time', 2 * 1000 * 60 * 60 * 24),
        ('2h', 'time', 2 * 1000 * 60 * 60),
        ('2min', 'time', 2 * 1000 * 60),
        ('2s', 'time', 2 * 1000),
        ('2ms', 'time', 2),
        ('replica', 'enum', 'replica'),
        (2, 'enum', '2'),
        (True, 'boolean', 'True'),
        (False, 'boolean', 'False'),
    ])
    def test_get_param_raw(self, value, param_type, excepted_result):
        assert get_param_raw(value, param_type) == excepted_result
