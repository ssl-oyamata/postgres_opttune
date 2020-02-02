import pytest

from pgopttune.utils.unit import get_param_raw, format_bytes, format_bytes_str, format_milliseconds, \
    format_milliseconds_str


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

    # bytes
    @pytest.mark.parametrize('byte, excepted_result', [
        (1024 ** 5 * 2, (2.0, 'PB')),
        (1024 ** 4 * 2, (2.0, 'TB')),
        (1024 ** 3 * 2, (2.0, 'GB')),
        (1024 ** 2 * 2, (2.0, 'MB')),
        (1024 ** 1 * 2, (2.0, 'kB')),
        (1024 ** 0 * 2, (2.0, 'B')),
        (1024 ** 4 + 5, (1.0000000000045475, 'TB')),
    ])
    def test_format_bytes(self, byte, excepted_result):
        assert format_bytes(byte) == excepted_result

    @pytest.mark.parametrize('byte, str_excepted_result', [
        (1024 ** 5 * 2, '2.0PB'),
        (1024 ** 4 * 2, '2.0TB'),
        (1024 ** 3 * 2, '2.0GB'),
        (1024 ** 2 * 2, '2.0MB'),
        (1024 ** 1 * 2, '2.0kB'),
        (1024 ** 0 * 2, '2.0B'),
        (1835008, '1.75MB'),
    ])
    def test_format_bytes_str(self, byte, str_excepted_result):
        assert format_bytes_str(byte) == str_excepted_result

    # time
    @pytest.mark.parametrize('millisecond, excepted_result', [
        (1000 * 60 * 60 * 24 * 2, (2.0, 'd')),
        (1000 * 60 * 60 * 2, (2.0, 'h')),
        (1000 * 60 * 2, (2.0, 'min')),
        (1000 * 2, (2.0, 's')),
        (2, (2.0, 'ms')),
        (1000 * 130, (2.1666666666666665, 'min')),
    ])
    def test_format_milliseconds(self, millisecond, excepted_result):
        assert format_milliseconds(millisecond) == excepted_result

    @pytest.mark.parametrize('millisecond, str_excepted_result', [
        (1000 * 60 * 60 * 24 * 2, '2.0d'),
        (1000 * 60 * 60 * 2, '2.0h'),
        (1000 * 60 * 2, '2.0min'),
        (1000 * 2, '2.0s'),
        (2, '2.0ms'),
        (1000 * 130, ('2.17min')),
    ])
    def test_format_milliseconds_str(self, millisecond, str_excepted_result):
        assert format_milliseconds_str(millisecond) == str_excepted_result
