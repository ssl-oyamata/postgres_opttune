BYTES_CONVERT = [
    (1024 ** 5, 'PB'),
    (1024 ** 4, 'TB'),
    (1024 ** 3, 'GB'),
    (1024 ** 2, 'MB'),
    (1024 ** 1, 'kB'),
    (1024 ** 0, 'B'),
]

TIME_CONVERT = [
    (1000 * 60 * 60 * 24, 'd'),
    (1000 * 60 * 60, 'h'),
    (1000 * 60, 'min'),
    (1000, 's'),
    (1, 'ms'),
]


def get_raw_size(value, system):
    for factor, suffix in system:
        if value.endswith(suffix):
            if len(value) == len(suffix):
                amount = 1
            else:
                try:
                    amount = int(value[:-len(suffix)])
                except ValueError:
                    continue
            return amount * factor
    return None


def get_param_raw(value, param_type):
    if param_type == 'integer':
        return int(value)
    elif param_type == 'float':
        return float(value)
    elif param_type == 'bytes':
        return get_raw_size(value, BYTES_CONVERT)
    elif param_type == 'time':
        return get_raw_size(value, TIME_CONVERT)
    elif param_type == 'enum':
        return str(value)
    elif param_type == 'boolean':
        return str(value)
    else:
        raise Exception('parameter Type does not support')


# bytes system format
def format_bytes_str(size, precision=2):
    format_size, format_unit = format_bytes(size)
    return join_size_and_unit(format_size, format_unit, precision)


def format_bytes(size):
    power = BYTES_CONVERT[5][0]
    unit = BYTES_CONVERT[5][1]  # default unit
    for byte_unit in (reversed(BYTES_CONVERT)):
        if size < byte_unit[0]:
            break
        power = byte_unit[0]
        unit = byte_unit[1]

    return size / power, unit


# time system format
def format_milliseconds_str(milliseconds, precision=2):
    format_time, format_unit = format_milliseconds(milliseconds)
    return join_size_and_unit(format_time, format_unit, precision)


def format_milliseconds(milliseconds):
    power = TIME_CONVERT[4][0]
    unit = TIME_CONVERT[4][1]  # default unit
    for time_unit in (reversed(TIME_CONVERT)):
        if milliseconds < time_unit[0]:
            break
        power = time_unit[0]
        unit = time_unit[1]

    return milliseconds / power, unit


def join_size_and_unit(format_size, format_unit, precision=2):
    return str(round(format_size, precision)) + format_unit