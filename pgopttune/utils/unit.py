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
