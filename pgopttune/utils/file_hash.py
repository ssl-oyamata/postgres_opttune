import hashlib
from logging import getLogger

logger = getLogger(__name__)


def get_file_hash(file_path, algorithm='sha1'):
    if 'md5' == algorithm:
        hash_calc = hashlib.md5()
    elif 'sha224' == algorithm:
        hash_calc = hashlib.sha224()
    elif 'sha256' == algorithm:
        hash_calc = hashlib.sha256()
    elif 'sha384' == algorithm:
        hash_calc = hashlib.sha384()
    elif 'sha512' == algorithm:
        hash_calc = hashlib.sha512()
    elif 'sha1' == algorithm:
        hash_calc = hashlib.sha1()
    else:
        raise ValueError("An unexpected algorithm was specified.\n"
                         "algorithms = {}".format(algorithm))

    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(2048 * hash_calc.block_size)
            if len(chunk) == 0:
                break
            hash_calc.update(chunk)
    digest = hash_calc.hexdigest()
    logger.debug("The hash value calculation is complete. File : {} , Hash : {}".format(file_path, digest))
    return digest


if __name__ == "__main__":
    from logging import basicConfig, DEBUG

    basicConfig(level=DEBUG)
    test_file_path = 'conf/tpcc_config_postgres.xml'
    get_file_hash(test_file_path)
