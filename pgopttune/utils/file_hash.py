import hashlib
import logging

logger = logging.getLogger(__name__)


def get_file_hash(file_path, algorithms='sha1'):
    if 'md5' == algorithms:
        hash_calc = hashlib.md5()
    elif 'sha224' == algorithms:
        hash_calc = hashlib.sha224()
    elif 'sha256' == algorithms:
        hash_calc = hashlib.sha256()
    elif 'sha384' == algorithms:
        hash_calc = hashlib.sha384()
    elif 'sha512' == algorithms:
        hash_calc = hashlib.sha512()
    elif 'sha1' == algorithms:
        hash_calc = hashlib.sha1()
    else:
        raise ValueError("An unexpected algorithm was specified.\n"
                         "algorithms = {}".format(algorithms))

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
    logging.basicConfig(level=logging.DEBUG)
    file_path = 'conf/tpcc_config_postgres.xml'
    get_file_hash(file_path)
