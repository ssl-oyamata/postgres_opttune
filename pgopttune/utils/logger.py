def logging_dict(debug=False):
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "root": {
            "level": "DEBUG",
            "handlers": [
                "consoleHandler",
                "logFileHandler"
            ]
        },
        "handlers": {
            "consoleHandler": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "consoleFormatter",
                "stream": "ext://sys.stdout"
            },
            "logFileHandler": {
                "class": "logging.FileHandler",
                "level": "INFO",
                "formatter": "logFileFormatter",
                "filename": "./log/tune.log",
                "mode": "a+",
                "encoding": "utf-8"
            }
        },
        "formatters": {
            "consoleFormatter": {
                "format": "[%(asctime)s] [%(levelname)s] %(message)s"
            },
            "logFileFormatter": {
                "format": "[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)s] %(message)s"
            }
        }
    }
    if debug:
        logging_config["handlers"]["consoleHandler"]["level"] = "DEBUG"
        logging_config["handlers"]["logFileHandler"]["level"] = "DEBUG"
    return logging_config
