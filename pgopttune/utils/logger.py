def logging_dict(debug=False):
    if debug:
        return {
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
                    "level": "DEBUG",
                    "formatter": "consoleFormatter",
                    "stream": "ext://sys.stdout"
                },
                "logFileHandler": {
                    "class": "logging.FileHandler",
                    "level": "DEBUG",
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
    else:
        return {
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
