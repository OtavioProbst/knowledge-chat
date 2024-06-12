import os
from pathlib import Path # easy directory path creation

import logging
from logging.handlers import RotatingFileHandler

def get_logging_level(level:str):
    match(level.lower()):
        case 'debug':
            return logging.DEBUG
        case 'info':
            return logging.INFO
        case 'warning':
            return logging.WARNING
        case 'error':
            return logging.ERROR
        case _:
            return logging.NOTSET

def configure_logger(name, level:str, path=""):

    print(f"Setting up \n\tlogger: {name}\n\tlevel: {level}\n\tpath: {path}\n")

    logger = logging.getLogger(name)

    logger.setLevel(get_logging_level(level))

    if not logger.hasHandlers():

        # Configurar o caminho do arquivo de log
        log_path_file = os.path.join(path, f'{name}.log')

        # if path to file doesn't exist, create it and its parents
        Path(path).mkdir(parents=True, exist_ok=True)

        handler = RotatingFileHandler(log_path_file, encoding='utf-8', maxBytes=10*1024*1024, backupCount=5)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger
