from datetime import datetime, timedelta

import logging
import logging.config
from logging import getLogger, INFO, ERROR, WARNING, CRITICAL, NOTSET, DEBUG, Formatter
from concurrent_log_handler import ConcurrentRotatingFileHandler
import os

LOG_PATH = str(os.path.realpath(__file__))
LOG_PATH = LOG_PATH.replace("logging_managers.pyc", "")
LOG_PATH = LOG_PATH.replace("logging_managers.pyo", "")
LOG_PATH = LOG_PATH.replace("logging_managers.py", "")
LOG_PATH = LOG_PATH + "../"

def register_managers(app, WSGI_PATH_PREFIX):
    return LoggingManagers(app, WSGI_PATH_PREFIX)

class LoggingManagers:
    def __init__(self, app, WSGI_PATH_PREFIX):
        # logging.config.fileConfig('logging.conf')
        log = getLogger()
        
        # Use an absolute path to prevent file rotation trouble.
        try:
            # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            print("==============================================", os.path.abspath)
            logfile = os.path.abspath(LOG_PATH +"serverlogs.log")
            # Rotate log after reaching 512K, keep 5 old copies.
            rotateHandler = ConcurrentRotatingFileHandler(logfile, "a", 512*1024, 5)
            log.addHandler(rotateHandler)
            log.setLevel(INFO)
            # log.setLevel(ERROR)
            # log.setLevel(WARNING)
            # log.setLevel(CRITICAL)
            # log.setLevel(NOTSET)
            # log.setLevel(DEBUG)
        except Exception as e:
            print("Exception Occured....", e)
            log.exception("Fatal error in main exception", exc_info=True)
            log.exception(str(e))
        
