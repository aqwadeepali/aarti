import schedule
import os, json
from datetime import timedelta, datetime
from time import strftime
# import datetime
import time
import os, json, csv
import re
import shutil
import zipfile
from stage_data import RunStage

import logging
import logging.config
from logging import getLogger, INFO, ERROR, WARNING, CRITICAL, NOTSET, DEBUG
from concurrent_log_handler import ConcurrentRotatingFileHandler
import os

LOG_PATH = str(os.path.realpath(__file__))
LOG_PATH = LOG_PATH.replace("stage_schedular.pyc", "")
LOG_PATH = LOG_PATH.replace("stage_schedular.pyo", "")
LOG_PATH = LOG_PATH.replace("stage_schedular.py", "")
LOG_PATH = LOG_PATH + "../"

log = logging.getLogger()
logfile = os.path.abspath(LOG_PATH + "stageschedularlogs.log")
log.setLevel(INFO)
# Rotate log after reaching 512K, keep 5 old copies.
rotateHandler = ConcurrentRotatingFileHandler(logfile, "a", 1024*1024, 5)
log.addHandler(rotateHandler)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
rotateHandler.setFormatter(formatter)

FILE_PATH = str(os.path.realpath(__file__))
FILE_PATH = FILE_PATH.replace("classes/stage_schedular.pyc", "")
FILE_PATH = FILE_PATH.replace("classes/stage_schedular.pyo", "")
FILE_PATH = FILE_PATH.replace("classes/stage_schedular.py", "")
FILE_PATH = FILE_PATH + "dbpush/data/"

SETTING_PATH = str(os.path.realpath(__file__))
SETTING_PATH = SETTING_PATH.replace("stage_schedular.pyc", "")
SETTING_PATH = SETTING_PATH.replace("stage_schedular.pyo", "")
SETTING_PATH = SETTING_PATH.replace("stage_schedular.py", "")
SETTING_PATH = SETTING_PATH + "data/"


def job():
    try:
        log.info("DB Push Working...")
        dt = datetime.now()
        dtime = datetime.now().time()
        
        #print "dtime: ", dtime
        # dtimetuple = time.mktime(datetime.now().timetuple())

        current_format_dt = dt.strftime('%Y-%m-%d')
        log.info("dt: "+ current_format_dt)

        dbManager = RunStage(current_format_dt)

        log.info("DB Push Sleeping...")
        # print("DB Push Sleeping...")
    except Exception as e:
        log.exception("Fatal error in main exception", exc_info=True)
        log.exception(str(e)) 

schedule.every(1).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
