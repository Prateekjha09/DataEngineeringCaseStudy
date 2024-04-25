# Importing all the required libraries which will be used in this module
import logging
import datetime
import configparser
import os
from logging.handlers import RotatingFileHandler
import sys

def log_object_creation():
    config_path = "privateKey\Config\config.ini"
    log_path = os.path.join("logGeneration",'LogUpto_')
    try:
        # Reading contents from config.ini file for decided parameters/ properties which 
        # will be used for running the code specially at DB level without exposing the 
        # values of valuable params
        config_obj = configparser.ConfigParser()
        config_obj.read(config_path)

        # Creating log file
        file_name = log_path + str(datetime.datetime.now())+".log"

        # Initializing a log file
        final_log_size = 0

        # iterating through different configurations setup in config.ini
        for section in config_obj.sections():
            sec = dict(config_obj[section])

            # Finding params from only desired connection section
            if section == "CONNECTION_NAME":
                # Assigning log file size from config file as decided
                final_log_size = int(sec['log_size'])
                break
        
        # Setting up format in which logs generated from different modules will be kept in log files
        log_formatter = logging.Formatter('%(asctime)s|%(levelname)s|%(message)s')

        my_handler = RotatingFileHandler(file_name,mode='a',
                                         maxBytes=final_log_size*1024*1024,
                                         backupCount=3,
                                         encoding=None,
                                         delay=0)
        
        my_handler.setFormatter(log_formatter)
        my_handler.setLevel(logging.info)

        app_log = logging.getLogger('root')
        app_log.setLevel(logging.info)
        app_log.addHandler(my_handler)

        return app_log,config_obj

    except Exception as e:
        # Recording error in case logger is not created clearly
        app_log.error("allLogsCaptured.py--log_object_creation()| There was an issue while creating log object: "+str(e))
        app_log.error("allLogsCaptured.py--log_object_creation()| Exiting the execution..")
        