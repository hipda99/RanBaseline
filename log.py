import logging
from datetime import date

from environment import LOG_PATH, COUNT_DATA

today = date.today()


def i(message, vender='all', freq="na", region:str = None):
    message = freq + "    " + message
    log = LOG_PATH + vender + "_info_" + str(today) + ".log"
    if region is not None:
        log = LOG_PATH + vender + "_" + region + "_info_" + str(today) + ".log"
    logging.basicConfig(filename=log, level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
    logging.info(message)
    print(message)


def count():
    log_file = LOG_PATH + "row_count.log"
    logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

    for key, val in COUNT_DATA.items():
        message = key + " : " + str(val)

        logging.info(message)
        print(message)


def e(message, vender='all', freq="na", region:str = None):
    message = freq + "    " + str(message)
    log = LOG_PATH + vender + "_error_" + str(today) + ".log"
    if region is not None:
        log = LOG_PATH + vender + "_" + region + "_error_" + str(today) + ".log"
    logging.basicConfig(filename=log, level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
    logging.error(message)
    print(e, message)


def d(message, vender='all', freq="na", region:str = None):
    message = freq + "    " + message
    log = LOG_PATH + vender + "_debug_" + str(today) + ".log"
    if region is not None:
        log = LOG_PATH + vender + "_" + region + "_debug_" + str(today) + ".log"
    logging.basicConfig(filename=log, level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
    logging.error(message)
    print(message)
