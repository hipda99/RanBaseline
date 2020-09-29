#!/usr/bin/python3

import datetime

import log
from environment import ZTE_VENDOR
from scr.parser import main_baseline_parser
from scr.util import parser_db

TIME_START_SCRIPT = datetime.datetime.now()
log.i(" ", ZTE_VENDOR, "3G")
log.i(" ", ZTE_VENDOR, "3G")
log.i("           ", ZTE_VENDOR, "3G")
log.i("Start Script : " + ZTE_VENDOR, ZTE_VENDOR, "3G")

parser_db.update_status(ZTE_VENDOR, '3G', parser_db.STATUS_OPEN)

main_baseline_parser.run(ZTE_VENDOR, "3G")

TIME_END_SCRIPT = datetime.datetime.now()
log.i("Time : " + str(TIME_END_SCRIPT - TIME_START_SCRIPT), ZTE_VENDOR, "3G")

log.i("--------------------------------", ZTE_VENDOR, "3G")
log.i("--------------------------------", ZTE_VENDOR, "3G")
log.count()
log.i("--------------------------------", ZTE_VENDOR, "3G")
log.i("--------------------------------", ZTE_VENDOR, "3G")
log.i("Done all : " + ZTE_VENDOR + " 3G", ZTE_VENDOR, "3G")
parser_db.update_status(ZTE_VENDOR, '3G', parser_db.STATUS_CLOSE)

log.i("           ", ZTE_VENDOR, "3G")
log.i("           ", ZTE_VENDOR, "3G")
log.i("           ", ZTE_VENDOR, "3G")
log.i("           ", ZTE_VENDOR, "3G")
log.i("           ", ZTE_VENDOR, "3G")
log.i("           ", ZTE_VENDOR, "3G")
