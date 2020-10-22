#!/usr/bin/python3

import datetime
import os

import log
from environment import ZTE_VENDOR
from scr.parser import main_baseline_parser_debug
from scr.util import parser_db

TIME_START_SCRIPT = datetime.datetime.now()
log.i(" ", ZTE_VENDOR, "4G")
log.i(" ", ZTE_VENDOR, "4G")
log.i("           ", ZTE_VENDOR)
log.i("Start Script : " + ZTE_VENDOR, ZTE_VENDOR, "4G")
parser_db.update_status(ZTE_VENDOR, '4G', parser_db.STATUS_OPEN)

main_baseline_parser_debug.run(ZTE_VENDOR, "4G")

TIME_END_SCRIPT = datetime.datetime.now()
log.i("Time : " + str(TIME_END_SCRIPT - TIME_START_SCRIPT), ZTE_VENDOR, "4G")

log.i("--------------------------------", ZTE_VENDOR, "4G")
log.i("--------------------------------", ZTE_VENDOR, "4G")
log.count()
log.i("--------------------------------", ZTE_VENDOR, "4G")
log.i("--------------------------------", ZTE_VENDOR, "4G")
log.i("Done all : " + ZTE_VENDOR + " 4G", ZTE_VENDOR, "4G")
parser_db.update_status(ZTE_VENDOR, '4G', parser_db.STATUS_CLOSE)

log.i("           ", ZTE_VENDOR)
log.i("           ", ZTE_VENDOR)
log.i("           ", ZTE_VENDOR)
log.i("           ", ZTE_VENDOR)
log.i("           ", ZTE_VENDOR)
log.i("           ", ZTE_VENDOR)

log.i("Start Command Report", ZTE_VENDOR)
os.system("/home/ngoss/RANBaseLine/ZTE_BL_Audit_Report.sh")
log.i("Done Command Report", ZTE_VENDOR)
