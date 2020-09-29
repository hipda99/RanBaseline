#!/usr/bin/python3
import datetime
import sys
import log
from environment import ERICSSON_VENDOR
from scr.parser import main_baseline_parser
from scr.util import parser_db

TIME_START_SCRIPT = datetime.datetime.now()
log.i(" ", ERICSSON_VENDOR, "3G")
log.i(" ", ERICSSON_VENDOR, "3G")
log.i("           ",ERICSSON_VENDOR)
log.i("Start Script : " + ERICSSON_VENDOR, ERICSSON_VENDOR, "3G")

parser_db.update_status(ERICSSON_VENDOR, '3G', parser_db.STATUS_OPEN)

main_baseline_parser.run(ERICSSON_VENDOR, "3G")

TIME_END_SCRIPT = datetime.datetime.now()
log.i("Time : " + str(TIME_END_SCRIPT - TIME_START_SCRIPT), ERICSSON_VENDOR, "3G")


log.i("--------------------------------", ERICSSON_VENDOR, "3G")
log.i("--------------------------------", ERICSSON_VENDOR, "3G")
log.count()
log.i("--------------------------------", ERICSSON_VENDOR, "3G")
log.i("--------------------------------", ERICSSON_VENDOR, "3G")
log.i("Done all : " + ERICSSON_VENDOR + " 3G", ERICSSON_VENDOR, "3G")

parser_db.update_status(ERICSSON_VENDOR, '3G', parser_db.STATUS_CLOSE)

log.i("           ", ERICSSON_VENDOR, "3G")
log.i("           ", ERICSSON_VENDOR, "3G")
log.i("           ", ERICSSON_VENDOR, "3G")
log.i("           ", ERICSSON_VENDOR, "3G")
log.i("           ", ERICSSON_VENDOR, "3G")
log.i("           ", ERICSSON_VENDOR, "3G")