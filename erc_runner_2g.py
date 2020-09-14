#!/usr/bin/python3
import datetime

import log
from environment import ERICSSON_VENDOR
from scr.parser import main_baseline_parser

TIME_START_SCRIPT = datetime.datetime.now()
log.i(" ", ERICSSON_VENDOR, "3G")
log.i(" ", ERICSSON_VENDOR, "3G")
log.i("           ", ERICSSON_VENDOR, "3G")
log.i("Start Script : " + ERICSSON_VENDOR, ERICSSON_VENDOR, "3G")

main_baseline_parser.run(ERICSSON_VENDOR, "2G")

TIME_END_SCRIPT = datetime.datetime.now()
log.i("Time : " + str(TIME_END_SCRIPT - TIME_START_SCRIPT), ERICSSON_VENDOR, "3G")

log.i("--------------------------------", ERICSSON_VENDOR, "3G")
log.i("--------------------------------", ERICSSON_VENDOR, "3G")
log.count()
log.i("--------------------------------", ERICSSON_VENDOR, "3G")
log.i("--------------------------------", ERICSSON_VENDOR, "3G")
log.i("Done all : " + ERICSSON_VENDOR + " 2G", ERICSSON_VENDOR, "3G")

log.i("           ", ERICSSON_VENDOR, "2G")
log.i("           ", ERICSSON_VENDOR, "3G")
log.i("           ", ERICSSON_VENDOR, "3G")
log.i("           ", ERICSSON_VENDOR, "3G")
log.i("           ", ERICSSON_VENDOR, "3G")
log.i("           ", ERICSSON_VENDOR, "3G")
