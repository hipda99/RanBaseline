#!/usr/bin/python3
import datetime

import log
from environment import ERICSSON_SW_VENDOR
from scr.parser import main_baseline_parser

TIME_START_SCRIPT = datetime.datetime.now()
log.i(" ", ERICSSON_SW_VENDOR, "3G")
log.i(" ", ERICSSON_SW_VENDOR, "3G")
log.i("           ", ERICSSON_SW_VENDOR)
log.i("Start Script : " + ERICSSON_SW_VENDOR, ERICSSON_SW_VENDOR, "3G")

main_baseline_parser.run(ERICSSON_SW_VENDOR, "3G")

TIME_END_SCRIPT = datetime.datetime.now()
log.i("Time : " + str(TIME_END_SCRIPT - TIME_START_SCRIPT), ERICSSON_SW_VENDOR, "3G")

log.i("--------------------------------", ERICSSON_SW_VENDOR, "3G")
log.i("--------------------------------", ERICSSON_SW_VENDOR, "3G")
log.count()
log.i("--------------------------------", ERICSSON_SW_VENDOR, "3G")
log.i("--------------------------------", ERICSSON_SW_VENDOR, "3G")

log.i("Done all : " + ERICSSON_SW_VENDOR + " 3G", ERICSSON_SW_VENDOR, "3G")

log.i("           ", ERICSSON_SW_VENDOR)
log.i("           ", ERICSSON_SW_VENDOR)
log.i("           ", ERICSSON_SW_VENDOR)
log.i("           ", ERICSSON_SW_VENDOR)
log.i("           ", ERICSSON_SW_VENDOR)
log.i("           ", ERICSSON_SW_VENDOR)
