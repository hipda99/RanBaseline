#!/usr/bin/python3
import datetime

import log
from environment import ERICSSON_VENDOR
from scr.parser import main_baseline_parser
import os

TIME_START_SCRIPT = datetime.datetime.now()
log.i(" ", ERICSSON_VENDOR, "5G")
log.i(" ", ERICSSON_VENDOR, "5G")
log.i("           ", ERICSSON_VENDOR, "5G")
log.i("Start Script : " + ERICSSON_VENDOR, ERICSSON_VENDOR, "5G")

main_baseline_parser.run(ERICSSON_VENDOR, "5G")

TIME_END_SCRIPT = datetime.datetime.now()
log.i("Time : " + str(TIME_END_SCRIPT - TIME_START_SCRIPT), ERICSSON_VENDOR, "5G")
log.i("--------------------------------", ERICSSON_VENDOR, "5G")
log.i("--------------------------------", ERICSSON_VENDOR, "5G")
log.count()
log.i("--------------------------------", ERICSSON_VENDOR, "5G")
log.i("--------------------------------", ERICSSON_VENDOR, "5G")
log.i("Done all : " + ERICSSON_VENDOR + " 5G", ERICSSON_VENDOR, "5G")


log.i("           ", ERICSSON_VENDOR, "5G")
log.i("           ", ERICSSON_VENDOR, "5G")
log.i("           ", ERICSSON_VENDOR, "5G")
log.i("           ", ERICSSON_VENDOR, "5G")
log.i("           ", ERICSSON_VENDOR, "5G")
log.i("           ", ERICSSON_VENDOR, "5G")
