#!/usr/bin/python3
import datetime

import log
from environment import ERICSSON_VENDOR
from scr.parser import main_baseline_parser
import os

TIME_START_SCRIPT = datetime.datetime.now()
log.i(" ", ERICSSON_VENDOR, "4G")
log.i(" ", ERICSSON_VENDOR, "4G")
log.i("           ", ERICSSON_VENDOR, "4G")
log.i("Start Script : " + ERICSSON_VENDOR, ERICSSON_VENDOR, "4G")

main_baseline_parser.run(ERICSSON_VENDOR, "4G")

TIME_END_SCRIPT = datetime.datetime.now()
log.i("Time : " + str(TIME_END_SCRIPT - TIME_START_SCRIPT), ERICSSON_VENDOR, "4G")
log.i("--------------------------------", ERICSSON_VENDOR, "4G")
log.i("--------------------------------", ERICSSON_VENDOR, "4G")
log.count()
log.i("--------------------------------", ERICSSON_VENDOR, "4G")
log.i("--------------------------------", ERICSSON_VENDOR, "4G")
log.i("Done all : " + ERICSSON_VENDOR + " 4G", ERICSSON_VENDOR, "4G")


log.i("           ", ERICSSON_VENDOR, "4G")
log.i("           ", ERICSSON_VENDOR, "4G")
log.i("           ", ERICSSON_VENDOR, "4G")
log.i("           ", ERICSSON_VENDOR, "4G")
log.i("           ", ERICSSON_VENDOR, "4G")
log.i("           ", ERICSSON_VENDOR, "4G")



log.i("Start Command Report", ERICSSON_VENDOR)
os.system("/home/ngoss/RANBaseLine/ERC_BL_Audit_Report.sh")
log.i("Done Command Report", ERICSSON_VENDOR)