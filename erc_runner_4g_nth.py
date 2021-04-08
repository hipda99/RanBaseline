#!/usr/bin/python3
import datetime

import log
from environment import ERICSSON_VENDOR
from scr.parser import main_baseline_parser
from scr.util import parser_db
import os

TIME_START_SCRIPT = datetime.datetime.now()
log.i(" ", ERICSSON_VENDOR, "4G-NTH")
log.i(" ", ERICSSON_VENDOR, "4G-NTH")
log.i("           ", ERICSSON_VENDOR, "4G-NTH")
log.i("Start Script : " + ERICSSON_VENDOR, ERICSSON_VENDOR, "4G-NTH")

parser_db.update_status(ERICSSON_VENDOR, '4G-NTH', parser_db.STATUS_OPEN)

main_baseline_parser.run(ERICSSON_VENDOR, "4G", "NTH")

TIME_END_SCRIPT = datetime.datetime.now()
log.i("Time : " + str(TIME_END_SCRIPT - TIME_START_SCRIPT), ERICSSON_VENDOR, "4G-NTH")
log.i("--------------------------------", ERICSSON_VENDOR, "4G-NTH")
log.i("--------------------------------", ERICSSON_VENDOR, "4G-NTH")
log.count()
log.i("--------------------------------", ERICSSON_VENDOR, "4G-NTH")
log.i("--------------------------------", ERICSSON_VENDOR, "4G-NTH")
log.i("Done all : " + ERICSSON_VENDOR + " 4G", ERICSSON_VENDOR, "4G-NTH")

parser_db.update_status(ERICSSON_VENDOR, '4G-NTH', parser_db.STATUS_CLOSE)

log.i("           ", ERICSSON_VENDOR, "4G-NTH")
log.i("           ", ERICSSON_VENDOR, "4G-NTH")
log.i("           ", ERICSSON_VENDOR, "4G-NTH")
log.i("           ", ERICSSON_VENDOR, "4G-NTH")
log.i("           ", ERICSSON_VENDOR, "4G-NTH")
log.i("           ", ERICSSON_VENDOR, "4G-NTH")


log.i("Start Command Report", ERICSSON_VENDOR)
os.system("/home/ngoss/RANBaseLine/ERC_BL_Audit_Report_4G_nth.sh")
log.i("Done Command Report", ERICSSON_VENDOR)