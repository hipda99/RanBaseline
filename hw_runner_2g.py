#!/usr/bin/python3
import datetime
import sys
import log
from environment import HUAWEI_VENDOR
from scr.parser import main_baseline_parser

TIME_START_SCRIPT = datetime.datetime.now()
log.i(" ", HUAWEI_VENDOR, "2G")
log.i(" ", HUAWEI_VENDOR, "2G")
log.i("           ",HUAWEI_VENDOR)
log.i("Start Script : " + HUAWEI_VENDOR, HUAWEI_VENDOR, "2G")

# main_parser.run(HUAWEI_VENDOR)

main_baseline_parser.run(HUAWEI_VENDOR, "2G")

TIME_END_SCRIPT = datetime.datetime.now()
log.i("Time : " + str(TIME_END_SCRIPT - TIME_START_SCRIPT), HUAWEI_VENDOR, "2G")

log.i("--------------------------------", HUAWEI_VENDOR, "2G")
log.i("--------------------------------", HUAWEI_VENDOR, "2G")
log.count()
log.i("--------------------------------", HUAWEI_VENDOR, "2G")
log.i("--------------------------------", HUAWEI_VENDOR, "2G")
log.i("Done all : " + HUAWEI_VENDOR + " 2G", HUAWEI_VENDOR, "2G")


log.i("           ",HUAWEI_VENDOR, "2G")
log.i("           ",HUAWEI_VENDOR, "2G")
log.i("           ",HUAWEI_VENDOR, "2G")
log.i("           ",HUAWEI_VENDOR, "2G")
log.i("           ",HUAWEI_VENDOR, "2G")
log.i("           ",HUAWEI_VENDOR, "2G")