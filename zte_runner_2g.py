#!/usr/bin/python3

import datetime

import log
from environment import ZTE_VENDOR
from scr.parser import main_baseline_parser

TIME_START_SCRIPT = datetime.datetime.now()
log.i(" ", ZTE_VENDOR, "2G")
log.i(" ", ZTE_VENDOR, "2G")
log.i("           ", ZTE_VENDOR, "2G")
log.i("Start Script : " + ZTE_VENDOR, ZTE_VENDOR, "2G")

main_baseline_parser.run(ZTE_VENDOR, "2G")

TIME_END_SCRIPT = datetime.datetime.now()
log.i("Time : " + str(TIME_END_SCRIPT - TIME_START_SCRIPT), ZTE_VENDOR, "2G")

log.i("--------------------------------", ZTE_VENDOR, "2G")
log.i("--------------------------------", ZTE_VENDOR, "2G")
log.count()
log.i("--------------------------------", ZTE_VENDOR, "2G")
log.i("--------------------------------", ZTE_VENDOR, "2G")

log.i("Done all : " + ZTE_VENDOR + " 2G", ZTE_VENDOR, "2G")

log.i("           ", ZTE_VENDOR, "2G")
log.i("           ", ZTE_VENDOR, "2G")
log.i("           ", ZTE_VENDOR, "2G")
log.i("           ", ZTE_VENDOR, "2G")
log.i("           ", ZTE_VENDOR, "2G")
log.i("           ", ZTE_VENDOR, "2G")
