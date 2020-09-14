#!/usr/bin/python3

import datetime
import sys

import log
from environment import ZTE_VENDOR
from scr.parser import main_parser

TIME_START_SCRIPT = datetime.datetime.now()
log.i(" ", ZTE_VENDOR)
log.i(" ", ZTE_VENDOR)
log.i("==================================================", ZTE_VENDOR)
log.i("Start Script : " + ZTE_VENDOR, ZTE_VENDOR)



main_parser.run(ZTE_VENDOR, "2G")


TIME_END_SCRIPT = datetime.datetime.now()
log.i("Done all : " + ZTE_VENDOR, ZTE_VENDOR)
log.i("Time : " + str(TIME_END_SCRIPT - TIME_START_SCRIPT), ZTE_VENDOR)
