#!/usr/bin/python3

import log
from environment import ZTE_VENDOR, ERICSSON_VENDOR, HUAWEI_VENDOR
from scr.parser import main_baseline_parser

#
# main_baseline_parser.run_baseline(ERICSSON_VENDOR, "2G")
# main_baseline_parser.run_baseline(ERICSSON_VENDOR, "3G")
# main_baseline_parser.run_baseline(ERICSSON_VENDOR, "4G")
#
# main_baseline_parser.run_baseline(ZTE_VENDOR, "2G")
# main_baseline_parser.run_baseline(ZTE_VENDOR, "3G")
# main_baseline_parser.run_baseline(ZTE_VENDOR, "4G")
#
# main_baseline_parser.run_baseline(HUAWEI_VENDOR, "2G")
# main_baseline_parser.run_baseline(HUAWEI_VENDOR, "3G")
main_baseline_parser.run_baseline(HUAWEI_VENDOR, "4G")
log.i("Update Baseline HW 4G ::: Done ")
