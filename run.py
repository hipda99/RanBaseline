#!/usr/bin/python3

from environment import HUAWEI_VENDOR, ERICSSON_VENDOR, ZTE_VENDOR, ERICSSON_FEATURE_VENDOR
from scr.parser import main_baseline_parser
from scr.parser.zte import zte_baseline_parser, zte_filler
import sys
import log


# main_baseline_parser.run_baseline(ERICSSON_VENDOR, "2G")
# main_baseline_parser.run_baseline(ERICSSON_VENDOR, "3G")
main_baseline_parser.run_baseline(ERICSSON_VENDOR, "4G")
main_baseline_parser.run_baseline(ERICSSON_VENDOR, "5G")
# main_baseline_parser.run(ERICSSON_FEATURE_VENDOR, "4G")

# main_baseline_parser.run_baseline(ZTE_VENDOR, "2G")
# main_baseline_parser.run_baseline(ZTE_VENDOR, "3G")
# main_baseline_parser.run_baseline(ZTE_VENDOR, "4G")
# main_baseline_parser.run_baseline(ZTE_VENDOR, "5G")

# main_baseline_parser.run_baseline(HUAWEI_VENDOR, "2G")
# main_baseline_parser.run_baseline(HUAWEI_VENDOR, "3G")
# main_baseline_parser.run_baseline(HUAWEI_VENDOR, "4G")
log.i("Done all")