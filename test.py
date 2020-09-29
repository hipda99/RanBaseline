#!/usr/bin/python3
import datetime

import log
from environment import ERICSSON_VENDOR
from scr.parser import main_baseline_parser
from scr.util import parser_db
import os


parser_db.update_status(ERICSSON_VENDOR, 'Baseline_5G', parser_db.STATUS_OPEN)
parser_db.update_status(ERICSSON_VENDOR, 'Baseline_5G', parser_db.STATUS_CLOSE)