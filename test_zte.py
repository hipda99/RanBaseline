#!/usr/bin/python3
import datetime

import log
from environment import ERICSSON_VENDOR
from scr.parser import main_baseline_parser
from scr.util import parser_db
import os

from scr.parser.zte import zte_baseline_parser

# parser_db.update_status(ERICSSON_VENDOR, 'Baseline_5G', parser_db.STATUS_OPEN)
# parser_db.update_status(ERICSSON_VENDOR, 'Baseline_5G', parser_db.STATUS_CLOSE)


zte_baseline_parser.parse_5g('D:/Libraries/git/true/RanBaselineApp/test/UMEID_ITBBU_ZTE_20201019060000-001.xml', '5G', {'EnDCCtrl': ['ASPSCELLSWCH', 'BASELINE_TYPE', 'REFERENCE_FIELD'] }, {'EnDCCtrl': 'CELL Level'})