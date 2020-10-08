import glob
import re

import yaml
from pandas import *

import log
from environment import *
from scr.config.model.raw_file import RawFile

VENDOR = 'Vendor'
DEFAULT_MAPPING_SHEET_NAME = 0


def read_mapping_yaml():
    with open(MAPPING_FILE_PATH, 'r') as f:
        # doc = yaml.load(f)
        doc = yaml.safe_load(f)

    return doc


def read_data_source(vendor):

    if vendor == ERICSSON_SW_VENDOR or vendor == ERICSSON_FEATURE_VENDOR:
        vendor = ERICSSON_VENDOR

    yaml_value = read_mapping_yaml()

    xls = ExcelFile(RAW_FILE_PATH_COLLECTION)

    data_frame = xls.parse(xls.sheet_names[DEFAULT_MAPPING_SHEET_NAME])

    # print(data_frame.iterrows())

    raw_file_collection = []

    for index, row in data_frame.iterrows():

        # log.d("VENDOR : " + row[VENDOR].upper(), vendor)
        # log.d("vendor : " + vendor.upper(), vendor)

        if row[VENDOR].upper() != vendor.upper():
            continue

        raw_file = RawFile(row)
        # Read file mapping path from yaml
        

        try:
            if raw_file.FrequencyType in yaml_value[vendor]['network']:
                raw_file.FileMappingPath = CONFIGURATION_PATH + yaml_value[vendor]['network'][raw_file.FrequencyType]['mapping']
            if 'feature' in yaml_value[vendor] and raw_file.FrequencyType in yaml_value[vendor]['feature']:
                raw_file.FileMappingFeaturePath = CONFIGURATION_PATH + yaml_value[vendor]['feature'][raw_file.FrequencyType]['mapping']
        except:
            pass

        # Get valid file name in directory based on file format
        collect_file_within_path(vendor, raw_file)
        raw_file_collection.append(raw_file)

    return raw_file_collection


def collect_file_within_path(vendor, raw_file):
    log.i(" ", vendor)
    log.i(vendor + "_" + raw_file.FrequencyType + "_" + raw_file.Region, vendor)
    log.i("Path : " + raw_file.Path, vendor)

    if vendor == ZTE_VENDOR:
        return collect_zte_file(raw_file)
    elif vendor == HUAWEI_VENDOR:
        return collect_huawei_file(raw_file)
    elif vendor == ERICSSON_VENDOR:
        return collect_ericsson_file(raw_file)


def collect_huawei_file(raw_file):
    if raw_file.FrequencyType == '2G':
        collect_huawei_file_by_extension(raw_file, 'txt')
    elif raw_file.FrequencyType == '3G':
        collect_huawei_file_by_extension(raw_file, 'txt')
    elif raw_file.FrequencyType == '4G':
        collect_huawei_file_by_extension(raw_file, 'xml')

    # print(raw_file)


def collect_ericsson_file(raw_file):
    if raw_file.FrequencyType == '2G':
        collect_ericsson_2g_file(raw_file)
    elif raw_file.FrequencyType == '3G':
        collect_ericsson_file_by_extension(raw_file, 'log')
    elif raw_file.FrequencyType == '4G':
        collect_ericsson_file_by_extension(raw_file, 'log')
    elif raw_file.FrequencyType == '5G':
        collect_ericsson_file_by_extension(raw_file, 'log')


def collect_zte_file(raw_file):
    if raw_file.Path != '':
        for filename in glob.glob(raw_file.Path + '*.xml'):
            if re.match(raw_file.FileFormat.upper(), filename.upper()):
                raw_file.RawFileList.append(filename)

        log.i("Read file count : " + str(raw_file.RawFileList.__len__()), ZTE_VENDOR)


def collect_ericsson_2g_file(raw_file):
    if raw_file.Path != '':
        for filename in glob.glob(raw_file.Path + '*.txt'):
            raw_file.RawFileList.append(filename)

        for filename in glob.glob(raw_file.Path + '*.log'):
            raw_file.RawFileList.append(filename)


def collect_huawei_file_by_extension(raw_file, file_extension):
    file_name_format = raw_file.FileFormat.split('.')[0]
    if raw_file.Path != '':
        for filename in glob.glob(raw_file.Path + '*.' + file_extension):
            raw_file.RawFileList.append(filename)

        for filename in glob.glob(raw_file.Path + '*.' + file_extension.upper()):
            raw_file.RawFileList.append(filename)

        log.i("Read file count : " + str(raw_file.RawFileList.__len__()), HUAWEI_VENDOR)


def collect_ericsson_file_by_extension(raw_file, file_extension):
    if raw_file.Path != '':
        for filename in glob.glob(raw_file.Path + '*.' + file_extension):
            raw_file.RawFileList.append(filename)

        log.i("Read file count : " + str(raw_file.RawFileList.__len__()), ERICSSON_VENDOR)
