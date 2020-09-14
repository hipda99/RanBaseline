from environment import ZTE_VENDOR, HUAWEI_VENDOR, ERICSSON_VENDOR, ERICSSON_SW_VENDOR, ERICSSON_FEATURE_VENDOR
from scr.helper.mapping_helper.ericsson_mapping_helper import read_ericsson_mapping
from scr.helper.mapping_helper.huawei_mapping_helper import read_huawei_mapping
from scr.helper.mapping_helper.zte_mapping_helper import read_zte_mapping

PARAMETER_GROUP_COLUMN_NAME = 'Mo'
PARAMETER_COLUMN_NAME = 'Parameter'
BASELINE_COLUMN_NAME = 'Baseline_SKA'
RED_ZONE_BASELINE_COLUMN_NAME = 'Baseline_RedZone'


def read_mapping(vendor, file_mapping_path_name, frequency_type="", frequency="", file_mapping_feature_path_name =""):
    if vendor == ZTE_VENDOR:
        return read_zte_mapping(file_mapping_path_name, frequency_type, frequency)
    if vendor == HUAWEI_VENDOR:
        return read_huawei_mapping(file_mapping_path_name, frequency_type, frequency)
    if vendor == ERICSSON_VENDOR:
        return read_ericsson_mapping(file_mapping_path_name, file_mapping_feature_path_name, frequency_type, frequency, vendor)

    if vendor == ERICSSON_FEATURE_VENDOR:
        return read_ericsson_mapping(file_mapping_path_name, file_mapping_feature_path_name, frequency_type, frequency, vendor)
