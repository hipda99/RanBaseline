import pprint

from pandas import *

from scr.helper import naming_helper
from scr.util.excel_reader import read_excel_mapping

PARAMETER_GROUP_COLUMN_NAME = 'MO'
PARAMETER_COLUMN_NAME = 'Parameter'
BASELINE_COLUMN_NAME = 'Baseline_SKA'
RED_ZONE_BASELINE_COLUMN_NAME = 'Baseline_RedZone'
LEVEL_COLUMN_NAME = 'Level'

BASELINE = 'baseline'
RED_ZONE = 'redzone'

BASELINE_TYPE = 'BASELINE_TYPE'
REFERENCE_FIELD_COLUMN_NAME = 'REFERENCE_FIELD'


BASELINE_3G_850_COLUMN = 'Baseline_850'
BASELINE_3G_2100_SKA_COLUMN = 'Baseline_2100_SKA'
BASELINE_3G_2100_REDZONE_COLUMN = 'Baseline_2100_RedZone'


BASELINE_850_TYPE = 'baseline_850'
RED_ZONE_2100_TYPE = 'redzone_2100'
BASELINE_2100_TYPE = 'baseline_2100'

BASELINE_LABEL_TYPE = 'label'


def read_zte_mapping(file_mapping_path_name, frequency_type, frequency):
    if frequency_type == '3G':
        return read_3g(file_mapping_path_name)
    else:
        return read(file_mapping_path_name)


def read_3g(file_mapping_path_name):
    df = read_excel_mapping(file_mapping_path_name, 0)

    param_dic = {}
    baseline_label_dic = {}
    baseline_850_dic = {}
    baseline_2100_dic = {}
    red_baseline_2100_dic = {}
    cell_level_dic = {}
    tmp_check_key_dic = {}

    for index, row in df.iterrows():
        param_group = str(row[PARAMETER_GROUP_COLUMN_NAME])

        baseline_label_value = str(row[PARAMETER_COLUMN_NAME])
        param_name = naming_helper.rule_column_name(baseline_label_value)

        cell_level = str(row[LEVEL_COLUMN_NAME])

        baseline_value_850 = str(row[BASELINE_3G_850_COLUMN])
        if baseline_value_850 == 'nan':
            baseline_value_850 = ""
        baseline_value_2100 = str(row[BASELINE_3G_2100_SKA_COLUMN])
        if baseline_value_2100 == 'nan':
            baseline_value_2100 = ""
        red_zone_value_2100 = str(row[BASELINE_3G_2100_REDZONE_COLUMN])

        if red_zone_value_2100 == 'nan':
            red_zone_value_2100 = ""

        if baseline_label_value == "nan":
            baseline_label_value = ""

        param_name = param_name.upper()
        
        if param_group.upper() in tmp_check_key_dic:

            if param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                param_dic[param_group].append(param_name)
                baseline_850_dic[param_group][0][param_name] = baseline_value_850
                baseline_850_dic[param_group][0][BASELINE_TYPE] = BASELINE_850_TYPE

                baseline_2100_dic[param_group][0][param_name] = baseline_value_2100
                baseline_2100_dic[param_group][0][BASELINE_TYPE] = BASELINE_2100_TYPE

                red_baseline_2100_dic[param_group][0][param_name] = red_zone_value_2100
                red_baseline_2100_dic[param_group][0][BASELINE_TYPE] = RED_ZONE_2100_TYPE

                baseline_label_dic[param_group][0][param_name] = baseline_label_value
                baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE

                tmp_check_key_dic[param_group.upper()].append(param_name.upper())
        else:
            param_dic[param_group] = [param_name]
            param_dic[param_group].append(BASELINE_TYPE)
            param_dic[param_group].append(REFERENCE_FIELD_COLUMN_NAME)
            cell_level_dic[param_group] = cell_level

            baseline_850_dic[param_group] = [{param_name: baseline_value_850}]
            baseline_850_dic[param_group][0][BASELINE_TYPE] = BASELINE_850_TYPE

            baseline_2100_dic[param_group] = [{param_name: baseline_value_2100}]
            baseline_2100_dic[param_group][0][BASELINE_TYPE] = BASELINE_2100_TYPE

            red_baseline_2100_dic[param_group] = [{param_name: red_zone_value_2100}]
            red_baseline_2100_dic[param_group][0][BASELINE_TYPE] = RED_ZONE_2100_TYPE

            baseline_label_dic[param_group] = [{param_name: baseline_label_value}]
            baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE

            tmp_check_key_dic[param_group.upper()] = [param_name.upper()]

    return param_dic, baseline_850_dic,baseline_2100_dic, red_baseline_2100_dic, cell_level_dic, baseline_label_dic


def read(file_mapping_path_name):
    df = read_excel_mapping(file_mapping_path_name, 0)

    baseline_label_dic = {}
    param_dic = {}
    baseline_dic = {}
    red_baseline_dic = {}
    cell_level_dic = {}
    tmp_check_key_dic = {}

    for index, row in df.iterrows():
        param_group = str(row[PARAMETER_GROUP_COLUMN_NAME])

        baseline_label_value = str(row[PARAMETER_COLUMN_NAME])
        param_name = naming_helper.rule_column_name(baseline_label_value)

        cell_level = str(row[LEVEL_COLUMN_NAME])
        param_name = naming_helper.rule_column_name(param_name)

        baseline_value = str(row[BASELINE_COLUMN_NAME])
        if baseline_value == 'nan':
            baseline_value = ""

        red_zone_value = str(row[RED_ZONE_BASELINE_COLUMN_NAME])
        if red_zone_value == 'nan':
            red_zone_value = ""

        if baseline_label_value == "nan":
            baseline_label_value = ""

        if param_group.upper() in tmp_check_key_dic:
            if param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                param_dic[param_group].append(param_name)
                baseline_dic[param_group][0][param_name] = baseline_value
                baseline_dic[param_group][0][BASELINE_TYPE] = BASELINE

                red_baseline_dic[param_group][0][param_name] = red_zone_value
                red_baseline_dic[param_group][0][BASELINE_TYPE] = RED_ZONE

                baseline_label_dic[param_group][0][param_name] = baseline_label_value
                baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE

                tmp_check_key_dic[param_group.upper()].append(param_name.upper())
        else:
            param_dic[param_group] = [param_name]
            param_dic[param_group].append(BASELINE_TYPE)
            param_dic[param_group].append(REFERENCE_FIELD_COLUMN_NAME)
            cell_level_dic[param_group] = cell_level

            baseline_dic[param_group] = [{param_name: baseline_value}]
            baseline_dic[param_group][0][BASELINE_TYPE] = BASELINE

            red_baseline_dic[param_group] = [{param_name: red_zone_value}]
            red_baseline_dic[param_group][0][BASELINE_TYPE] = RED_ZONE

            baseline_label_dic[param_group] = [{param_name: baseline_label_value}]
            baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE

            tmp_check_key_dic[param_group.upper()] = [param_name.upper()]

    return param_dic, baseline_dic, red_baseline_dic, cell_level_dic, baseline_label_dic
