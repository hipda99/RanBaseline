import pprint

from pandas import *

from scr.helper import naming_helper
from scr.util.excel_reader import read_excel_mapping

PARAMETER_GROUP_COLUMN_NAME = 'MO'
PARAMETER_COLUMN_NAME = 'Parameter'

# 4G
BASELINE_4G_COLUMN_NAME = 'Baseline_SKA'
RED_ZONE_4G_BASELINE_COLUMN_NAME = 'Baseline_RedZone'
LEVEL_COLUMN_NAME = 'Level'

BASELINE = 'baseline'
RED_ZONE = 'redzone'

BASELINE_TYPE = 'BASELINE_TYPE'
REFERENCE_FIELD_COLUMN_NAME = 'REFERENCE_FIELD'

# 2020CR - NR/TDD - Additional L2600, Anchor L1800
# Excel Column name
BASELINE_4G_2600_COLUMN = 'Baseline_2600'
BASELINE_4G_REDZONE_2600_COLUMN = 'Baseline_Redzone_2600'
BASELINE_4G_L900_ANCHOR_COLUMN = 'Baseline_900_Anchor'
BASELINE_4G_L1800_ANCHOR_COLUMN = 'Baseline_1800_Anchor'
BASELINE_4G_L2100_ANCHOR_COLUMN = 'Baseline_2100_Anchor'
BASELINE_4G_L2600_ANCHOR_COLUMN = 'Baseline_2600_Anchor'
# Database column label
L2600_BASELINE = 'baseline_2600'
REDZONE_L2600_BASELINE = 'baseline_redzone_2600'
# Add anchor for LTE (all band)
L900_ANCHOR = 'baseline_900_anchor'
L1800_ANCHOR = 'baseline_1800_anchor'
L2100_ANCHOR = 'baseline_2100_anchor'
L2600_ANCHOR = 'baseline_2600_anchor'

# 3G
BASELINE_3G_850_COLUMN = 'Baseline_850'
BASELINE_3G_2100_SKA_COLUMN = 'Baseline_2100_SKA'
BASELINE_3G_2100_REDZONE_COLUMN = 'Baseline_2100_RedZone'

BASELINE_3G_850_TYPE = 'baseline_850'
RED_ZONE_3G_2100_TYPE = 'redzone_2100'
BASELINE_3G_2100_TYPE = 'baseline_2100'
BASELINE_3G_2100_TYPE = 'baseline_2100'

# 5G
# 2020CR - NR/TDD- 5G NR
# Excel Column name
BASELINE_5G_2600_TYPE = 'baseline_2600'
BASELINE_5G_REDZONE_2600_TYPE = 'baseline_redzone_2600'

BASELINE_LABEL_TYPE = 'label'


def read_zte_mapping(file_mapping_path_name, frequency_type, frequency):
    if frequency_type == '3G':
        return read_3g(file_mapping_path_name)
    elif frequency_type == '4G':
        return read_4g(file_mapping_path_name)
    elif frequency_type == '5G':
        return read_5g(file_mapping_path_name)

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
                baseline_850_dic[param_group][0][BASELINE_TYPE] = BASELINE_3G_850_TYPE

                baseline_2100_dic[param_group][0][param_name] = baseline_value_2100
                baseline_2100_dic[param_group][0][BASELINE_TYPE] = BASELINE_3G_2100_TYPE

                red_baseline_2100_dic[param_group][0][param_name] = red_zone_value_2100
                red_baseline_2100_dic[param_group][0][BASELINE_TYPE] = RED_ZONE_3G_2100_TYPE

                baseline_label_dic[param_group][0][param_name] = baseline_label_value
                baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE

                tmp_check_key_dic[param_group.upper()].append(param_name.upper())
        else:
            param_dic[param_group] = [param_name]
            param_dic[param_group].append(BASELINE_TYPE)
            param_dic[param_group].append(REFERENCE_FIELD_COLUMN_NAME)
            cell_level_dic[param_group] = cell_level

            baseline_850_dic[param_group] = [{param_name: baseline_value_850}]
            baseline_850_dic[param_group][0][BASELINE_TYPE] = BASELINE_3G_850_TYPE

            baseline_2100_dic[param_group] = [{param_name: baseline_value_2100}]
            baseline_2100_dic[param_group][0][BASELINE_TYPE] = BASELINE_3G_2100_TYPE

            red_baseline_2100_dic[param_group] = [{param_name: red_zone_value_2100}]
            red_baseline_2100_dic[param_group][0][BASELINE_TYPE] = RED_ZONE_3G_2100_TYPE

            baseline_label_dic[param_group] = [{param_name: baseline_label_value}]
            baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE

            tmp_check_key_dic[param_group.upper()] = [param_name.upper()]

    return param_dic, baseline_850_dic,baseline_2100_dic, red_baseline_2100_dic, cell_level_dic, baseline_label_dic


def read_4g(file_mapping_path_name):
    df = read_excel_mapping(file_mapping_path_name, 0)

    baseline_label_dic = {}
    param_dic = {}
    baseline_dic = {}
    red_baseline_dic = {}
    baseline_2600_dic = {}
    baseline_redzone_2600_dic = {}
    baseline_l900_anchor_dic = {}
    baseline_l1800_anchor_dic = {}
    baseline_l2100_anchor_dic = {}
    baseline_l2600_anchor_dic = {}
    cell_level_dic = {}
    tmp_check_key_dic = {}

    for index, row in df.iterrows():
        param_group = str(row[PARAMETER_GROUP_COLUMN_NAME])

        baseline_label_value = str(row[PARAMETER_COLUMN_NAME])
        param_name = naming_helper.rule_column_name(baseline_label_value)

        cell_level = str(row[LEVEL_COLUMN_NAME])
        param_name = naming_helper.rule_column_name(param_name)

        baseline_value = str(row[BASELINE_4G_COLUMN_NAME])
        if baseline_value == 'nan':
            baseline_value = ""

        red_zone_value = str(row[RED_ZONE_4G_BASELINE_COLUMN_NAME])
        if red_zone_value == 'nan':
            red_zone_value = ""

        if baseline_label_value == "nan":
            baseline_label_value = ""

        # CR2020 - Add 4G 2600 & Anchor
        l2600_value = str(row[BASELINE_4G_2600_COLUMN])
        if l2600_value == 'nan':
            l2600_value = ""

        redzone_l2600_value = str(row[BASELINE_4G_REDZONE_2600_COLUMN])
        if redzone_l2600_value == 'nan':
            redzone_l2600_value = ""

        l900_anchor_value = str(row[BASELINE_4G_L900_ANCHOR_COLUMN])
        if l900_anchor_value == 'nan':
            l900_anchor_value = ""

        l1800_anchor_value = str(row[BASELINE_4G_L1800_ANCHOR_COLUMN])
        if l1800_anchor_value == 'nan':
            l1800_anchor_value = ""

        l2100_anchor_value = str(row[BASELINE_4G_L2100_ANCHOR_COLUMN])
        if l2100_anchor_value == 'nan':
            l2100_anchor_value = ""

        l2600_anchor_value = str(row[BASELINE_4G_L900_ANCHOR_COLUMN])
        if l2600_anchor_value == 'nan':
            l2600_anchor_value = ""


        if param_group.upper() in tmp_check_key_dic:
            if param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                param_dic[param_group].append(param_name)
                baseline_dic[param_group][0][param_name] = baseline_value
                baseline_dic[param_group][0][BASELINE_TYPE] = BASELINE

                red_baseline_dic[param_group][0][param_name] = red_zone_value
                red_baseline_dic[param_group][0][BASELINE_TYPE] = RED_ZONE

                baseline_label_dic[param_group][0][param_name] = baseline_label_value
                baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE

                # CR2020 - Add L2600 & Anchor
                baseline_2600_dic[param_group][0][param_name] = l2600_value
                baseline_2600_dic[param_group][0][BASELINE_TYPE] = L2600_BASELINE
                baseline_redzone_2600_dic[param_group][0][param_name] = redzone_l2600_value
                baseline_redzone_2600_dic[param_group][0][BASELINE_TYPE] = REDZONE_L2600_BASELINE
                baseline_l900_anchor_dic[param_group][0][param_name] = l900_anchor_value
                baseline_l900_anchor_dic[param_group][0][BASELINE_TYPE] = L900_ANCHOR
                baseline_l1800_anchor_dic[param_group][0][param_name] = l1800_anchor_value
                baseline_l1800_anchor_dic[param_group][0][BASELINE_TYPE] = L1800_ANCHOR
                baseline_l2100_anchor_dic[param_group][0][param_name] = l2100_anchor_value
                baseline_l2100_anchor_dic[param_group][0][BASELINE_TYPE] = L2100_ANCHOR
                baseline_l2600_anchor_dic[param_group][0][param_name] = l2600_anchor_value
                baseline_l2600_anchor_dic[param_group][0][BASELINE_TYPE] = L2600_ANCHOR

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

            # CR2020 - Add L2600 & Anchor
            baseline_2600_dic[param_group] = [{param_name: l2600_value}]
            baseline_2600_dic[param_group][0][BASELINE_TYPE] = L2600_BASELINE
            baseline_l900_anchor_dic[param_group] = [{param_name: l900_anchor_value}]
            baseline_l900_anchor_dic[param_group][0][BASELINE_TYPE] = L900_ANCHOR
            baseline_l1800_anchor_dic[param_group] = [{param_name: l1800_anchor_value}]
            baseline_l1800_anchor_dic[param_group][0][BASELINE_TYPE] = L1800_ANCHOR
            baseline_l2100_anchor_dic[param_group] = [{param_name: l2100_anchor_value}]
            baseline_l2100_anchor_dic[param_group][0][BASELINE_TYPE] = L2100_ANCHOR
            baseline_l2600_anchor_dic[param_group] = [{param_name: l2600_anchor_value}]
            baseline_l2600_anchor_dic[param_group][0][BASELINE_TYPE] = L2600_ANCHOR

            tmp_check_key_dic[param_group.upper()] = [param_name.upper()]

    return param_dic, baseline_dic, red_baseline_dic, baseline_2600_dic, baseline_redzone_2600_dic, baseline_l900_anchor_dic, baseline_l1800_anchor_dic, baseline_l2100_anchor_dic, baseline_l2600_anchor_dic, cell_level_dic, baseline_label_dic

def read_5g(file_mapping_path_name):
    df = read_excel_mapping(file_mapping_path_name, 0)

    param_dic = {}
    baseline_label_dic = {}
    baseline_2600_dic = {}
    baseline_redzone_2600_dic = {}
    cell_level_dic = {}
    tmp_check_key_dic = {}

    for index, row in df.iterrows():
        param_group = str(row[PARAMETER_GROUP_COLUMN_NAME])

        baseline_label_value = str(row[PARAMETER_COLUMN_NAME])
        param_name = naming_helper.rule_column_name(baseline_label_value)

        cell_level = str(row[LEVEL_COLUMN_NAME])

        baseline_value_2600 = str(row[BASELINE_5G_2600_TYPE])
        if baseline_value_2600 == 'nan':
            baseline_value_2600 = ""

        baseline_value_redzone_2600 = str(row[BASELINE_5G_REDZONE_2600_TYPE])
        if baseline_value_redzone_2600 == 'nan':
            baseline_value_redzone_2600 = ""

        param_name = param_name.upper()

        if param_group.upper() in tmp_check_key_dic:

            if param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                param_dic[param_group].append(param_name)
                baseline_2600_dic[param_group][0][param_name] = baseline_value_2600
                baseline_2600_dic[param_group][0][BASELINE_TYPE] = BASELINE_5G_2600_TYPE

                baseline_redzone_2600_dic[param_group][0][param_name] = baseline_value_redzone_2600
                baseline_redzone_2600_dic[param_group][0][BASELINE_TYPE] = BASELINE_5G_REDZONE_2600_TYPE

                baseline_label_dic[param_group][0][param_name] = baseline_label_value
                baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE

                tmp_check_key_dic[param_group.upper()].append(param_name.upper())
        else:
            param_dic[param_group] = [param_name]
            param_dic[param_group].append(BASELINE_TYPE)
            param_dic[param_group].append(REFERENCE_FIELD_COLUMN_NAME)
            cell_level_dic[param_group] = cell_level

            baseline_2600_dic[param_group] = [{param_name: baseline_value_2600}]
            baseline_2600_dic[param_group][0][BASELINE_TYPE] = BASELINE_5G_2600_TYPE

            baseline_redzone_2600_dic[param_group] = [{param_name: baseline_value_redzone_2600}]
            baseline_redzone_2600_dic[param_group][0][BASELINE_TYPE] = BASELINE_5G_REDZONE_2600_TYPE

            baseline_label_dic[param_group] = [{param_name: baseline_label_value}]
            baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE

            tmp_check_key_dic[param_group.upper()] = [param_name.upper()]

    return param_dic, baseline_2600_dic, baseline_redzone_2600_dic, cell_level_dic, baseline_label_dic
