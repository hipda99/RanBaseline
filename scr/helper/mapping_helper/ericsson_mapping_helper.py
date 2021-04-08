from environment import *
from scr.helper import naming_helper
from scr.util.excel_reader import read_excel_mapping

PARAMETER_GROUP_COLUMN_NAME = 'Parameter Group'
PARAMETER_COLUMN_NAME = 'Parameter Name'
BASELINE_COLUMN_NAME = 'Expected Value'
EXPECTED_VALUE_COLUMN_NAME = 'Expected Value'
EXPECTED_700_VALUE_COLUMN_NAME = 'Expected Value(700)'
EXPECTED_900_VALUE_COLUMN_NAME = 'Expected Value(900)'
EXPECTED_1800_VALUE_COLUMN_NAME = 'Expected Value(1800)'
EXPECTED_2100_VALUE_COLUMN_NAME = 'Expected Value(2100)'

# Add 2600 and Anchor (for 4G)
EXPECTED_2600_VALUE_COLUMN_NAME = 'Expected Value(2600)'
EXPECTED_700_ANCHOR_VALUE_COLUMN_NAME = 'Expected Value(700_Anchor)'
EXPECTED_900_ANCHOR_VALUE_COLUMN_NAME = 'Expected Value(900_Anchor)'
EXPECTED_1800_ANCHOR_VALUE_COLUMN_NAME = 'Expected Value(1800_Anchor)'
EXPECTED_2100_ANCHOR_VALUE_COLUMN_NAME = 'Expected Value(2100_Anchor)'
EXPECTED_2600_ANCHOR_VALUE_COLUMN_NAME = 'Expected Value(2600_Anchor)'

FEATURE_PARAMETER_GROUP_COLUMN_NAME = 'TYPE'
FEATURE_PARAMETER_COLUMN_NAME = 'KeyID'
FEATURE_BASELINE_COLUMN_NAME = 'BASELINE'
FEATURE_DESCRIPTION_COLUMN_NAME = 'Description'
FEATURE_SERVICESTATE_COLUMN_NAME = 'ServiceState'
FEATURE_LICENSESTATE_COLUMN_NAME = 'LicenseState'

LEVEL_COLUMN_NAME = 'Level'

REFERENCE_FIELD_COLUMN_NAME = 'REFERENCE_FIELD'
FILENAME_FIELD_COLUMN_NAME = 'FILENAME'

BASELINE_TYPE = 'BASELINE_TYPE'
BASELINE_700_TYPE = 'baseline_700'
BASELINE_900_TYPE = 'baseline_900'
BASELINE_1800_TYPE = 'baseline_1800'
BASELINE_2100_TYPE = 'baseline_2100'
BASELINE_2600_TYPE = 'baseline_2600'
BASELINE_700_ANCHOR_TYPE = 'baseline_700_anchor'
BASELINE_900_ANCHOR_TYPE = 'baseline_900_anchor'
BASELINE_1800_ANCHOR_TYPE = 'baseline_1800_anchor'
BASELINE_2100_ANCHOR_TYPE = 'baseline_2100_anchor'
BASELINE_2600_ANCHOR_TYPE = 'baseline_2600_anchor'
BASELINE_LABEL_TYPE = 'label'
BASELINE_NORMAL_TYPE = 'baseline'
BASELINE_DESC_TYPE = 'baseline_desc'

LV_COLUMN = 'LV'

feature_column = [
    "NAME",
    "REFERENCE_FIELD",
    "FILENAME",

    "KEY_ID",
    "FEATURESTATE",
    "LICENSESTATE",
    "SERVICESTATE",
    "DESCRIPTION"

]


def read_ericsson_mapping(file_mapping_path_name, file_mapping_feature_path_name, frequency_type, frequency, vendor):
    if vendor == ERICSSON_FEATURE_VENDOR:
        return read_feature(file_mapping_feature_path_name)
    else:

        if frequency_type == "2G":
            return read_2g(file_mapping_path_name)
        elif frequency_type == "3G":
            return read_3g(file_mapping_path_name)
        elif frequency_type == '4G':
            return read_4g(file_mapping_path_name)        
        else:
            return read_5g(file_mapping_path_name)


def read_feature(file_mapping_path_name):
    df = read_excel_mapping(file_mapping_path_name, 0)

    # param_dic = [
    #     "NAME",
    #     "REFERENCE_FIELD",
    #     "FILENAME",

    #     "KEY_ID",
    #     "FEATURESTATE",
    #     "DESCRIPTION"        

    # ]
    param_dic = feature_column
    baseline_dic = []
    key_dic = []

    tmp_check_key_dic = {}

    for index, row in df.iterrows():
        param_group = str(row[FEATURE_PARAMETER_GROUP_COLUMN_NAME])
        # param_group = naming_helper.rule_column_name(param_group)

        param_name = str(row[FEATURE_PARAMETER_COLUMN_NAME])
        # param_name = naming_helper.rule_column_name(param_name)
        key_dic.append(param_name)

        baseline_value = str(row[FEATURE_BASELINE_COLUMN_NAME])
        baseline_desc_value = str(row[FEATURE_DESCRIPTION_COLUMN_NAME])
        license_value = ""
        service_value = ""
        if FEATURE_LICENSESTATE_COLUMN_NAME in row:
            license_value = str(row[FEATURE_LICENSESTATE_COLUMN_NAME])
        if FEATURE_SERVICESTATE_COLUMN_NAME in row:
            service_value = str(row[FEATURE_SERVICESTATE_COLUMN_NAME])        

        if baseline_value == "nan":
            baseline_value = ""
        
        if license_value == "nan":
            license_value = ""
        
        if service_value == "nan":
            service_value = ""

        if baseline_desc_value == "nan":
            baseline_desc_value = ""

        baseline_dic.append(
            {
                "KEY_ID": param_name,
                "FEATURESTATE": baseline_value,
                "LICENSESTATE": license_value,
                "SERVICESTATE": service_value,
                "DESCRIPTION": baseline_desc_value,
                "LV" : param_group
            }
        )

    return param_dic, baseline_dic, key_dic

def read_5g(file_mapping_path_name):
    df = read_excel_mapping(file_mapping_path_name, 0)

    param_dic = {}
    baseline_label_dic = {}
    baseline_700_dic = {}
    baseline_2600_dic = {}

    tmp_check_key_dic = {}

    param_cell_level = {}
    param_cell_mo = {}

    for index, row in df.iterrows():
        param_group = str(row[PARAMETER_GROUP_COLUMN_NAME])
        param_group = naming_helper.rule_column_name(param_group)

        baseline_label_value = str(row[PARAMETER_COLUMN_NAME])
        param_name = naming_helper.rule_column_name(baseline_label_value)

        baseline_700_value = str(row[EXPECTED_700_VALUE_COLUMN_NAME])
        baseline_2600_value = str(row[EXPECTED_2600_VALUE_COLUMN_NAME])
        

        if baseline_700_value == "nan":
            baseline_700_value = ""

        if baseline_2600_value == "nan":
            baseline_2600_value = ""

        if baseline_label_value == "nan":
            baseline_label_value = ""

        cell_level = str(row[LEVEL_COLUMN_NAME])

        param_cell_level[param_group] = cell_level

        if param_group.upper() in tmp_check_key_dic:
            if param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                param_dic[param_group].append(param_name)

                baseline_700_dic[param_group][0][param_name] = baseline_700_value
                baseline_700_dic[param_group][0][BASELINE_TYPE] = BASELINE_700_TYPE
                baseline_700_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_2600_dic[param_group][0][param_name] = baseline_2600_value
                baseline_2600_dic[param_group][0][BASELINE_TYPE] = BASELINE_2600_TYPE
                baseline_2600_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_label_dic[param_group][0][param_name] = baseline_label_value
                baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE
                baseline_label_dic[param_group][0][LV_COLUMN] = cell_level

                tmp_check_key_dic[param_group.upper()].append(param_name.upper())

        else:

            param_dic[param_group] = [param_name]

            param_dic[param_group].append("FILENAME")
            param_dic[param_group].append("REFERENCE_FIELD")
            param_dic[param_group].append(BASELINE_TYPE)

            if param_name == "MO":
                param_cell_mo[param_group] = baseline_2600_value
            
            baseline_700_dic[param_group] = [{param_name: baseline_700_value}]
            baseline_700_dic[param_group][0][BASELINE_TYPE] = BASELINE_700_TYPE
            baseline_700_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_2600_dic[param_group] = [{param_name: baseline_2600_value}]
            baseline_2600_dic[param_group][0][BASELINE_TYPE] = BASELINE_2600_TYPE
            baseline_2600_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_label_dic[param_group] = [{param_name: baseline_label_value}]
            baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE
            baseline_label_dic[param_group][0][LV_COLUMN] = cell_level

            tmp_check_key_dic[param_group.upper()] = [param_name.upper()]

    return param_dic, baseline_700_dic, baseline_2600_dic, param_cell_level, param_cell_mo, baseline_label_dic

def read_4g(file_mapping_path_name):
    df = read_excel_mapping(file_mapping_path_name, 0)

    param_dic = {}
    baseline_label_dic = {}
    baseline_700_dic = {}
    baseline_900_dic = {}
    baseline_1800_dic = {}
    baseline_2100_dic = {}
    baseline_2600_dic = {}
    baseline_700_anchor_dic = {}
    baseline_900_anchor_dic = {}
    baseline_1800_anchor_dic = {}
    baseline_2100_anchor_dic = {}
    baseline_2600_anchor_dic = {}

    tmp_check_key_dic = {}

    param_cell_level = {}
    param_cell_mo = {}

    for index, row in df.iterrows():
        param_group = str(row[PARAMETER_GROUP_COLUMN_NAME])
        param_group = naming_helper.rule_column_name(param_group)

        baseline_label_value = str(row[PARAMETER_COLUMN_NAME])
        param_name = naming_helper.rule_column_name(baseline_label_value)

        baseline_700_value = str(row[EXPECTED_700_VALUE_COLUMN_NAME])
        baseline_900_value = str(row[EXPECTED_900_VALUE_COLUMN_NAME])
        baseline_1800_value = str(row[EXPECTED_1800_VALUE_COLUMN_NAME])
        baseline_2100_value = str(row[EXPECTED_2100_VALUE_COLUMN_NAME])        
        baseline_2600_value = str(row[EXPECTED_2600_VALUE_COLUMN_NAME])

        if baseline_700_value == "nan":
            baseline_700_value = ""

        if baseline_900_value == "nan":
            baseline_900_value = ""

        if baseline_1800_value == "nan":
            baseline_1800_value = ""

        if baseline_2100_value == "nan":
            baseline_2100_value = ""

        if baseline_2600_value == "nan":
            baseline_2600_value = ""

        # Anchor
        baseline_700_anchor_value = str(row[EXPECTED_700_ANCHOR_VALUE_COLUMN_NAME])
        baseline_900_anchor_value = str(row[EXPECTED_900_ANCHOR_VALUE_COLUMN_NAME])
        baseline_1800_anchor_value = str(row[EXPECTED_1800_ANCHOR_VALUE_COLUMN_NAME])
        baseline_2100_anchor_value = str(row[EXPECTED_2100_ANCHOR_VALUE_COLUMN_NAME])
        baseline_2600_anchor_value = str(row[EXPECTED_2600_ANCHOR_VALUE_COLUMN_NAME])
        
        if baseline_700_anchor_value == "nan":
            baseline_700_anchor_value = ""
        if baseline_900_anchor_value == "nan":
            baseline_900_anchor_value = ""
        if baseline_1800_anchor_value == "nan":
            baseline_1800_anchor_value = ""
        if baseline_2100_anchor_value == "nan":
            baseline_2100_anchor_value = ""
        if baseline_2600_anchor_value == "nan":
            baseline_2600_anchor_value = ""

        if baseline_label_value == "nan":
            baseline_label_value = ""

        cell_level = str(row[LEVEL_COLUMN_NAME])

        param_cell_level[param_group] = cell_level

        if param_group.upper() in tmp_check_key_dic:
            if param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                param_dic[param_group].append(param_name)

                baseline_700_dic[param_group][0][param_name] = baseline_700_value
                baseline_700_dic[param_group][0][BASELINE_TYPE] = BASELINE_700_TYPE
                baseline_700_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_900_dic[param_group][0][param_name] = baseline_900_value
                baseline_900_dic[param_group][0][BASELINE_TYPE] = BASELINE_900_TYPE
                baseline_900_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_1800_dic[param_group][0][param_name] = baseline_1800_value
                baseline_1800_dic[param_group][0][BASELINE_TYPE] = BASELINE_1800_TYPE
                baseline_1800_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_2100_dic[param_group][0][param_name] = baseline_2100_value
                baseline_2100_dic[param_group][0][BASELINE_TYPE] = BASELINE_2100_TYPE
                baseline_2100_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_2600_dic[param_group][0][param_name] = baseline_2600_value
                baseline_2600_dic[param_group][0][BASELINE_TYPE] = BASELINE_2600_TYPE
                baseline_2600_dic[param_group][0][LV_COLUMN] = cell_level

                # Anchor
                baseline_700_anchor_dic[param_group][0][param_name] = baseline_700_anchor_value
                baseline_700_anchor_dic[param_group][0][BASELINE_TYPE] = BASELINE_700_ANCHOR_TYPE
                baseline_700_anchor_dic[param_group][0][LV_COLUMN] = cell_level
                baseline_900_anchor_dic[param_group][0][param_name] = baseline_900_anchor_value
                baseline_900_anchor_dic[param_group][0][BASELINE_TYPE] = BASELINE_900_ANCHOR_TYPE
                baseline_900_anchor_dic[param_group][0][LV_COLUMN] = cell_level
                baseline_1800_anchor_dic[param_group][0][param_name] = baseline_1800_anchor_value
                baseline_1800_anchor_dic[param_group][0][BASELINE_TYPE] = BASELINE_1800_ANCHOR_TYPE
                baseline_1800_anchor_dic[param_group][0][LV_COLUMN] = cell_level
                baseline_2100_anchor_dic[param_group][0][param_name] = baseline_2100_anchor_value
                baseline_2100_anchor_dic[param_group][0][BASELINE_TYPE] = BASELINE_2100_ANCHOR_TYPE
                baseline_2100_anchor_dic[param_group][0][LV_COLUMN] = cell_level
                baseline_2600_anchor_dic[param_group][0][param_name] = baseline_2600_anchor_value
                baseline_2600_anchor_dic[param_group][0][BASELINE_TYPE] = BASELINE_2600_ANCHOR_TYPE
                baseline_2600_anchor_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_label_dic[param_group][0][param_name] = baseline_label_value
                baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE
                baseline_label_dic[param_group][0][LV_COLUMN] = cell_level

                tmp_check_key_dic[param_group.upper()].append(param_name.upper())

        else:

            param_dic[param_group] = [param_name]

            param_dic[param_group].append("FILENAME")
            param_dic[param_group].append("REFERENCE_FIELD")
            param_dic[param_group].append(BASELINE_TYPE)

            if param_name == "MO":
                param_cell_mo[param_group] = baseline_900_value

            baseline_700_dic[param_group] = [{param_name: baseline_700_value}]
            baseline_700_dic[param_group][0][BASELINE_TYPE] = BASELINE_700_TYPE
            baseline_700_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_900_dic[param_group] = [{param_name: baseline_900_value}]
            baseline_900_dic[param_group][0][BASELINE_TYPE] = BASELINE_900_TYPE
            baseline_900_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_1800_dic[param_group] = [{param_name: baseline_1800_value}]
            baseline_1800_dic[param_group][0][BASELINE_TYPE] = BASELINE_1800_TYPE
            baseline_1800_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_2100_dic[param_group] = [{param_name: baseline_2100_value}]
            baseline_2100_dic[param_group][0][BASELINE_TYPE] = BASELINE_2100_TYPE
            baseline_2100_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_2600_dic[param_group] = [{param_name: baseline_2600_value}]
            baseline_2600_dic[param_group][0][BASELINE_TYPE] = BASELINE_2600_TYPE
            baseline_2600_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_700_anchor_dic[param_group] = [{param_name: baseline_700_anchor_value}]
            baseline_700_anchor_dic[param_group][0][BASELINE_TYPE] = BASELINE_700_ANCHOR_TYPE
            baseline_700_anchor_dic[param_group][0][LV_COLUMN] = cell_level
            baseline_900_anchor_dic[param_group] = [{param_name: baseline_900_anchor_value}]
            baseline_900_anchor_dic[param_group][0][BASELINE_TYPE] = BASELINE_900_ANCHOR_TYPE
            baseline_900_anchor_dic[param_group][0][LV_COLUMN] = cell_level
            baseline_1800_anchor_dic[param_group] = [{param_name: baseline_1800_anchor_value}]
            baseline_1800_anchor_dic[param_group][0][BASELINE_TYPE] = BASELINE_1800_ANCHOR_TYPE
            baseline_1800_anchor_dic[param_group][0][LV_COLUMN] = cell_level
            baseline_2100_anchor_dic[param_group] = [{param_name: baseline_2100_anchor_value}]
            baseline_2100_anchor_dic[param_group][0][BASELINE_TYPE] = BASELINE_2100_ANCHOR_TYPE
            baseline_2100_anchor_dic[param_group][0][LV_COLUMN] = cell_level
            baseline_2600_anchor_dic[param_group] = [{param_name: baseline_2600_anchor_value}]
            baseline_2600_anchor_dic[param_group][0][BASELINE_TYPE] = BASELINE_2600_ANCHOR_TYPE
            baseline_2600_anchor_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_label_dic[param_group] = [{param_name: baseline_label_value}]
            baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE
            baseline_label_dic[param_group][0][LV_COLUMN] = cell_level

            tmp_check_key_dic[param_group.upper()] = [param_name.upper()]

    return param_dic, baseline_700_dic, baseline_900_dic, baseline_1800_dic, baseline_2100_dic, baseline_2600_dic, baseline_700_anchor_dic, baseline_900_anchor_dic, baseline_1800_anchor_dic, baseline_2100_anchor_dic, baseline_2600_anchor_dic, param_cell_level, param_cell_mo, baseline_label_dic


def read_3g(file_mapping_path_name):
    df = read_excel_mapping(file_mapping_path_name, 0)

    param_dic = {}
    baseline_dic = {}
    baseline_label_dic = {}

    tmp_check_key_dic = {}

    param_cell_level = {}
    param_cell_mo = {}

    for index, row in df.iterrows():
        param_group = str(row[PARAMETER_GROUP_COLUMN_NAME])
        param_group = naming_helper.rule_column_name(param_group)

        baseline_label_value = str(row[PARAMETER_COLUMN_NAME])
        param_name = naming_helper.rule_column_name(baseline_label_value)

        baseline_value = str(row[EXPECTED_VALUE_COLUMN_NAME])

        if baseline_value == "nan":
            baseline_value = ""

        cell_level = str(row[LEVEL_COLUMN_NAME])

        param_cell_level[param_group] = cell_level

        if param_group.upper() in tmp_check_key_dic:
            if param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                param_dic[param_group].append(param_name)

                baseline_dic[param_group][0][param_name] = baseline_value
                baseline_label_dic[param_group][0][param_name] = baseline_label_value

                tmp_check_key_dic[param_group.upper()].append(param_name.upper())


        else:

            param_dic[param_group] = [param_name]

            param_dic[param_group].append("FILENAME")
            param_dic[param_group].append("REFERENCE_FIELD")

            if param_name == "MO":
                param_cell_mo[param_group] = baseline_value

            baseline_dic[param_group] = [{param_name: baseline_value}]
            baseline_label_dic[param_group] = [{param_name: baseline_label_value}]
            baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = "label"

            tmp_check_key_dic[param_group.upper()] = [param_name.upper()]

    return param_dic, baseline_dic, param_cell_level, param_cell_mo, baseline_label_dic


def read_2g(file_mapping_path_name):
    df = read_excel_mapping(file_mapping_path_name, 0)

    param_dic = {}
    baseline_label_dic = {}
    baseline_dic = {}
    cell_level_dic = {}
    tmp_check_key_dic = {}
    param_cell_mo = {}

    for index, row in df.iterrows():
        param_group = str(row[PARAMETER_GROUP_COLUMN_NAME]).upper()

        baseline_label_value = str(row[PARAMETER_COLUMN_NAME])
        param_name = str(row[PARAMETER_COLUMN_NAME]).upper()
        cell_level = str(row[LEVEL_COLUMN_NAME]).upper()

        # log.i(param_group, ERICSSON_VENDOR)

        baseline_value = str(row[BASELINE_COLUMN_NAME]).strip()

        if baseline_value == "nan":
            baseline_value = ""

        if param_group.upper() in tmp_check_key_dic:
            if param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                param_dic[param_group.upper()].append(param_name.upper())
                baseline_dic[param_group][0][param_name.upper()] = baseline_value
                baseline_label_dic[param_group][0][param_name.upper()] = baseline_label_value

                tmp_check_key_dic[param_group.upper()].append(param_name.upper())
        else:
            param_dic[param_group] = [param_name]
            param_dic[param_group].append(REFERENCE_FIELD_COLUMN_NAME)
            param_dic[param_group].append(FILENAME_FIELD_COLUMN_NAME)
            cell_level_dic[param_group] = cell_level

            baseline_dic[param_group] = [{param_name: baseline_value}]
            baseline_label_dic[param_group] = [{param_name: baseline_label_value}]
            baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = "label"

            tmp_check_key_dic[param_group.upper()] = [param_name.upper()]

    return param_dic, baseline_dic, cell_level_dic, param_cell_mo, baseline_label_dic
