from scr.helper import naming_helper
from scr.helper.naming_helper import get_huawei_comment_column_name
from scr.util.excel_reader import read_excel_mapping

PARAMETER_GROUP_COLUMN_NAME = 'ParameterGroup'
PARAMETER_COLUMN_NAME = 'ParameterName'
COMMENT_COLUMN_NAME = 'Comment'

EXPECTED_VALUE_COLUMN_NAME = "ExpectedValue"
EXPECTED_900_VALUE_COLUMN_NAME = 'Expected Value(900)'
EXPECTED_1800_VALUE_COLUMN_NAME = 'Expected Value(1800)'
EXPECTED_2100_VALUE_COLUMN_NAME = 'Expected Value(2100)'
EXPECTED_2600_VALUE_COLUMN_NAME = 'Expected Value(2600)'

EXPECTED_850BMA_VALUE_COLUMN_NAME = 'Expected Value (850_BMA)'
EXPECTED_850UPC_VALUE_COLUMN_NAME = 'Expected Value (850_UPC)'

REFERENCE_FIELD_COLUMN_NAME = 'REFERENCE_FIELD'

COMMENT_VALUE_COLUMN_NAME = 'CommentValue'
LEVEL_COLUMN_NAME = 'Level'
BASELINE_TYPE = 'BASELINE_TYPE'
BASELINE_900_TYPE = 'baseline_900'
BASELINE_1800_TYPE = 'baseline_1800'
BASELINE_2100_TYPE = 'baseline_2100'
BASELINE_2600_TYPE = 'baseline_2600'
BASELINE_ANCHOR_TYPE = 'baseline_anchor'

BASELINE_850bma_TYPE = 'baseline_850_bma'
BASELINE_850upa_TYPE = 'baseline_850_upc'

BASELINE_LABEL_TYPE = 'label'

BASELINE = 'baseline'

LV_COLUMN = 'LV'


def read_huawei_mapping(file_mapping_path_name, frequency_type, frequency):
    if frequency_type == "4G":
        return read_4g(file_mapping_path_name)
    elif frequency_type == "3G":
        return read_3g(file_mapping_path_name)
    else:
        return read_2g(file_mapping_path_name)


def read_3g(file_mapping_path_name):
    df = read_excel_mapping(file_mapping_path_name, 0)

    param_dic = {}
    baseline_label_dic = {}
    baseline_850bma_dic = {}
    baseline_850upc_dic = {}
    baseline_2100_dic = {}

    tmp_check_key_dic = {}

    param_cell_level = {}

    for index, row in df.iterrows():
        param_group = str(row[PARAMETER_GROUP_COLUMN_NAME])

        baseline_label_value = str(row[PARAMETER_COLUMN_NAME])

        param_name = naming_helper.rule_column_name(baseline_label_value)

        comment = str(row[COMMENT_COLUMN_NAME])
        baseline_850bma_value = str(row[EXPECTED_850BMA_VALUE_COLUMN_NAME])
        baseline_850upc_value = str(row[EXPECTED_850UPC_VALUE_COLUMN_NAME])
        baseline_2100_value = str(row[EXPECTED_2100_VALUE_COLUMN_NAME])

        if baseline_850bma_value == "nan":
            baseline_850bma_value = ""

        if baseline_850upc_value == "nan":
            baseline_850upc_value = ""

        if baseline_2100_value == "nan":
            baseline_2100_value = ""

        if baseline_label_value == "nan":
            baseline_label_value = ""

        comment_value = str(row[COMMENT_VALUE_COLUMN_NAME])
        if comment_value == "nan":
            comment_value = comment

        comment_tmp = comment
        if comment == comment_value:
            comment_tmp = "Comment"

        cell_level = str(row[LEVEL_COLUMN_NAME])

        param_cell_level[param_group] = cell_level

        if param_group.upper() in tmp_check_key_dic:
            if param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                param_dic[param_group].append(param_name)

                baseline_850bma_dic[param_group][0][param_name] = baseline_850bma_value
                baseline_850bma_dic[param_group][0][BASELINE_TYPE] = BASELINE_850bma_TYPE
                baseline_850bma_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_850upc_dic[param_group][0][param_name] = baseline_850upc_value
                baseline_850upc_dic[param_group][0][BASELINE_TYPE] = BASELINE_850upa_TYPE
                baseline_850upc_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_2100_dic[param_group][0][param_name] = baseline_2100_value
                baseline_2100_dic[param_group][0][BASELINE_TYPE] = BASELINE_2100_TYPE
                baseline_2100_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_label_dic[param_group][0][param_name] = baseline_label_value
                baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE
                baseline_label_dic[param_group][0][LV_COLUMN] = cell_level

                tmp_check_key_dic[param_group.upper()].append(param_name.upper())

                if comment == 'nan':
                    comment = ""
                    continue

                comment_param_name = get_huawei_comment_column_name("C", param_name.upper(), comment_tmp)

                if comment_param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                    param_dic[param_group].append(comment_param_name)
                    baseline_850bma_dic[param_group][0][comment_param_name] = comment_value
                    baseline_850upc_dic[param_group][0][comment_param_name] = comment_value
                    baseline_2100_dic[param_group][0][comment_param_name] = comment_value

                    baseline_label_dic[param_group][0][comment_param_name] = baseline_label_value + " : " + comment_tmp

                    tmp_check_key_dic[param_group.upper()].append(comment_param_name.upper())

            else:

                if comment == 'nan':
                    comment = ""
                    continue

                comment_param_name = get_huawei_comment_column_name("C", param_name.upper(), comment_tmp)

                if comment_param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                    param_dic[param_group].append(comment_param_name)
                    baseline_850bma_dic[param_group][0][comment_param_name] = comment_value
                    baseline_850upc_dic[param_group][0][comment_param_name] = comment_value
                    baseline_2100_dic[param_group][0][comment_param_name] = comment_value

                    baseline_label_dic[param_group][0][comment_param_name] = baseline_label_value + " : " + comment_tmp

                    tmp_check_key_dic[param_group.upper()].append(comment_param_name.upper())
        else:

            param_dic[param_group] = [param_name]

            param_dic[param_group].append("MO")
            param_dic[param_group].append("FILENAME")
            param_dic[param_group].append("REFERENCE_FIELD")
            param_dic[param_group].append(BASELINE_TYPE)

            baseline_850bma_dic[param_group] = [{param_name: baseline_850bma_value}]
            baseline_850bma_dic[param_group][0][BASELINE_TYPE] = BASELINE_850bma_TYPE
            baseline_850bma_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_850upc_dic[param_group] = [{param_name: baseline_850upc_value}]
            baseline_850upc_dic[param_group][0][BASELINE_TYPE] = BASELINE_850upa_TYPE
            baseline_850upc_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_2100_dic[param_group] = [{param_name: baseline_2100_value}]
            baseline_2100_dic[param_group][0][BASELINE_TYPE] = BASELINE_2100_TYPE
            baseline_2100_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_label_dic[param_group] = [{param_name: baseline_label_value}]
            baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE
            baseline_label_dic[param_group][0][LV_COLUMN] = cell_level

            tmp_check_key_dic[param_group.upper()] = [param_name.upper()]

            if comment == 'nan':
                comment = ""
                continue

            comment_param_name = get_huawei_comment_column_name("C", param_name.upper(), comment_tmp)
            param_dic[param_group].append(comment_param_name)
            baseline_850bma_dic[param_group][0][comment_param_name] = comment_value
            baseline_850upc_dic[param_group][0][comment_param_name] = comment_value
            baseline_2100_dic[param_group][0][comment_param_name] = comment_value
            baseline_label_dic[param_group][0][comment_param_name] = baseline_label_value + " : " + comment_tmp

            tmp_check_key_dic[param_group.upper()].append(comment_param_name.upper())

    return param_dic, baseline_850bma_dic, baseline_850upc_dic, baseline_2100_dic, param_cell_level, baseline_label_dic


def read_2g(file_mapping_path_name):
    df = read_excel_mapping(file_mapping_path_name, 0)

    param_dic = {}
    baseline_label_dic = {}
    baseline_dic = {}

    tmp_check_key_dic = {}

    param_cell_level = {}

    for index, row in df.iterrows():
        param_group = str(row[PARAMETER_GROUP_COLUMN_NAME])
        param_name = str(row[PARAMETER_COLUMN_NAME])
        param_name = naming_helper.rule_column_name(param_name)

        comment = str(row[COMMENT_COLUMN_NAME])
        baseline_value = str(row[EXPECTED_VALUE_COLUMN_NAME])
        baseline_label_value = str(row[PARAMETER_COLUMN_NAME])

        if baseline_value == "nan":
            baseline_value = ""

        if baseline_label_value == "nan":
            baseline_label_value = ""

        comment_value = str(row[COMMENT_VALUE_COLUMN_NAME])
        if comment_value == "nan":
            comment_value = comment

        comment_tmp = comment
        if comment == comment_value:
            comment_tmp = "Comment"

        cell_level = str(row[LEVEL_COLUMN_NAME])

        param_cell_level[param_group] = cell_level

        if param_group.upper() in tmp_check_key_dic:
            if param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                param_dic[param_group].append(param_name)

                baseline_dic[param_group][0][param_name] = baseline_value
                baseline_dic[param_group][0][LV_COLUMN] = cell_level
                baseline_dic[param_group][0][BASELINE_TYPE] = BASELINE

                baseline_label_dic[param_group][0][param_name] = baseline_label_value
                baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE
                baseline_label_dic[param_group][0][LV_COLUMN] = cell_level

                tmp_check_key_dic[param_group.upper()].append(param_name.upper())

                if comment == 'nan':
                    comment = ""
                    continue

                comment_param_name = get_huawei_comment_column_name("C", param_name.upper(), comment_tmp)

                if comment_param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                    param_dic[param_group].append(comment_param_name)
                    baseline_dic[param_group][0][comment_param_name] = comment_value
                    baseline_label_dic[param_group][0][comment_param_name] = baseline_label_value + " : " + comment_tmp
                    tmp_check_key_dic[param_group.upper()].append(comment_param_name.upper())

            else:

                if comment == 'nan':
                    comment = ""
                    continue

                comment_param_name = get_huawei_comment_column_name("C", param_name.upper(), comment_tmp)

                if comment_param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                    param_dic[param_group].append(comment_param_name)
                    baseline_dic[param_group][0][comment_param_name] = comment_value
                    baseline_label_dic[param_group][0][comment_param_name] = baseline_label_value + " : " + comment_tmp
                    tmp_check_key_dic[param_group.upper()].append(comment_param_name.upper())
        else:

            param_dic[param_group] = [param_name]

            param_dic[param_group].append("MO")
            param_dic[param_group].append("FILENAME")
            param_dic[param_group].append("REFERENCE_FIELD")
            param_dic[param_group].append(BASELINE_TYPE)

            baseline_dic[param_group] = [{param_name: baseline_value}]
            baseline_dic[param_group][0][BASELINE_TYPE] = BASELINE
            baseline_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_label_dic[param_group] = [{param_name: baseline_label_value}]
            baseline_label_dic[param_group][0][LV_COLUMN] = cell_level
            baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE
            
            tmp_check_key_dic[param_group.upper()] = [param_name.upper()]

            if comment == 'nan':
                comment = ""
                continue

            comment_param_name = get_huawei_comment_column_name("C", param_name.upper(), comment_tmp)
            param_dic[param_group].append(comment_param_name)
            baseline_dic[param_group][0][comment_param_name] = comment_value
            baseline_label_dic[param_group][0][comment_param_name] = baseline_label_value + " : " + comment_tmp
            tmp_check_key_dic[param_group.upper()].append(comment_param_name.upper())

    return param_dic, baseline_dic, param_cell_level, baseline_label_dic




def read_4g(file_mapping_path_name):
    df = read_excel_mapping(file_mapping_path_name, 0)

    param_dic = {}
    baseline_label_dic = {}
    baseline_1800_dic = {}
    baseline_2100_dic = {}
    baseline_2600_dic = {}

    tmp_check_key_dic = {}

    param_cell_level = {}

    for index, row in df.iterrows():
        param_group = str(row[PARAMETER_GROUP_COLUMN_NAME]).upper()

        baseline_label_value = str(row[PARAMETER_COLUMN_NAME])
        param_name = naming_helper.rule_column_name(baseline_label_value)

        comment = str(row[COMMENT_COLUMN_NAME])
        baseline_900_value = str(row[EXPECTED_900_VALUE_COLUMN_NAME])
        baseline_1800_value = str(row[EXPECTED_1800_VALUE_COLUMN_NAME])
        baseline_2100_value = str(row[EXPECTED_2100_VALUE_COLUMN_NAME])
        baseline_2600_value = str(row[EXPECTED_2600_VALUE_COLUMN_NAME])

        if baseline_900_value == "nan":
            baseline_900_value = ""

        if baseline_1800_value == "nan":
            baseline_1800_value = ""

        if baseline_2100_value == "nan":
            baseline_2100_value = ""

        if baseline_2600_value == "nan":
            baseline_2600_value = ""

        if baseline_label_value == "nan":
            baseline_label_value = ""

        comment_value = str(row[COMMENT_VALUE_COLUMN_NAME])
        if comment_value == "nan":
            comment_value = comment

        comment_tmp = comment
        if comment == comment_value:
            comment_tmp = "Comment"

        cell_level = str(row[LEVEL_COLUMN_NAME])

        param_cell_level[param_group] = cell_level

        if param_group.upper() in tmp_check_key_dic:
            if param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                param_dic[param_group].append(param_name)


                baseline_1800_dic[param_group][0][param_name] = baseline_1800_value
                baseline_1800_dic[param_group][0][BASELINE_TYPE] = BASELINE_1800_TYPE
                baseline_1800_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_2100_dic[param_group][0][param_name] = baseline_2100_value
                baseline_2100_dic[param_group][0][BASELINE_TYPE] = BASELINE_2100_TYPE
                baseline_2100_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_2600_dic[param_group][0][param_name] = baseline_2600_value
                baseline_2600_dic[param_group][0][BASELINE_TYPE] = BASELINE_2600_TYPE
                baseline_2600_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_label_dic[param_group][0][param_name] = baseline_label_value
                baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE
                baseline_label_dic[param_group][0][LV_COLUMN] = cell_level

                tmp_check_key_dic[param_group.upper()].append(param_name.upper())

                if comment == 'nan':
                    continue

                comment_param_name = get_huawei_comment_column_name("C", param_name.upper(), comment_tmp)

                if comment_param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                    param_dic[param_group].append(comment_param_name)
                    baseline_1800_dic[param_group][0][comment_param_name] = comment_value
                    baseline_2100_dic[param_group][0][comment_param_name] = comment_value
                    baseline_2600_dic[param_group][0][comment_param_name] = comment_value

                    baseline_label_dic[param_group][0][comment_param_name] = baseline_label_value + " : " + comment_tmp

                    tmp_check_key_dic[param_group.upper()].append(comment_param_name.upper())

            else:

                if comment == 'nan':
                    continue

                comment_param_name = get_huawei_comment_column_name("C", param_name.upper(), comment_tmp)

                if comment_param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                    param_dic[param_group].append(comment_param_name)
                    baseline_1800_dic[param_group][0][comment_param_name] = comment_value
                    baseline_2100_dic[param_group][0][comment_param_name] = comment_value
                    baseline_2600_dic[param_group][0][comment_param_name] = comment_value

                    baseline_label_dic[param_group][0][comment_param_name] = baseline_label_value + " : " + comment_tmp

                    tmp_check_key_dic[param_group.upper()].append(comment_param_name.upper())
        else:

            param_dic[param_group] = [param_name]

            param_dic[param_group].append("MO")
            param_dic[param_group].append("FILENAME")
            param_dic[param_group].append("REFERENCE_FIELD")
            param_dic[param_group].append(BASELINE_TYPE)


            baseline_1800_dic[param_group] = [{param_name: baseline_1800_value}]
            baseline_1800_dic[param_group][0][BASELINE_TYPE] = BASELINE_1800_TYPE
            baseline_1800_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_2100_dic[param_group] = [{param_name: baseline_2100_value}]
            baseline_2100_dic[param_group][0][BASELINE_TYPE] = BASELINE_2100_TYPE
            baseline_2100_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_2600_dic[param_group] = [{param_name: baseline_2600_value}]
            baseline_2600_dic[param_group][0][BASELINE_TYPE] = BASELINE_2600_TYPE
            baseline_2600_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_label_dic[param_group] = [{param_name: baseline_label_value}]
            baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE
            baseline_label_dic[param_group][0][LV_COLUMN] = cell_level

            tmp_check_key_dic[param_group.upper()] = [param_name.upper()]

            if comment == 'nan':
                continue

            comment_param_name = get_huawei_comment_column_name("C", param_name.upper(), comment_tmp)
            param_dic[param_group].append(comment_param_name)
            baseline_1800_dic[param_group][0][comment_param_name] = comment_value
            baseline_2100_dic[param_group][0][comment_param_name] = comment_value
            baseline_2600_dic[param_group][0][comment_param_name] = comment_value

            baseline_label_dic[param_group][0][comment_param_name] = baseline_label_value + " : " + comment_tmp

            tmp_check_key_dic[param_group.upper()].append(comment_param_name.upper())



def read_5g(file_mapping_path_name):
    df = read_excel_mapping(file_mapping_path_name, 0)

    param_dic = {}
    baseline_label_dic = {}
    baseline_2600_dic = {}

    tmp_check_key_dic = {}

    param_cell_level = {}

    for index, row in df.iterrows():
        param_group = str(row[PARAMETER_GROUP_COLUMN_NAME]).upper()

        baseline_label_value = str(row[PARAMETER_COLUMN_NAME])
        param_name = naming_helper.rule_column_name(baseline_label_value)

        comment = str(row[COMMENT_COLUMN_NAME])        
        baseline_2600_value = str(row[EXPECTED_2600_VALUE_COLUMN_NAME])        

        if baseline_2600_value == "nan":
            baseline_2600_value = ""

        if baseline_label_value == "nan":
            baseline_label_value = ""

        comment_value = str(row[COMMENT_VALUE_COLUMN_NAME])
        if comment_value == "nan":
            comment_value = comment

        comment_tmp = comment
        if comment == comment_value:
            comment_tmp = "Comment"

        cell_level = str(row[LEVEL_COLUMN_NAME])

        param_cell_level[param_group] = cell_level

        if param_group.upper() in tmp_check_key_dic:
            if param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                param_dic[param_group].append(param_name)

                baseline_2600_dic[param_group][0][param_name] = baseline_2600_value
                baseline_2600_dic[param_group][0][BASELINE_TYPE] = BASELINE_2600_TYPE
                baseline_2600_dic[param_group][0][LV_COLUMN] = cell_level

                baseline_label_dic[param_group][0][param_name] = baseline_label_value
                baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE
                baseline_label_dic[param_group][0][LV_COLUMN] = cell_level

                tmp_check_key_dic[param_group.upper()].append(param_name.upper())

                if comment == 'nan':
                    continue

                comment_param_name = get_huawei_comment_column_name("C", param_name.upper(), comment_tmp)

                if comment_param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                    param_dic[param_group].append(comment_param_name)
                    baseline_2600_dic[param_group][0][comment_param_name] = comment_value

                    baseline_label_dic[param_group][0][comment_param_name] = baseline_label_value + " : " + comment_tmp

                    tmp_check_key_dic[param_group.upper()].append(comment_param_name.upper())

            else:

                if comment == 'nan':
                    continue

                comment_param_name = get_huawei_comment_column_name("C", param_name.upper(), comment_tmp)

                if comment_param_name.upper() not in tmp_check_key_dic[param_group.upper()]:
                    param_dic[param_group].append(comment_param_name)
                    baseline_2600_dic[param_group][0][comment_param_name] = comment_value

                    baseline_label_dic[param_group][0][comment_param_name] = baseline_label_value + " : " + comment_tmp

                    tmp_check_key_dic[param_group.upper()].append(comment_param_name.upper())
        else:

            param_dic[param_group] = [param_name]

            param_dic[param_group].append("MO")
            param_dic[param_group].append("FILENAME")
            param_dic[param_group].append("REFERENCE_FIELD")
            param_dic[param_group].append(BASELINE_TYPE)            

            baseline_2600_dic[param_group] = [{param_name: baseline_2600_value}]
            baseline_2600_dic[param_group][0][BASELINE_TYPE] = BASELINE_2600_TYPE
            baseline_2600_dic[param_group][0][LV_COLUMN] = cell_level

            baseline_label_dic[param_group] = [{param_name: baseline_label_value}]
            baseline_label_dic[param_group][0][REFERENCE_FIELD_COLUMN_NAME] = BASELINE_LABEL_TYPE
            baseline_label_dic[param_group][0][LV_COLUMN] = cell_level

            tmp_check_key_dic[param_group.upper()] = [param_name.upper()]

            if comment == 'nan':
                continue

            comment_param_name = get_huawei_comment_column_name("C", param_name.upper(), comment_tmp)
            param_dic[param_group].append(comment_param_name)
            baseline_2600_dic[param_group][0][comment_param_name] = comment_value

            baseline_label_dic[param_group][0][comment_param_name] = baseline_label_value + " : " + comment_tmp

            tmp_check_key_dic[param_group.upper()].append(comment_param_name.upper())
