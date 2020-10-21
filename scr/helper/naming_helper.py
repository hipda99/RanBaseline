from environment import BASELINE_TABLE_PREFIX, BASELINE, RED_ZONE

TABLE_NAME_FORMAT = '{0}_{1}_{2}'
SEQUENCE_NAME = '{0}Id'

HUAWEI_COMMENT_COLUMN_NAME = '{0}_{1}_{2}'

preserved_oracle_name = ['TO']
preserved_characater = ['\\', '/', '-']

START_PARSING_STATEMENT = '- Starting {0} at {1}'
PREPARING_TABLE_STATEMENT = '-- preparing table'
PARSING_TABLE_STATEMENT = '-- parsing data'
PARSING_FILE_STATEMENT = '--- file: {0}'
START_PARSER = 'Starting {0} Parser'
FINISH_PARSER = 'Finishing {0} Parser'

removed_params_word = {'eutranmeasparas_', 'utranmeasparas_', 'offfreqprioritypara_', 'eutranrslpara_', 'utranrslpara_', 'geranmeasparas_'}


def get_table_name(vendor, signal, parameter_group):
    table_name = TABLE_NAME_FORMAT.format(vendor, signal, parameter_group.upper())
    return cut_limit_character(table_name)


def clean_value_data(value):
    if len(value) < 5 and value.endswith(';'):
        value = value[:-1]

    return value.strip()


def get_sequence_name(table_name):
    if table_name.__len__() > 28:
        decrease_value = 28 - table_name.__len__()
        table_name = table_name[:decrease_value]
    sequence_name = SEQUENCE_NAME.format(table_name)
    return sequence_name


def rule_column_name(columm_name, frequency_type=None):
    for word in removed_params_word:
        if columm_name.upper().startswith(word.upper()):
            columm_name = columm_name.upper().replace(word.upper(), '')
            break

    columm_name = columm_name.upper()
    columm_name = columm_name.replace(" ", "")
    columm_name = columm_name.replace("ReportConfig".upper(), "RepCon")
    columm_name = columm_name.replace("dlResourceAllocationStrategy".upper(), "dlResAllocationStr")
    columm_name = columm_name.replace("resourceAllocationStrategy".upper(), "resAllocationStr")
    columm_name = columm_name.replace("sPrioritySearch".upper(), "sPrioSearch".upper())

    columm_name = columm_name.replace("FreqLayerSwitch".upper(), "FreqLayerSw".upper())
    columm_name = columm_name.replace("UtranFreqLayerMeasSwitch".upper(), "UtranMeasSw".upper())
    columm_name = columm_name.replace("UtranFreqLayerBlindSwitch".upper(), "UtranBlindSw".upper())
    columm_name = columm_name.replace("UtranSrvccSteeringSwitch".upper(), "UtranSrvccSteeringSw".upper())
    columm_name = columm_name.replace("RsvdSwPara1_bit".upper(), "bit".upper())
    columm_name = columm_name.replace("X2SonDeleteSwitch".upper(), "X2SonDelSw".upper())
    columm_name = columm_name.replace("BASED_ON_X2".upper(), "X2".upper())
    columm_name = columm_name.replace("WITH_NEGO".upper(), "WT_NEGO".upper())
    columm_name = columm_name.replace("WITHOUT_NEGO".upper(), "WO_NEGO".upper())
    columm_name = columm_name.replace("systemInformationBlock3_", "sysInfoBk3_")
    columm_name = columm_name.replace("CFGSWITCH", "CFGSW")
    columm_name = columm_name.replace("COMMENT", "")

    if columm_name[0].isdigit():
        columm_name = "X" + columm_name

    columm_name = columm_name.translate({ord(c): "" for c in "!@#$%^&*()[]{};:,./<>?\|`~-=+"})[0:30]

    for preserve in preserved_characater:
        if preserve in columm_name:
            columm_name = columm_name.replace(preserve, '_')

    if columm_name.upper() in preserved_oracle_name:
        columm_name = columm_name + '_'

    if columm_name.__len__() > 30:
        decrease_value = columm_name.__len__() - 30
        columm_name = columm_name[decrease_value:]

    if columm_name.startswith('_'):
        columm_name = columm_name[1:]

    return columm_name.upper()


def cut_limit_character(text):
    # Add exceptional Case help
    if "BL_ERC_5G_QCIPROFILEENDCCONFIGEXT" == text:
        text = "BL_ERC_5G_QCIPRFENDCCNFGEXT"
    elif "ZTE_5G_SCSSPECIFICCARRIERLISTDL" == text:
        text = "ZTE_5G_SCSSPCFCCRRRLSTDL"
    elif "BL_ZTE_5G_SCSSPECIFICCARRIERLISTDL" == text:
        text = "BL_ZTE_5G_SCSSPCFCCRRRLSTDL"
    elif "ZTE_5G_SCSSPECIFICCARRIERLISTUL" == text:
        text = "ZTE_5G_SCSSPCFCCRRRLSTUL"
    elif "BL_ZTE_5G_SCSSPECIFICCARRIERLISTUL" == text:
        text = "BL_ZTE_5G_SCSSPCFCCRRRLSTUL"
    if text.__len__() > 30:
        decrease_value = 30 - text.__len__()
        text = text[:decrease_value]
    return text


def get_zte_mo_name(parameter_group):
    return 'reservedBy' + parameter_group


def get_table_name_collection(table_prefix):
    table_name_collection = [table_prefix,
                             BASELINE_TABLE_PREFIX.format(table_prefix),
                             BASELINE_TABLE_PREFIX.format(table_prefix)]
    return table_name_collection


def is_baseline_table(table_name):
    if table_name.startswith(BASELINE):
        return True
    return False


def is_red_zone_table(table_name):
    if table_name.startswith(RED_ZONE):
        return True
    return False


def get_huawei_comment_column_name(prefix, param_group, comment):
    if comment == "" or comment == 'Comment':
        column_name = '{0}_{1}'.format(prefix, param_group)
    else:
        column_name = '{0}_{1}_{2}'.format(prefix, param_group, comment)

    return rule_column_name(column_name)


def get_ericsson_struct_column_name(prefix, param_group, struct):
    if struct == "":
        column_name = '{0}_{1}'.format(prefix, param_group)
    else:
        column_name = '{0}_{1}_{2}'.format(prefix, param_group, struct)

    return rule_column_name(column_name)
