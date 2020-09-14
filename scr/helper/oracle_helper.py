from environment import PATH_SEPARATOR


def add_additional_result(oracle_value_pair_dic, file_name):
    if 'FileName' in oracle_value_pair_dic:
        oracle_value_pair_dic['FileName'] = file_name.split(PATH_SEPARATOR)[-1]
