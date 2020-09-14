import os
import json

from environment import *


def print(raw_file, group_param_name, parsed_raw_dic):
    target_save_file_path = JSON_RESULT_PATH.format((str(raw_file.Frequency)), raw_file.Type, group_param_name)

    directory_name = os.path.dirname(target_save_file_path)
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)

    saved_file_name = target_save_file_path + '\\' + group_param_name + '.json'
    with open(saved_file_name, 'a') as outfile:
        json.dump(parsed_raw_dic[group_param_name], outfile)