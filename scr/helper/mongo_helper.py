from environment import PATH_SEPARATOR
from scr.helper import naming_helper


def add_additional_result(mongo_value_pair_dic, frequency_type, user_label, file_name, parsed_time_duration):
    mongo_value_pair_dic['parsedTimeDuration'] = str(parsed_time_duration)
    mongo_value_pair_dic['frequency'] = frequency_type
    mongo_value_pair_dic['ManagedElement_userLabel'] = user_label
    mongo_value_pair_dic['FileName'] = file_name.split(PATH_SEPARATOR)[-1]



def find_zte_mo(mongo_value_pair_dic, parameter_group):
    zte_mo_represent_name = naming_helper.get_zte_mo_name(parameter_group)

    for key in mongo_value_pair_dic:
        mo_ref_param = ''
        if 'reservedBy' in key:
            new_tag_name = key.replace('reservedBy', '')

            if new_tag_name.startswith('U'):
                mo_ref_param = 'reservedBy' + new_tag_name
            else:
                mo_ref_param = 'reservedByU' + new_tag_name

            if zte_mo_represent_name == mo_ref_param:
                mongo_value_pair_dic['MO'] = mongo_value_pair_dic[key]
