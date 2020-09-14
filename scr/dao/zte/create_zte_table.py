from scr.config.parser import data_source_parser
from scr.helper.mapping_helper import field_mapping_parser

VENDOR = 'ZTE_TABLE_PREFIX'


def create_table():
    data_sources = data_source_parser.read_data_source(VENDOR)
    for zte_data_source in data_sources:
        field_mapping_dic = field_mapping_parser.read(zte_data_source.FileMappingPath)
        for group_param_name in field_mapping_dic.keys():
            field_mapping_dic[group_param_name]