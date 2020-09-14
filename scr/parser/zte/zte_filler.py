import cx_Oracle

import datetime
from environment import *
from scr.config.parser.data_source_parser import read_mapping_yaml
from scr.dao import granite_mongo, ran_baseline_oracle
from scr.helper import naming_helper
from scr.helper.mapping_helper import field_mapping_parser

table_name = 'ZTE_TABLE_PREFIX' + '{0}{1}'




def open():
    dsn_tns = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, ORACLE_SID)
    connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, dsn_tns)
    cur = connection.cursor()
    return connection, cur


def close(connection, cur):
    connection.close()

def run(frequency, type):
    print('Starting ZTE_TABLE_PREFIX Fulfiller')

    # print('- Begin Filling: ' + str(frequency) + ' [' + type + '] at ' + str(datetime.datetime.now()))
    #
    # yaml_value = read_mapping_yaml()
    # fileMappingPath = CONFIGURATION_PATH + yaml_value['zte']['network'][frequency][type]['mapping']
    # frequencyType = yaml_value['zte']['network'][frequency]['type']
    #
    # field_mapping_dic = field_mapping_parser.read_2g(fileMappingPath)
    #
    # sum_result = {}
    # try:
    #     con, cur = open()
    #
    #     ran_baseline_oracle.connection = con
    #     ran_baseline_oracle.cur = cur
    #     ran_baseline_oracle.drop(cur, collection_name)
    #     ran_baseline_oracle.push(collection_name, result_list)
    #
    #     for group_param_name in field_mapping_dic:
    #         sum_result[group_param_name] = []
    #         collection_name = naming_helper.get_table_name('ZTE_TABLE_PREFIX', frequencyType, group_param_name)
    #
    #         oracle_parameter_list = field_mapping_dic[group_param_name]
    #         dict.fromkeys(oracle_parameter_list, 1)
    #         result_list = granite_mongo.get(collection_name, oracle_parameter_list)
    #
    #         if result_list.__len__() == 0:
    #             print('-- This Collection ['+collection_name+'] is not found in MongoDB.')
    #             print('Finishing ZTE_TABLE_PREFIX Fullfiller')
    #             return
    #
    #     sum_result.clear()
    #
    #     close(con, cur)
    # except Exception as e:
    #     print(e)
    #     close(con, cur)
    #
    # print('Finishing ZTE_TABLE_PREFIX Fulfiller')



if __name__ == '__main__':
    run(900, 'r3')


