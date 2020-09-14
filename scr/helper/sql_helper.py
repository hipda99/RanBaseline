from scr.helper import naming_helper
from scr.helper.mapping_helper.zte_mapping_helper import BASELINE, RED_ZONE, BASELINE_TYPE

SQL_CURRENT_SEQUENCE_ID = 'select {0}.currval from dual'
SQL_CHECK_TABLE_EXIST = "select table_name from user_tables where table_name='{0}'"
SQL_CREATE_TABLE = 'create table {0} ({1} number primary key , {2})'
SQL_CREATE_SEQUENCE = 'create sequence {0}'
SQL_INSERT_INTO_TABLE = 'insert into {0} ({1}) values ({2}.nextval,{3})'
SQL_DROP_TABLE = 'DROP TABLE {0}'
SQL_DROP_SEQUENCE = 'DROP SEQUENCE {0}'
SQL_CHECK_EMPTY_TABLE = 'SELECT 1 FROM {0} WHERE ROWNUM=1'
SQL_CHECK_EMPTY_TABLE_BASELINE = 'SELECT 1 FROM {0} WHERE ROWNUM=1 AND {1} = \'{2}\''


def get_sql_current_sequence_id(table_name):
    return SQL_CURRENT_SEQUENCE_ID.format(naming_helper.get_sequence_name(table_name))


def get_sql_check_exist_table(table_name):
    return SQL_CHECK_TABLE_EXIST.format(table_name)


def get_sql_create_table(table_name, column_list):
    value_pair = '{0} varchar(4000)'
    new_column_list = []
    for column in column_list:

        new_column_name = naming_helper.rule_column_name(column)
        value = value_pair.format(new_column_name)
        new_column_list.append(value)

    value = value_pair.format("LV")
    new_column_list.append(value)

    column_str = ','.join(new_column_list)
    column_str = column_str + ',' + " CREATED_AT DATE DEFAULT CURRENT_TIMESTAMP "

    sequence_name = naming_helper.get_sequence_name(table_name)

    return SQL_CREATE_TABLE.format(table_name, sequence_name, column_str)


def get_sql_create_sequence(table_name):
    sequence_name = naming_helper.get_sequence_name(table_name)
    return SQL_CREATE_SEQUENCE.format(sequence_name)


def get_sql_insert_into_table(table_name, column_to_insert, value_to_insert):
    param_args = []
    for value in value_to_insert:
        for idx, val in enumerate(value):
            param_args.append(':' + str(idx + 1))
        break;

    sql_value_arguments_str = ','.join(param_args)
    column_str = ','.join(column_to_insert)
    sequence_name = naming_helper.get_sequence_name(table_name)
    prepared_value = [tuple(l) for l in value_to_insert]

    return SQL_INSERT_INTO_TABLE.format(table_name, column_str, sequence_name, sql_value_arguments_str), prepared_value


def get_sql_drop_table(table_name):
    return SQL_DROP_TABLE.format(table_name)


def get_sql_drop_sequence(table_name):
    sequence_name = naming_helper.get_sequence_name(table_name)
    return SQL_DROP_SEQUENCE.format(sequence_name)


def get_sql_check_empty_table(table_name):
    return SQL_CHECK_EMPTY_TABLE.format(table_name)


def get_sql_check_empty_table_of_baseline(table_name):
    """

    :rtype:
    """
    return SQL_CHECK_EMPTY_TABLE_BASELINE.format(table_name, BASELINE_TYPE, BASELINE)


def get_sql_check_empty_table_of_redzone(table_name):
    return SQL_CHECK_EMPTY_TABLE_BASELINE.format(table_name, BASELINE_TYPE, RED_ZONE)


def get_sql_sync_insert_table(table_name, column_to_insert, value_to_insert):
    sql_insert_stmt_collection = []
    for value in value_to_insert:
        param_args = []
        for idx, val in enumerate(value):
            param_args.append("'" + val + "'")

        sql_value_arguments_str = ','.join(param_args)
        column_str = ','.join(column_to_insert)
        sequence_name = naming_helper.get_sequence_name(table_name)
        sql = SQL_INSERT_INTO_TABLE.format(table_name, column_str, sequence_name, sql_value_arguments_str)
        sql_insert_stmt_collection.append(sql)

    return sql_insert_stmt_collection
