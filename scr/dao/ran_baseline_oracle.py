import log
from scr.helper import naming_helper, sql_helper


def extract_column(table_name, value_to_insert):
    column_insert_result = []
    row_insert_result = []
    sequence_name = naming_helper.get_sequence_name(table_name)
    for row_result in value_to_insert:
        if len(row_result) == 0:
            continue

        row_value = []
        column_value = []
        for parameter in row_result.keys():
            column_value.append(naming_helper.rule_column_name(parameter))
            if row_result[parameter] is None:
                row_value.append('')
            else:
                row_value.append(row_result[parameter])
        row_insert_result.append(row_value)
        column_value.insert(0, sequence_name)
        column_insert_result.append(column_value)
    return column_insert_result[0], row_insert_result


def get_next_val(cur, table_name):
    sql = sql_helper.get_sql_current_sequence_id(table_name)
    cur.execute(sql)
    result = int(cur.fetchone()[0])
    return result


def check_exist_table(cur, table_name):
    sql = sql_helper.get_sql_check_exist_table(table_name)
    result = cur.execute(sql)
    for table, in result:
        if '' in table:
            return True
        else:
            return False
    return False


def check_table_empty(cur, table_name):
    sql = sql_helper.get_sql_check_empty_table(table_name)
    result = cur.execute(sql)
    value = result.fetchone()
    if value is None or len(value) == 0:
        return True
    else:
        return False


def check_table_has_one_row(cur, table_name):
    sql = sql_helper.get_sql_check_empty_table(table_name)
    result = cur.execute(sql)
    value = result.fetchone()
    if value is None or len(value) == 0 or len(value) > 1:
        return False
    else:
        return True


def gen_txrx_cell(cur):
    try:
        sb = ['INSERT INTO HW_4G_TXRX_CELL (HW_4G_TXRX_CELLID, CRSPORTNUM, C_CRSPORTNUM, TXRXMODE, C_TXRXMODE, MO, FILENAME, REFERENCE_FIELD, MAXMIMORANKPARA, C_MAXMIMORANKPARA, SECTORID, SECTOREQMID ) ',
            'SELECT HW_4G_TXRX_CELLid.nextval, CRSPORTNUM, C_CRSPORTNUM, TXRXMODE, C_TXRXMODE, HW_4G_CELL.MO, HW_4G_CELL.FILENAME, HW_4G_CELL.REFERENCE_FIELD, MAXMIMORANKPARA, C_MAXMIMORANKPARA,  HW_4G_EUSECTOREQMGROUP.SECTORID,  HW_4G_EUSECTOREQMGROUP.SECTOREQMID ',
            'from HW_4G_CELL left outer ',
            'join HW_4G_CellDlschAlgo  on HW_4G_CellDlschAlgo.REFERENCE_FIELD = HW_4G_CELL.REFERENCE_FIELD ',
            'left outer join HW_4G_EUSECTOREQMGROUP on HW_4G_EUSECTOREQMGROUP.REFERENCE_FIELD = HW_4G_CELL.REFERENCE_FIELD ']

        sql = "".join(sb)

        cur.execute('TRUNCATE TABLE HW_4G_TXRX_CELL')
        result = cur.execute(sql)

        print("gen TXRX Cell --- DONE")
    except Exception as e:
        print(f"WARNING truncate table HW_4G_TXRX_CELL: {str(e)}")


def gen_txrx_node(cur):
    try:
        sb = [
            'INSERT INTO HW_4G_TXRX (HW_4G_TXRXID , COMPATIBILITYCTRLSWITCH ,  C_COMPATIBILITYCTRLSWITCH_TM3T , MO ,  FILENAME , REFERENCE_FIELD  ) ',
            'SELECT  HW_4G_TXRXid.nextval, HW_4G_ENODEBALGOSWITCH.COMPATIBILITYCTRLSWITCH, HW_4G_ENODEBALGOSWITCH.C_COMPATIBILITYCTRLSWITCH_TM3T, HW_4G_ENODEBALGOSWITCH.MO, HW_4G_ENODEBALGOSWITCH.FILENAME, HW_4G_ENODEBALGOSWITCH.REFERENCE_FIELD  ',
            'from HW_4G_ENodeBAlgoSwitch'
        ]

        sql = "".join(sb)

        cur.execute('TRUNCATE TABLE HW_4G_TXRX')
        result = cur.execute(sql)

        print("gen TXRX eNodeB --- DONE")
    except Exception as e:
        print(f"WARNING truncate table v: {str(e)}")


def create_trigger(cur):
    sb = [
        ' CREATE OR REPLACE TRIGGER ERC_4G_QCI_AFTER_INSERT \n ', \
        ' BEFORE INSERT \n ', \
        ' ON ERC_4G_QCIPROFILEPREDEFINED \n ', \
        ' FOR EACH ROW \n ', \
        ' BEGIN \n ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set AQMMODE_QCI1   = :NEW.AQMMODE_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.AQMMODE_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set AQMMODE_QCI5   = :NEW.AQMMODE_QCI5   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.AQMMODE_QCI5   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set AQMMODE_QCI6   = :NEW.AQMMODE_QCI6   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.AQMMODE_QCI6   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set AQMMODE_QCI7   = :NEW.AQMMODE_QCI7   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.AQMMODE_QCI7   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set AQMMODE_QCI8   = :NEW.AQMMODE_QCI8   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.AQMMODE_QCI8   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set AQMMODE_QCI9   = :NEW.AQMMODE_QCI9   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.AQMMODE_QCI9   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DATAFWDPERQCIENABLED_QCI1   = :NEW.DATAFWDPERQCIENABLED_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DATAFWDPERQCIENABLED_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DATAFWDPERQCIENABLED_QCI2   = :NEW.DATAFWDPERQCIENABLED_QCI2   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DATAFWDPERQCIENABLED_QCI2   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DATAFWDPERQCIENABLED_QCI5   = :NEW.DATAFWDPERQCIENABLED_QCI5   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DATAFWDPERQCIENABLED_QCI5   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DLMINBITRATE_QCI6   = :NEW.DLMINBITRATE_QCI6   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DLMINBITRATE_QCI6   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DLMINBITRATE_QCI7   = :NEW.DLMINBITRATE_QCI7   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DLMINBITRATE_QCI7   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DLMINBITRATE_QCI8   = :NEW.DLMINBITRATE_QCI8   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DLMINBITRATE_QCI8   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DLMINBITRATE_QCI9   = :NEW.DLMINBITRATE_QCI9   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DLMINBITRATE_QCI9   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DLRESALLOCATIONSTR_QCI1   = :NEW.DLRESALLOCATIONSTR_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DLRESALLOCATIONSTR_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DLRESALLOCATIONSTR_QCI2   = :NEW.DLRESALLOCATIONSTR_QCI2   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DLRESALLOCATIONSTR_QCI2   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DLRESALLOCATIONSTR_QCI3   = :NEW.DLRESALLOCATIONSTR_QCI3   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DLRESALLOCATIONSTR_QCI3   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DLRESALLOCATIONSTR_QCI4   = :NEW.DLRESALLOCATIONSTR_QCI4   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DLRESALLOCATIONSTR_QCI4   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DLRESALLOCATIONSTR_QCI5   = :NEW.DLRESALLOCATIONSTR_QCI5   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DLRESALLOCATIONSTR_QCI5   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DLRESALLOCATIONSTR_QCI6   = :NEW.DLRESALLOCATIONSTR_QCI6   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DLRESALLOCATIONSTR_QCI6   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DLRESALLOCATIONSTR_QCI7   = :NEW.DLRESALLOCATIONSTR_QCI7   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DLRESALLOCATIONSTR_QCI7   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DLRESALLOCATIONSTR_QCI8   = :NEW.DLRESALLOCATIONSTR_QCI8   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DLRESALLOCATIONSTR_QCI8   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DLRESALLOCATIONSTR_QCI9   = :NEW.DLRESALLOCATIONSTR_QCI9   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DLRESALLOCATIONSTR_QCI9   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DRXPRIORITY_QCI1   = :NEW.DRXPRIORITY_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DRXPRIORITY_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set DRXPRIORITY_QCI2   = :NEW.DRXPRIORITY_QCI2   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.DRXPRIORITY_QCI2   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set INACTIVITYTIMEROFFSET   = :NEW.INACTIVITYTIMEROFFSET   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.INACTIVITYTIMEROFFSET   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set LOGICALCHANNELGROUP_QCI6   = :NEW.LOGICALCHANNELGROUP_QCI6   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.LOGICALCHANNELGROUP_QCI6   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set LOGICALCHANNELGROUP_QCI7   = :NEW.LOGICALCHANNELGROUP_QCI7   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.LOGICALCHANNELGROUP_QCI7   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set LOGICALCHANNELGROUP_QCI8   = :NEW.LOGICALCHANNELGROUP_QCI8   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.LOGICALCHANNELGROUP_QCI8   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set LOGICALCHANNELGROUP_QCI9   = :NEW.LOGICALCHANNELGROUP_QCI9   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.LOGICALCHANNELGROUP_QCI9   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set PDB_QCI1   = :NEW.PDB_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.PDB_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set PDB_QCI6   = :NEW.PDB_QCI6   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.PDB_QCI6   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set PDB_QCI7   = :NEW.PDB_QCI7   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.PDB_QCI7   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set PDB_QCI8   = :NEW.PDB_QCI8   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.PDB_QCI8   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set PDB_QCI9   = :NEW.PDB_QCI9   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.PDB_QCI9   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set PDBOFFSET_QCI6   = :NEW.PDBOFFSET_QCI6   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.PDBOFFSET_QCI6   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set PDBOFFSET_QCI7   = :NEW.PDBOFFSET_QCI7   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.PDBOFFSET_QCI7   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set PDBOFFSET_QCI8   = :NEW.PDBOFFSET_QCI8   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.PDBOFFSET_QCI8   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set PDBOFFSET_QCI9   = :NEW.PDBOFFSET_QCI9   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.PDBOFFSET_QCI9   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set PRIORITY_QCI1   = :NEW.PRIORITY_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.PRIORITY_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set PRIORITY_QCI5   = :NEW.PRIORITY_QCI5   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.PRIORITY_QCI5   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set QCISUBSCRIPTIONQUANTA_QCI1   = :NEW.QCISUBSCRIPTIONQUANTA_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.QCISUBSCRIPTIONQUANTA_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set QCISUBSCRIPTIONQUANTA_QCI5   = :NEW.QCISUBSCRIPTIONQUANTA_QCI5   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.QCISUBSCRIPTIONQUANTA_QCI5   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set QCISUBSCRIPTIONQUANTA_QCI8   = :NEW.QCISUBSCRIPTIONQUANTA_QCI8   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.QCISUBSCRIPTIONQUANTA_QCI8   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set RESALLOCATIONSTR_QCI1   = :NEW.RESALLOCATIONSTR_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.RESALLOCATIONSTR_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set RESALLOCATIONSTR_QCI2   = :NEW.RESALLOCATIONSTR_QCI2   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.RESALLOCATIONSTR_QCI2   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set RESALLOCATIONSTR_QCI3   = :NEW.RESALLOCATIONSTR_QCI3   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.RESALLOCATIONSTR_QCI3   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set RESALLOCATIONSTR_QCI4   = :NEW.RESALLOCATIONSTR_QCI4   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.RESALLOCATIONSTR_QCI4   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set RESALLOCATIONSTR_QCI5   = :NEW.RESALLOCATIONSTR_QCI5   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.RESALLOCATIONSTR_QCI5   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set RESALLOCATIONSTR_QCI6   = :NEW.RESALLOCATIONSTR_QCI6   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.RESALLOCATIONSTR_QCI6   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set RESALLOCATIONSTR_QCI7   = :NEW.RESALLOCATIONSTR_QCI7   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.RESALLOCATIONSTR_QCI7   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set RESALLOCATIONSTR_QCI8   = :NEW.RESALLOCATIONSTR_QCI8   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.RESALLOCATIONSTR_QCI8   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set RESALLOCATIONSTR_QCI9   = :NEW.RESALLOCATIONSTR_QCI9   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.RESALLOCATIONSTR_QCI9   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set RLCMODE_QCI1   = :NEW.RLCMODE_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.RLCMODE_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set RLCMODE_QCI2   = :NEW.RLCMODE_QCI2   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.RLCMODE_QCI2   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set RLCSNLENGTH_QCI1   = :NEW.RLCSNLENGTH_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.RLCSNLENGTH_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set ROHCENABLED_QCI1   = :NEW.ROHCENABLED_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.ROHCENABLED_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SCHEDULINGALGORITHM_QCI1   = :NEW.SCHEDULINGALGORITHM_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SCHEDULINGALGORITHM_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SCHEDULINGALGORITHM_QCI2   = :NEW.SCHEDULINGALGORITHM_QCI2   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SCHEDULINGALGORITHM_QCI2   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SCHEDULINGALGORITHM_QCI6   = :NEW.SCHEDULINGALGORITHM_QCI6   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SCHEDULINGALGORITHM_QCI6   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SCHEDULINGALGORITHM_QCI7   = :NEW.SCHEDULINGALGORITHM_QCI7   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SCHEDULINGALGORITHM_QCI7   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SCHEDULINGALGORITHM_QCI8   = :NEW.SCHEDULINGALGORITHM_QCI8   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SCHEDULINGALGORITHM_QCI8   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SCHEDULINGALGORITHM_QCI9   = :NEW.SCHEDULINGALGORITHM_QCI9   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SCHEDULINGALGORITHM_QCI9   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SERVICETYPE_QCI1   = :NEW.SERVICETYPE_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SERVICETYPE_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SRSALLOCATIONSTRATEGY_QCI1   = :NEW.SRSALLOCATIONSTRATEGY_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SRSALLOCATIONSTRATEGY_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SRSALLOCATIONSTRATEGY_QCI2   = :NEW.SRSALLOCATIONSTRATEGY_QCI2   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SRSALLOCATIONSTRATEGY_QCI2   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SRSALLOCATIONSTRATEGY_QCI3   = :NEW.SRSALLOCATIONSTRATEGY_QCI3   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SRSALLOCATIONSTRATEGY_QCI3   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SRSALLOCATIONSTRATEGY_QCI4   = :NEW.SRSALLOCATIONSTRATEGY_QCI4   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SRSALLOCATIONSTRATEGY_QCI4   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SRSALLOCATIONSTRATEGY_QCI5   = :NEW.SRSALLOCATIONSTRATEGY_QCI5   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SRSALLOCATIONSTRATEGY_QCI5   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SRSALLOCATIONSTRATEGY_QCI6   = :NEW.SRSALLOCATIONSTRATEGY_QCI6   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SRSALLOCATIONSTRATEGY_QCI6   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SRSALLOCATIONSTRATEGY_QCI7   = :NEW.SRSALLOCATIONSTRATEGY_QCI7   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SRSALLOCATIONSTRATEGY_QCI7   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SRSALLOCATIONSTRATEGY_QCI8   = :NEW.SRSALLOCATIONSTRATEGY_QCI8   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SRSALLOCATIONSTRATEGY_QCI8   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set SRSALLOCATIONSTRATEGY_QCI9   = :NEW.SRSALLOCATIONSTRATEGY_QCI9   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.SRSALLOCATIONSTRATEGY_QCI9   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set QCIPROFILEPREDEFINED_QCI1   = :NEW.QCIPROFILEPREDEFINED_QCI1   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.QCIPROFILEPREDEFINED_QCI1   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set QCIPROFILEPREDEFINED_QCI5   = :NEW.QCIPROFILEPREDEFINED_QCI5   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.QCIPROFILEPREDEFINED_QCI5   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set QCIPROFILEPREDEFINED_QCI6   = :NEW.QCIPROFILEPREDEFINED_QCI6   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.QCIPROFILEPREDEFINED_QCI6   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set QCIPROFILEPREDEFINED_QCI7   = :NEW.QCIPROFILEPREDEFINED_QCI7   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.QCIPROFILEPREDEFINED_QCI7   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set QCIPROFILEPREDEFINED_QCI8   = :NEW.QCIPROFILEPREDEFINED_QCI8   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.QCIPROFILEPREDEFINED_QCI8   is not null; "   ', \
        ' "update ERC_4G_QCIPROFILEPREDEFINED set QCIPROFILEPREDEFINED_QCI9   = :NEW.QCIPROFILEPREDEFINED_QCI9   where REFERENCE_FIELD = :NEW.REFERENCE_FIELD and :NEW.QCIPROFILEPREDEFINED_QCI9   is not null; "   ', \
 \
        ' END; '

    ]

    sql = "".join(sb)

    result = cur.execute(sql)

    print("creat trigger --- DONE")


def check_table_has_one_row_for_baseline(cur, table_name):
    try:

        sql = sql_helper.get_sql_check_empty_table_of_baseline(table_name)
        result = cur.execute(sql)
        value = result.fetchone()
        if value is None or len(value) == 0 or len(value) > 1:
            return False
        else:
            return True
    except Exception as e:
        log.e(e)
        log.e(table_name)
        return False


def check_table_has_one_row_for_redzone(cur, table_name):
    sql = sql_helper.get_sql_check_empty_table_of_redzone(table_name)
    result = cur.execute(sql)
    value = result.fetchone()
    if value is None or len(value) == 0 or len(value) > 1:
        return False
    else:
        return True


def create_table(cur, table_name, column_list):
    try:

        sql_create_table = sql_helper.get_sql_create_table(table_name, column_list)
        sql_create_sequence = sql_helper.get_sql_create_sequence(table_name)
        cur.execute(sql_create_table)
        cur.execute(sql_create_sequence)

    except:
        return


def insert_into_table(cur, table_name, column_to_insert, value_to_insert):
    sql, prepared_value = sql_helper.get_sql_insert_into_table(table_name, column_to_insert, value_to_insert)
    cur.executemany(sql, prepared_value)


def drop(cur, table_name):
    try:

        sql_drop_table = sql_helper.get_sql_drop_table(table_name)
        sql_drop_seq = sql_helper.get_sql_drop_sequence(table_name)
        cur.execute(sql_drop_table)
        cur.execute(sql_drop_seq)

    except:
        return


def push(cur, table_name, value_to_push):
    if len(value_to_push) == 0:
        return

    try:
        column_to_insert, value_to_insert = extract_column(table_name, value_to_push)
        insert_into_table(cur, table_name, column_to_insert, value_to_insert)

    except Exception as e:
        log.e(e)
        log.e("table_name : " + table_name)
        log.e("column_to_insert : " + column_to_insert)
        log.e("column : " + column_to_insert)
        log.e("value : " + value_to_insert[0])
        raise Exception(e)


def create_sql_file(cur, table_name, value_to_push):
    if len(value_to_push) == 0:
        return

    try:
        column_to_insert, value_to_insert = extract_column(table_name, value_to_push)

        insert_into_table(cur, table_name, column_to_insert, value_to_insert)

    except Exception as e:
        log.e("table_name : " + table_name)
        log.e("column_to_insert : " + column_to_insert)
        log.e("column : " + column_to_insert)
        log.e("value : " + value_to_insert[0])
        raise Exception(e)


def get_sql_insert_statment(table_name, value_to_insert):
    column_to_insert, value_to_insert = extract_column(table_name, value_to_insert)
    sql = sql_helper.get_sql_sync_insert_table(table_name, column_to_insert, value_to_insert)
    return sql
