import os
import json
import cx_Oracle
import datetime
import time
import logging

from environment import *

logger = logging.getLogger(__name__)
STATUS_OPEN = "processing"
STATUS_CLOSE = "done"

def update_status(vendor, parser, state):
	dsn_tns = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, ORACLE_SID)
	connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, dsn_tns, encoding="UTF-8",nencoding="UTF-8")
	cursor = None
	try:	
		ts = datetime.datetime.now()	
		query = f"""MERGE INTO PARSER_STATUS d
		USING (SELECT * from PARSER_STATUS WHERE VENDOR = '{vendor}' AND PARSER = '{parser}' AND trunc(DT,'DD') = trunc(sysdate, 'DD')) s
		ON (d.vendor = s.vendor AND d.parser = s.parser AND d.dt = s.dt)
		WHEN MATCHED THEN UPDATE SET d.status = '{state}'
		WHEN NOT MATCHED THEN INSERT (vendor, parser, status, dt) VALUES ('{vendor}', '{parser}', '{state}', :t_val);
		"""
		# query = f"INSERT INTO PARSER_STATUS VALUES ('{vendor}','{parser}','{state}', {today}"
		cursor = connection.cursor()
		execute_time = time.time()	
		cursor.prepare(query)	
		cursor.setinputsizes(t_val=cx_Oracle.TIMESTAMP)
		cursor.execute(None, {'t_val': ts})
		connection.commit()
		elapsed_time = time.time() - execute_time		
		logger.info(f'parser.update_status.done: elapsed time = {elapsed_time:.2f} sec')

	except Exception as e:		
		logger.exception(f'parser.update_status.err: {str(e)}')
	finally:
		if cursor is not None:
			cursor.close()
		if connection is not None:
			connection.close()
	return		

