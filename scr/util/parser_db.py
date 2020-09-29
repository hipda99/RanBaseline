import os
import json
import cx_Oracle
import datetime
import time
import log

from environment import *

STATUS_OPEN = "processing"
STATUS_CLOSE = "done"

def update_status(vendor, parser, state):
	dsn_tns = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, ORACLE_SID)
	connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, dsn_tns, encoding="UTF-8",nencoding="UTF-8")
	cursor = None
	try:	
		# ts = datetime.datetime.now()	
		cursor = connection.cursor()
		cursor = f"SELECT COUNT(1) FROM PARSER_STATUS WHERE VENDOR = '{vendor}' AND PARSER = '{parser}' AND trunc(DT,'DD') = trunc(sysdate, 'DD')"
		cursor.execute(cursor)
		if cursor.fetchone()[0]:
			# Existing
			query = f"""UPDATE PARSER_STATUS SET status = '{state}'
			WHERE vendor = '{vendor}' and parser = '{parser}' AND TRUNC(dt, 'DD') = TRUNC(sysdate, 'DD')
			"""						
			# execute_time = time.time()	
			cursor.execute(query)
			connection.commit()
			# elapsed_time = time.time() - execute_time		
			# log.i(f'parser.update_status.done: elapsed time = {elapsed_time:.2f} sec')
		else:
			query = f"""INSERT INTO PARSER_STATUS (vendor, parser, status, dt) VALUES ('{vendor}', '{parser}', '{state}', sysdate)"""			
			cursor = connection.cursor()
			# execute_time = time.time()	
			cursor.execute(query)
			connection.commit()
			# elapsed_time = time.time() - execute_time		
			# log.i(f'parser.update_status.done: elapsed time = {elapsed_time:.2f} sec')


	except Exception as e:		
		log.e(f'parser.update_status.err: {str(e)}')
	finally:
		if cursor is not None:
			cursor.close()
		if connection is not None:
			connection.close()
	return		

