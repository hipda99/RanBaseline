import datetime
import multiprocessing as mp
import traceback
import re

import cx_Oracle
from lxml import ElementInclude
from lxml import etree
from lxml.etree import XPathEvalError

import log
from environment import *
from scr.dao import ran_baseline_oracle
from scr.helper import naming_helper
from scr.helper.mapping_helper.zte_mapping_helper import REFERENCE_FIELD_COLUMN_NAME, BASELINE_TYPE
from scr.helper.naming_helper import PREPARING_TABLE_STATEMENT, PARSING_TABLE_STATEMENT, \
	START_PARSING_STATEMENT

ZTE_XML_DESCRIPTOR_REF = 'http://ZTESpecificAttributes#ZTESpecificAttributes'
ZTE_XML_DESCRIPTOR = 'zs'

ZTE_XML_DESCRIPTOR_REF_EN = 'en'
ZTE_XML_DESCRIPTOR_EN = "http://www.3gpp.org/ftp/specs/archive/32_series/32.765#eutranNrm"

ZTE_XML_DESCRIPTOR_REF_XN = 'xn'
ZTE_XML_DESCRIPTOR_XN = "http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm"

ZTE_XML_DESCRIPTOR_REF_GN = 'gn'
ZTE_XML_DESCRIPTOR_GN = "http://www.3gpp.org/ftp/specs/archive/32_series/32.765#gsmNrm"

ZTE_XML_DESCRIPTOR_REF_UN = 'un'
ZTE_XML_DESCRIPTOR_UN = "http://www.3gpp.org/ftp/specs/archive/32_series/32.765#utranNrm"

ZTE_UME_NAMESPACE = {
					"xmlns": "http://www.zte.com/specs/nrm",
					"xsi": "http://www.w3.org/2001/XMLSchema-instance"
				}

PARSING_FILE_STATEMENT = '--- file: {0}'

external_gsm_cell = 'SubNetwork={0},ExternalGsmCell={1}'
external_utran_cell_fdd = 'SubNetwork={0},ExternalRncFunction={1},ExternalUtranCellFDD={2}'

env_mo_path = 'SubNetwork={0},ManagedElement={1},ENBFunction={2}'
eu_cell_path = 'SubNetwork={0},ManagedElement={1},ENBFunction={2},EUtranCellFDD={3}'
g2_cell_path = 'SubNetwork={0},ManagedElement={1},BssFunction={2},BtsSiteManager={3},GsmCell={4}'
g2_bsc_path = 'SubNetwork={0},ManagedElement={1},BssFunction={2}'

g3_cell_path = 'SubNetwork={0},ManagedElement={1},RncFunction={2},UtranCellFDD={3}'
g3_rnc_path = 'SubNetwork={0},ManagedElement={1},RncFunction={2}'

nr_cell_path = 'SubNetwork={0},ManagedElement={1},GNBFunction={2},NrCell={3}'
tdd_cell_path = 'SubNetwork={0},ManagedElement={1},ENBFunction={2},EUtranCellTDD={3}'
gnb_path = 'SubNetwork={0},ManagedElement={1},GNBFunction={2}'

KEY_TABLE = "{0}_{1}_{2}"

sw_column = [
	"NAME",
	"REFERENCE_FIELD",
	"SWVERSION",
	"FILENAME",
	"NETYPENAME",
	"NEFUNCTION"
]


REGEX_4G_LDN_ENBCUCPFUNC = r"^(ENBCUCPFunction=(\d+)).*$"
REGEX_4G_LDN_CELLFDDLTE = r"^((ENBCUCPFunction=([^,]*)),CULTE=([^,]*),CUEUtranCellFDDLTE=([^,]+)).*$"
REGEX_4G_LDN_CELLTDDLTE = r"^((ENBCUCPFunction=([^,]*)),CULTE=([^,]*),CUEUtranCellTDDLTE=([^,]+)).*$"
REGEX_5G_LDN_NRCELLCU = r"^(GNBCUCPFunction=((\d+)-(\d+)_(\d+)),NRCellCU=([^,]+)).*$"
REGEX_5G_LDN_NRPHYSICALCELLDU = r"^(NRRadioInfrastructure=\d+,NRPhysicalCellDU=([^,]+)).*$"
REGEX_5G_LDN_NRCARRIER = r"^(NRRadioInfrastructure=\d+,NRCarrier=([^,]+)).*$"
REGEX_5G_LDN_GNBDUFUNC = r"^GNBDUFunction=((\d+)-(\d+)_(\d+)).*$"
REGEX_5G_LDN_GNBCUCPFUNC = r"^GNBCUCPFunction=((\d+)-(\d+)_(\d+)).*$"
REGEX_SW_NAME_4G_2 = '^MO.*,MeContext=(.*),SystemFunctions=1,BrM=1,BrmBackupManager=1,BrmBackup=(.*)'
REGEX_SW_NAME_3G_2 = '^MO.*,MeContext=(.*),SystemFunctions=1,SwInventory=1,SwVersion=(.*)'


def close_connection(connection, cur):
	cur.close()
	connection.close()


def open_connection():
	dsn_tns = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, ORACLE_SID)
	connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, dsn_tns)
	cur = connection.cursor()
	return connection, cur

def strip_ns_prefix(tree):
	#xpath query for selecting all element nodes in namespace
	query = "descendant-or-self::*[namespace-uri()!='']"
	#for each element returned by the above xpath query...
	for element in tree.xpath(query):
		#replace element name with its local name
		element.tag = etree.QName(element).localname
	return tree

def parseData(node, xpath_str, index, ns=None, default=''):
	ret = default
	if node is not None and xpath_str is not None:
		try:
			if ns is not None:
				ret = node.xpath(xpath_str,  namespaces = ns)[index]
			else:
				ret = node.xpath(xpath_str)[index]
		except IndexError:
			errMsg = f"XPath={xpath_str} index={index} not found"
		except XPathEvalError:
			errMsg = f"XPath={xpath_str} eval error"
		except Exception as e:
			errMsg = f"XPath={xpath_str} index={index}, {str(e)}"
	return ret

def prepare_oracle_table_5g(oracle_con, oracle_cur, frequency_type, field_mapping_dic, base_mapping_2600_dic, drop_param=True, baseline_label_dic={}):
	for group_param in field_mapping_dic:
		table_name = naming_helper.get_table_name(BASELINE_TABLE_PREFIX.format(ZTE_TABLE_PREFIX), frequency_type, group_param)

		columns = field_mapping_dic[group_param]
		column_collection = columns

		if (ZTE_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
			ran_baseline_oracle.drop(oracle_cur, table_name)
			ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

			try:
				ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_2600_dic[group_param])
				if baseline_label_dic:
					ran_baseline_oracle.push(oracle_cur, table_name, baseline_label_dic[group_param])
			except:
				traceback.print_exc()
				pass

	if drop_param:

		if (ZTE_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
			ran_baseline_oracle.drop(oracle_cur, "SW_" + ZTE_TABLE_PREFIX + "_" + frequency_type)
			ran_baseline_oracle.create_table(oracle_cur, "SW_" + ZTE_TABLE_PREFIX + "_" + frequency_type, sw_column)

		for group_param in field_mapping_dic:
			table_name = naming_helper.get_table_name(ZTE_TABLE_PREFIX, frequency_type, group_param)
			field_mapping_dic[group_param].remove(BASELINE_TYPE)
			field_mapping_dic[group_param].append('FILENAME')
			field_mapping_dic[group_param].append('MO')
			columns = field_mapping_dic[group_param]
			column_collection = columns

			if (ZTE_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
				ran_baseline_oracle.drop(oracle_cur, table_name)
				ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

	CREATED_TABLE[ZTE_TABLE_PREFIX + "_" + frequency_type] = []
	log.i("Prepare_oracle_table End : CREATED_TABLE : " + str(CREATED_TABLE))

	oracle_con.commit()

	return

def prepare_oracle_table_4g(oracle_con, oracle_cur, frequency_type, field_mapping_dic, base_mapping_dic, red_mapping_dic, base_mapping_2600_dic, base_mapping_1800_anchor_dic, drop_param=True, baseline_label_dic={}):
	for group_param in field_mapping_dic:
		table_name = naming_helper.get_table_name(BASELINE_TABLE_PREFIX.format(ZTE_TABLE_PREFIX), frequency_type, group_param)

		columns = field_mapping_dic[group_param]
		column_collection = columns

		if (ZTE_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
			ran_baseline_oracle.drop(oracle_cur, table_name)
			ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

			try:
				ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_dic[group_param])
				ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_2600_dic[group_param])
				ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_1800_anchor_dic[group_param])

				if len(red_mapping_dic) != 0:
					ran_baseline_oracle.push(oracle_cur, table_name, red_mapping_dic[group_param])

				if baseline_label_dic:
					ran_baseline_oracle.push(oracle_cur, table_name, baseline_label_dic[group_param])

			except:
				traceback.print_exc()
				pass



	if drop_param:

		if (ZTE_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
			ran_baseline_oracle.drop(oracle_cur, "SW_" + ZTE_TABLE_PREFIX + "_" + frequency_type)
			ran_baseline_oracle.create_table(oracle_cur, "SW_" + ZTE_TABLE_PREFIX + "_" + frequency_type, sw_column)

		for group_param in field_mapping_dic:
			table_name = naming_helper.get_table_name(ZTE_TABLE_PREFIX, frequency_type, group_param)
			field_mapping_dic[group_param].remove(BASELINE_TYPE)
			field_mapping_dic[group_param].append('FILENAME')
			field_mapping_dic[group_param].append('MO')
			columns = field_mapping_dic[group_param]
			column_collection = columns

			if (ZTE_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
				ran_baseline_oracle.drop(oracle_cur, table_name)
				ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

	CREATED_TABLE[ZTE_TABLE_PREFIX + "_" + frequency_type] = []
	log.i("Prepare_oracle_table End : CREATED_TABLE : " + str(CREATED_TABLE))

	oracle_con.commit()

	return


def prepare_oracle_table_3g(oracle_con, oracle_cur, frequency_type, field_mapping_dic, base_mapping_850_dic, base_mapping_2100_dic, red_mapping_dic, drop_param=True, baseline_label_dic={}):
	for group_param in field_mapping_dic:
		table_name = naming_helper.get_table_name(BASELINE_TABLE_PREFIX.format(ZTE_TABLE_PREFIX), frequency_type, group_param)

		columns = field_mapping_dic[group_param]
		column_collection = columns

		if (ZTE_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
			ran_baseline_oracle.drop(oracle_cur, table_name)
			ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

			if ran_baseline_oracle.check_table_empty(oracle_cur, table_name) is True:
				try:
					ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_850_dic[group_param])
					ran_baseline_oracle.push(oracle_cur, table_name, base_mapping_2100_dic[group_param])
					ran_baseline_oracle.push(oracle_cur, table_name, red_mapping_dic[group_param])
					if baseline_label_dic:
						ran_baseline_oracle.push(oracle_cur, table_name, baseline_label_dic[group_param])
				except:
					traceback.print_exc()
					pass

	if drop_param:
		if (ZTE_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
			ran_baseline_oracle.drop(oracle_cur, "SW_" + ZTE_TABLE_PREFIX + "_" + frequency_type)
			ran_baseline_oracle.create_table(oracle_cur, "SW_" + ZTE_TABLE_PREFIX + "_" + frequency_type, sw_column)

		for group_param in field_mapping_dic:
			table_name = naming_helper.get_table_name(ZTE_TABLE_PREFIX, frequency_type, group_param)
			field_mapping_dic[group_param].remove(BASELINE_TYPE)
			field_mapping_dic[group_param].append('FILENAME')
			field_mapping_dic[group_param].append('MO')
			columns = field_mapping_dic[group_param]
			column_collection = columns

			if (ZTE_TABLE_PREFIX + "_" + frequency_type) not in CREATED_TABLE:
				ran_baseline_oracle.drop(oracle_cur, table_name)
				ran_baseline_oracle.create_table(oracle_cur, table_name, column_collection)

	CREATED_TABLE[ZTE_TABLE_PREFIX + "_" + frequency_type] = []
	log.i("Prepare_oracle_table End : CREATED_TABLE : " + str(CREATED_TABLE))
	oracle_con.commit()

	return


def run(source, field_mapping_dic, cell_level_dic):
	log.i("   ", ZTE_VENDOR)
	log.i("   ", ZTE_VENDOR)
	log.i(START_PARSING_STATEMENT.format(source.FrequencyType, source.Region), ZTE_VENDOR)
	log.i(PREPARING_TABLE_STATEMENT, ZTE_VENDOR)

	log.i(PARSING_TABLE_STATEMENT, ZTE_VENDOR)

	pool = mp.Pool(processes=MAX_RUNNING_PROCESS)

	for raw_file in source.RawFileList:

		if source.FrequencyType == '2G':
			# parse_2g(raw_file, source.FrequencyType, field_mapping_dic, cell_level_dic)
			pool.apply_async(parse_2g, args=(raw_file, source.FrequencyType, field_mapping_dic, cell_level_dic,))

		elif source.FrequencyType == '3G':
			# parse3g(raw_file, source.FrequencyType, field_mapping_dic, oracle_con, oracle_cur, cell_level_dic)
			pool.apply_async(parse_3g, args=(raw_file, source.FrequencyType, field_mapping_dic, cell_level_dic,))

		elif source.FrequencyType == '4G':
			# parse_4g(raw_file, source.FrequencyType, field_mapping_dic, cell_level_dic)
			if ITBBU_FILE in raw_file:
				pool.apply_async(parse_itbbu, args=(raw_file, source.FrequencyType, field_mapping_dic, cell_level_dic,))
			else:
				pool.apply_async(parse_4g, args=(raw_file, source.FrequencyType, field_mapping_dic, cell_level_dic,))
		elif source.FrequencyType == '5G':
			pool.apply_async(parse_itbbu, args=(raw_file, source.FrequencyType, field_mapping_dic, cell_level_dic,))

	pool.close()
	pool.join()

	print("Done -------Pool ALL")


def parse_2g(raw_file, frequency_type, field_mapping_dic, cell_level_dic):
	try:
		log.i(PARSING_FILE_STATEMENT.format(raw_file), ZTE_VENDOR, frequency_type)

		oracle_con, oracle_cur = open_connection()

		log.i("----- Start Parser : " + str(datetime.datetime.now()), ZTE_VENDOR, frequency_type)

		mongo_result = {}
		oracle_result = {}

		tree = ElementInclude.default_loader(raw_file, 'xml')

		subnet = tree.xpath('.//xn:SubNetwork', namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF, ZTE_XML_DESCRIPTOR_REF_XN: ZTE_XML_DESCRIPTOR_XN})
		subid = subnet[1].xpath('@id')[0]
		start_parser_time = datetime.datetime.now()
		filename = raw_file.split(PATH_SEPARATOR)[-1]

		subnetwork_userLabel = ""

		# external_rnc = tree.xpath('.//xn:SubNetwork//un:ExternalRncFunction',
		#                           namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF, ZTE_XML_DESCRIPTOR_REF_UN: ZTE_XML_DESCRIPTOR_UN})

		subnetwork = tree.xpath('.//xn:SubNetwork//xn:SubNetwork//xn:attributes//xn:userLabel',
								namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF, ZTE_XML_DESCRIPTOR_REF_XN: ZTE_XML_DESCRIPTOR_XN})

		if len(subnetwork) > 0:
			if subnetwork[0].text is not None:
				subnetwork_userLabel = subnetwork[0].text

		if len(subnetwork) > 1:
			if subnetwork[1].text is not None:
				subnetwork_userLabel = subnetwork[1].text

		if len(subnet) <= 1:
			log.e("subnet size = <= 1", ZTE_VENDOR, frequency_type)
			return

		for e in subnet[1]:
			submo = etree.fromstring(etree.tostring(e))
			tag = submo.tag.replace('{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#gsmNrm}', '')

			if tag == "ExternalGsmCell":
				external_gsmcell_id = submo.xpath('@id')[0]
				parameter_group = tag
				valuedic = field_mapping_dic[parameter_group]

				userLabel = ""
				bcchFrequency = ""
				bcc = ""
				ncc = ""
				lac = ""

				mongo_value_pair_dic = {}
				oracle_value_pair_dic = dict.fromkeys(valuedic, '')

				cell_type = cell_level_dic[parameter_group]

				for data_tmp in submo:
					for data_tmp_2 in data_tmp:
						tag = data_tmp_2.tag.replace('{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#gsmNrm}', '')
						if tag == "userLabel":
							userLabel = data_tmp_2.text
						elif tag.upper() == 'BCCHFREQUENCY':
							bcchFrequency = data_tmp_2.text
						elif tag.upper() == 'BCC':
							bcc = data_tmp_2.text
						elif tag.upper() == 'NCC':
							ncc = data_tmp_2.text
						elif tag.upper() == 'LAC':
							lac = data_tmp_2.text
					break

				xpath = './/zs:vsData{0}'.format("ExternalGsmCell")
				mo_group_collection = e.xpath(xpath, namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF, ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})

				for externalgsmcell in mo_group_collection:

					for gsmcell in externalgsmcell:
						tag = gsmcell.tag.replace('{http://ZTESpecificAttributes#ZTESpecificAttributes}', '')

						value = gsmcell.text
						mongo_value_pair_dic[tag.upper()] = value

						if tag.upper() in oracle_value_pair_dic:
							oracle_value_pair_dic[tag.upper()] = value

					oracle_value_pair_dic["BCCHFREQUENCY"] = bcchFrequency
					oracle_value_pair_dic["BCC"] = bcc
					oracle_value_pair_dic["NCC"] = ncc
					oracle_value_pair_dic["LAC"] = lac
					oracle_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = userLabel
					oracle_value_pair_dic['FILENAME'] = filename
					oracle_value_pair_dic['LV'] = cell_type
					oracle_value_pair_dic['MO'] = external_gsm_cell.format(subid, external_gsmcell_id)

					mongo_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = userLabel
					mongo_value_pair_dic['FILENAME'] = filename
					mongo_value_pair_dic['LV'] = cell_type
					mongo_value_pair_dic['MO'] = external_gsm_cell.format(subid, external_gsmcell_id)

					if KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group) not in COUNT_DATA:
						COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = 0
					COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] + 1

				if parameter_group in mongo_result:
					mongo_result[parameter_group].append(mongo_value_pair_dic)
				else:
					mongo_result[parameter_group] = []
					mongo_result[parameter_group].append(mongo_value_pair_dic)

				if parameter_group in oracle_result:
					oracle_result[parameter_group].append(oracle_value_pair_dic)
				else:
					oracle_result[parameter_group] = []
					oracle_result[parameter_group].append(oracle_value_pair_dic)

			else:

				manage_group_collection = submo.xpath('.//xn:ManagedElement', namespaces={ZTE_XML_DESCRIPTOR_REF_XN: ZTE_XML_DESCRIPTOR_XN})

				for manage_group in manage_group_collection:

					manageid = manage_group.xpath('@id')[0]

					bss_group_collection = manage_group.xpath('.//gn:BssFunction',
															  namespaces={
																  'gn': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#gsmNrm',
																  ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})

					manage_att = manage_group.xpath('.//xn:attributes',
													namespaces={
														ZTE_XML_DESCRIPTOR_REF_XN: ZTE_XML_DESCRIPTOR_XN,
														ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})

					if len(manage_att) > 0:
						dic = dict.fromkeys(sw_column, '')

						for attributes in manage_att:
							for attribute in attributes:
								tag = attribute.tag.replace(
									'{http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm}', '')
								if tag == 'userLabel':
									manage_userlabel = attribute.text
									dic["NAME"] = manage_userlabel
									dic["REFERENCE_FIELD"] = subnetwork_userLabel
								elif tag == "swVersion":
									manage_swVersion = attribute.text
									dic["SWVERSION"] = manage_swVersion
									# break

								elif tag == "managedElementType":
									manage_managedElementType = attribute.text
									dic["NETYPENAME"] = manage_managedElementType
									# break

								else:
									continue

							break

						sw_result = {}

						dic["FILENAME"] = filename
						dic["NEFUNCTION"] = "BSC"

						sw_key = "SW_" + ZTE_TABLE_PREFIX + "_" + frequency_type

						sw_result[sw_key] = []
						sw_result[sw_key].append(dic)

						ran_baseline_oracle.push(oracle_cur, sw_key, sw_result[sw_key])
						oracle_con.commit()

					for bss in bss_group_collection:
						bssid = bss.xpath('@id')[0]

						for parameter_group, valuedic in field_mapping_dic.items():
							cell_type = cell_level_dic[parameter_group]

							if cell_type == 'CELL Level':
								# log.i('This is Cell Level')
								btsmo = bss.xpath('.//gn:BtsSiteManager',
												  namespaces={'gn': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#gsmNrm',
															  ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})

								for bts in btsmo:
									btsid = bts.xpath('@id')[0]

									gsm_cell_collection = bts.xpath('.//gn:GsmCell',
																	namespaces={'gn': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#gsmNrm',
																				ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})

									for gsm in gsm_cell_collection:
										gsmid = gsm.xpath('@id')[0]
										attribute_group = gsm.xpath('.//gn:attributes',
																	namespaces={
																		'gn': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#gsmNrm',
																		ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})

										rxlevaccessmin = ""

										if len(attribute_group) > 0:
											for attribute in attribute_group[0]:
												tag = attribute.tag.replace(
													'{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#gsmNrm}', '')
												if tag == 'userLabel':
													valuess = attribute.text
													# log.i('CellLevel: ' + str(valuess))
													break

												if tag.upper() == 'RXLEVACCESSMIN' and parameter_group.upper() == 'GsmCell'.upper():
													rxlevaccessmin = attribute.text

										xpath = './/zs:vsData{0}'.format(parameter_group)
										mo_group_collection = gsm.xpath(xpath,
																		namespaces={
																			ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF,
																			ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})

										for enb_moo in mo_group_collection:
											mongo_value_pair_dic = {}
											oracle_value_pair_dic = dict.fromkeys(valuedic, '')

											if parameter_group == 'GsmCell':
												oracle_value_pair_dic["RXLEVACCESSMIN"] = rxlevaccessmin

											for attribute in enb_moo:
												tag = attribute.tag.replace(
													'{http://ZTESpecificAttributes#ZTESpecificAttributes}',
													'')
												value = attribute.text
												mongo_value_pair_dic[tag] = value

												if tag.upper() in oracle_value_pair_dic:
													oracle_value_pair_dic[tag.upper()] = value

													# xsubmo = mo_xml.xpath('.//en:EUtranCellFDD',
													#                       namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF,
													#                                   ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})
													# log.i(xsubmo)

											if KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group) not in COUNT_DATA:
												COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = 0
											COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] + 1

											oracle_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = valuess
											oracle_value_pair_dic['FILENAME'] = filename
											oracle_value_pair_dic['LV'] = cell_type
											oracle_value_pair_dic['MO'] = g2_cell_path.format(subid, manageid, bssid, btsid, gsmid)

											mongo_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = valuess
											mongo_value_pair_dic['FILENAME'] = filename
											mongo_value_pair_dic['LV'] = cell_type
											mongo_value_pair_dic['MO'] = g2_cell_path.format(subid, manageid, bssid, btsid, gsmid)

											if parameter_group in mongo_result:
												mongo_result[parameter_group].append(mongo_value_pair_dic)
											else:
												mongo_result[parameter_group] = []
												mongo_result[parameter_group].append(mongo_value_pair_dic)

											if parameter_group in oracle_result:
												oracle_result[parameter_group].append(oracle_value_pair_dic)
											else:
												oracle_result[parameter_group] = []
												oracle_result[parameter_group].append(oracle_value_pair_dic)

							else:
								mo_xml = etree.fromstring(etree.tostring(bss))

								for enb_mo in mo_xml:
									xpath = './/zs:vsData{0}'.format(parameter_group)
									mo_group_collection = enb_mo.xpath(xpath,
																	   namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF,
																				   ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})
									for enb_moo in mo_group_collection:
										mongo_value_pair_dic = {}
										oracle_value_pair_dic = dict.fromkeys(valuedic, '')
										for attribute in enb_moo:
											tag = attribute.tag.replace('{http://ZTESpecificAttributes#ZTESpecificAttributes}',
																		'')

											value = attribute.text
											mongo_value_pair_dic[tag.upper()] = value

											if tag.upper() in oracle_value_pair_dic:
												oracle_value_pair_dic[tag.upper()] = value

												# xsubmo = mo_xml.xpath('.//en:EUtranCellFDD',
												#                       namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF,
												#                                   ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})
												# log.i(xsubmo)

										if KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group) not in COUNT_DATA:
											COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = 0
										COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] + 1

										oracle_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = manage_userlabel
										oracle_value_pair_dic['FILENAME'] = raw_file.split(PATH_SEPARATOR)[-1]
										oracle_value_pair_dic['LV'] = cell_type
										oracle_value_pair_dic['MO'] = g2_bsc_path.format(subid, manageid, bssid)

										mongo_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = manage_userlabel
										mongo_value_pair_dic['FILENAME'] = raw_file.split(PATH_SEPARATOR)[-1]
										mongo_value_pair_dic['LV'] = cell_type
										mongo_value_pair_dic['MO'] = g2_bsc_path.format(subid, manageid, bssid)

										if parameter_group in mongo_result:
											mongo_result[parameter_group].append(mongo_value_pair_dic)
										else:
											mongo_result[parameter_group] = []
											mongo_result[parameter_group].append(mongo_value_pair_dic)

										if parameter_group in oracle_result:
											oracle_result[parameter_group].append(oracle_value_pair_dic)
										else:
											oracle_result[parameter_group] = []
											oracle_result[parameter_group].append(oracle_value_pair_dic)

		# log.i('---- pushing to mongo')
		# for result in mongo_result:
		#     table_name = naming_helper.get_table_name(ZTE_TABLE_PREFIX, frequency_type, result)
		#     try:
		#         granite_mongo.push(table_name, mongo_result[result])
		#     except Exception as e:
		#         log.e('#################################### Error occur granite_mongo (003): ', ZTE_VENDOR, frequency_type)
		#         log.e('Exception Into Table: ' + table_name, ZTE_VENDOR, frequency_type)
		#         log.e(e, ZTE_VENDOR, frequency_type)
		#         traceback.print_exc()
		#         log.e('#################################### Error granite_mongo ', ZTE_VENDOR, frequency_type)
		#
		#         continue

		# log.i('---- generating SQL insert statement')
		# for result in oracle_result:
		#     table_name = naming_helper.get_table_name(ZTE_TABLE_PREFIX, frequency_type, result)
		#     sqllist = ran_baseline_oracle.get_sql_insert_statment(table_name, oracle_result[result])
		#
		#     file = open(result + '.sql', 'w')
		#     for s in sqllist:
		#         file.write(s + ';\n')
		#     file.close()

		log.i('---- pushing to oracle', ZTE_VENDOR, frequency_type)
		for result in oracle_result:

			table_name = naming_helper.get_table_name(ZTE_TABLE_PREFIX, frequency_type, result)

			try:
				ran_baseline_oracle.push(oracle_cur, table_name, oracle_result[result])

			except Exception as e:
				log.e('#################################### Error occur (002): ', ZTE_VENDOR, frequency_type)
				log.e('Exception Into Table: ' + table_name, ZTE_VENDOR, frequency_type)
				log.e(e, ZTE_VENDOR, frequency_type)
				traceback.print_exc()
				log.e('#################################### Error ', ZTE_VENDOR, frequency_type)

				oracle_con.commit()
				oracle_con, oracle_cur = open_connection()
				continue

		oracle_con.commit()
		log.i("Done :::: " + filename + " ::::::::", ZTE_VENDOR, frequency_type)
		log.i("<<<< Time : " + str(datetime.datetime.now() - start_parser_time), ZTE_VENDOR, frequency_type)

		close_connection(oracle_con, oracle_cur)

	except:
		pass


def parse_3g(raw_file, frequency_type, field_mapping_dic, cell_level_dic):
	log.i(PARSING_FILE_STATEMENT.format(raw_file), ZTE_VENDOR, frequency_type)

	oracle_con, oracle_cur = open_connection()

	log.i("----- Start Parser : " + str(datetime.datetime.now()), ZTE_VENDOR, frequency_type)
	mongo_result = {}
	oracle_result = {}

	tree = ElementInclude.default_loader(raw_file, 'xml')

	subnet = tree.xpath('.//xn:SubNetwork', namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF,
														ZTE_XML_DESCRIPTOR_REF_XN: ZTE_XML_DESCRIPTOR_XN})
	subid = subnet[1].xpath('@id')[0]
	filename = raw_file.split(PATH_SEPARATOR)[-1]
	subnetwork_userLabel = ""

	# external_rnc = tree.xpath('.//xn:SubNetwork//un:ExternalRncFunction',
	#                           namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF, ZTE_XML_DESCRIPTOR_REF_UN: ZTE_XML_DESCRIPTOR_UN})

	subnetwork = tree.xpath('.//xn:SubNetwork//xn:SubNetwork//xn:attributes//xn:userLabel',
							namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF, ZTE_XML_DESCRIPTOR_REF_XN: ZTE_XML_DESCRIPTOR_XN})

	if len(subnetwork) > 0:
		subnetwork_userLabel = subnetwork[0].text

	for diff_2 in subnet[0]:
		tag = diff_2.tag.replace('{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#utranNrm}', '')

		if tag == 'ExternalRncFunction':

			externalrncfunction_id = diff_2.xpath('@id')[0]
			externalrnc = diff_2.xpath('.//un:ExternalUtranCellFDD', namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF, ZTE_XML_DESCRIPTOR_REF_UN: ZTE_XML_DESCRIPTOR_UN})

			for externalgsmcell in externalrnc:
				tag = externalgsmcell.tag.replace('{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#utranNrm}', '')

				if tag == 'ExternalUtranCellFDD':
					parameter_group = tag

					if tag in field_mapping_dic:

						valuedic = field_mapping_dic[parameter_group]
						mongo_value_pair_dic = {}
						oracle_value_pair_dic = dict.fromkeys(valuedic, '')
						cell_type = cell_level_dic[parameter_group]

						externalutrancellfdd_id = externalgsmcell.xpath('@id')[0]
						for attributes in externalgsmcell:

							for gsmcell in attributes:
								tag = gsmcell.tag.replace('{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#utranNrm}', '')
								value = gsmcell.text
								mongo_value_pair_dic[tag.upper()] = value

								if tag.upper() in oracle_value_pair_dic:
									oracle_value_pair_dic[tag.upper()] = value

							oracle_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = subnetwork_userLabel
							oracle_value_pair_dic['FILENAME'] = filename
							oracle_value_pair_dic['LV'] = cell_type
							oracle_value_pair_dic['MO'] = external_utran_cell_fdd.format(subid, externalrncfunction_id, externalutrancellfdd_id)

							mongo_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = subnetwork_userLabel
							mongo_value_pair_dic['FILENAME'] = filename
							mongo_value_pair_dic['LV'] = cell_type
							mongo_value_pair_dic['MO'] = external_utran_cell_fdd.format(subid, externalrncfunction_id, externalutrancellfdd_id)

							if KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group) not in COUNT_DATA:
								COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = 0
							COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] + 1

						if parameter_group in mongo_result:
							mongo_result[parameter_group].append(mongo_value_pair_dic)
						else:
							mongo_result[parameter_group] = []
							mongo_result[parameter_group].append(mongo_value_pair_dic)

						if parameter_group in oracle_result:
							oracle_result[parameter_group].append(oracle_value_pair_dic)
						else:
							oracle_result[parameter_group] = []
							oracle_result[parameter_group].append(oracle_value_pair_dic)

	manage_group_collection = tree.xpath('.//xn:SubNetwork//xn:ManagedElement', namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF,
																							ZTE_XML_DESCRIPTOR_REF_XN: ZTE_XML_DESCRIPTOR_XN})
	manageid = ""

	if len(manage_group_collection) > 0:
		manageid = manage_group_collection[0].xpath('@id')[0]

	start_parser_time = datetime.datetime.now()

	dic = dict.fromkeys(sw_column, '')

	for manage_group in manage_group_collection:
		for attributes in manage_group:
			for attribute in attributes:
				tag = attribute.tag.replace(
					'{http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm}', '')
				if tag == 'userLabel':
					manage_userlabel = subnetwork_userLabel
					# manage_userlabel = attribute.text
					dic["NAME"] = attribute.text
					dic["REFERENCE_FIELD"] = manage_userlabel
				elif tag == "swVersion":
					manage_swVersion = attribute.text
					dic["SWVERSION"] = manage_swVersion
					# break

				elif tag == "managedElementType":
					manage_managedElementType = attribute.text
					dic["NETYPENAME"] = manage_managedElementType
					# break

				else:
					continue

			break

	sw_result = {}

	dic["FILENAME"] = filename
	dic["NEFUNCTION"] = "NodeB"

	sw_key = "SW_" + ZTE_TABLE_PREFIX + "_" + frequency_type

	sw_result[sw_key] = []
	sw_result[sw_key].append(dic)

	ran_baseline_oracle.push(oracle_cur, sw_key, sw_result[sw_key])
	oracle_con.commit()

	rnc_group_collection = manage_group_collection[0].xpath('.//un:RncFunction',
															namespaces={
																'un': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#utranNrm',
																ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})
	valuess = subnetwork_userLabel
	eutrancellid = ""

	rnc_mo = rnc_group_collection[0]

	rncId = rnc_mo.xpath('@id')[0]

	base_xml = etree.fromstring(etree.tostring(rnc_mo))

	for parameter_group, valuedic in field_mapping_dic.items():

		mongo_value_pair_dic = {}
		oracle_value_pair_dic = dict.fromkeys(valuedic, '')

		cell_type = cell_level_dic[parameter_group]

		if parameter_group == 'ExternalUtranCellFDD':
			continue
			# print(parameter_group)
			# rnc_group_collection = tree.xpath('.//xn:SubNetwork//un:ExternalRncFunction',
			#                                                         namespaces={
			#                                                             'un': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#utranNrm',
			#                                                             ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})
			#
			# rnc_mo = rnc_group_collection[0]
			#
			# rncId = rnc_mo.xpath('@id')[0]
			#
			# print(rncId)
		else:

			if cell_type == 'CELL Level':
				# log.i('This is Cell Level')
				mo_xml = base_xml.xpath('.//un:UtranCellFDD',
										namespaces={'un': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#utranNrm',
													ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})

			else:
				mo_xml = base_xml
				# log.i('This is eNodeB Level')

			for enb_mo in mo_xml:
				if cell_type == 'RNC Level':
					valuess = subnetwork_userLabel

				else:

					if parameter_group == 'nan' or enb_mo.xpath('@id') == False or len(enb_mo.xpath('@id')) == 0:
						print(enb_mo)
						continue

					eutrancellid = enb_mo.xpath('@id')[0]

					eutranatt = enb_mo.xpath('.//un:attributes',
											 namespaces={
												 'un': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#utranNrm',
												 ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})

					if len(eutranatt) > 0:

						if parameter_group == 'UtranCellFDD':

							mongo_value_pair_dic = {}
							oracle_value_pair_dic = dict.fromkeys(valuedic, '')

							for attribute in eutranatt:
								for item in attribute:

									tag = item.tag.replace('{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#utranNrm}', '')
									value = item.text

									mongo_value_pair_dic[tag] = value

									if tag.upper() in oracle_value_pair_dic:
										oracle_value_pair_dic[tag.upper()] = value

						for attribute in eutranatt[0]:
							tag = attribute.tag.replace(
								'{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#utranNrm}', '')
							if tag == 'userLabel':
								valuess = attribute.text
								break

				xpath = './/zs:vsData{0}'.format(parameter_group)
				mo_group_collection = enb_mo.xpath(xpath,
												   namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF,
															   ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})
				for rnc_mo in mo_group_collection:

					if parameter_group != 'UtranCellFDD':
						mongo_value_pair_dic = {}
						oracle_value_pair_dic = dict.fromkeys(valuedic, '')

					for attribute in rnc_mo:
						tag = attribute.tag.replace('{http://ZTESpecificAttributes#ZTESpecificAttributes}', '')
						value = attribute.text
						mongo_value_pair_dic[tag] = value

						if tag.upper() in oracle_value_pair_dic:
							oracle_value_pair_dic[tag.upper()] = value

					oracle_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = valuess
					oracle_value_pair_dic["LV"] = cell_type
					oracle_value_pair_dic['FILENAME'] = filename

					mongo_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = valuess
					mongo_value_pair_dic["LV"] = cell_type
					mongo_value_pair_dic['FILENAME'] = filename

					if KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group) not in COUNT_DATA:
						COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = 0
					COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] + 1

					if cell_type == 'RNC Level':
						oracle_value_pair_dic['MO'] = g3_rnc_path.format(subid, manageid, rncId)
						mongo_value_pair_dic['MO'] = g3_rnc_path.format(subid, manageid, rncId)
					else:
						oracle_value_pair_dic['MO'] = g3_cell_path.format(subid, manageid, rncId, eutrancellid)
						mongo_value_pair_dic['MO'] = g3_cell_path.format(subid, manageid, rncId, eutrancellid)

					if parameter_group in mongo_result:
						mongo_result[parameter_group].append(mongo_value_pair_dic)
					else:
						mongo_result[parameter_group] = []
						mongo_result[parameter_group].append(mongo_value_pair_dic)

					if parameter_group in oracle_result:
						oracle_result[parameter_group].append(oracle_value_pair_dic)
					else:
						oracle_result[parameter_group] = []
						oracle_result[parameter_group].append(oracle_value_pair_dic)

	log.i('---- pushing to oracle')
	for result in oracle_result:

		table_name = naming_helper.get_table_name(ZTE_TABLE_PREFIX, frequency_type, result)
		# granite_mongo.push(table_name, mongo_result[result])
		try:
			ran_baseline_oracle.push(oracle_cur, table_name, oracle_result[result])
			oracle_con.commit()
		except Exception as e:
			log.i('Exception Into Table: ' + table_name)
			log.i(e)
			traceback.print_exc()
			oracle_con.commit()
			oracle_con, oracle_cur = open_connection()
			continue

	log.i("Done :::: " + filename + " ::::::::", ZTE_VENDOR, frequency_type)
	log.i("<<<< Time : " + str(datetime.datetime.now() - start_parser_time), ZTE_VENDOR, frequency_type)

	close_connection(oracle_con, oracle_cur)


def parse_4g(raw_file, frequency_type, field_mapping_dic, cell_level_dic):
	log.i(PARSING_FILE_STATEMENT.format(raw_file), ZTE_VENDOR, frequency_type)

	oracle_con, oracle_cur = open_connection()

	log.i("----- Start Parser : " + str(datetime.datetime.now()), ZTE_VENDOR, frequency_type)

	mongo_result = {}
	oracle_result = {}

	tree = ElementInclude.default_loader(raw_file, 'xml')

	subnet = tree.xpath('.//xn:SubNetwork', namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF,
														ZTE_XML_DESCRIPTOR_REF_XN: ZTE_XML_DESCRIPTOR_XN})
	subid = subnet[1].xpath('@id')[0]

	start_parser_time = datetime.datetime.now()

	filename = raw_file.split(PATH_SEPARATOR)[-1]
	eutrancellid = ""

	for e in subnet[1]:
		submo = etree.fromstring(etree.tostring(e))

		manage_group_collection = submo.xpath('.//xn:ManagedElement', namespaces={ZTE_XML_DESCRIPTOR_REF_XN: ZTE_XML_DESCRIPTOR_XN})

		for manage_group in manage_group_collection:

			manageid = manage_group.xpath('@id')[0]

			if len(manage_group_collection) > 0:

				sw_name = ''
				sw_reference_field = ''
				sw_version = ''
				sw_netypename = ''

				for attributes in manage_group:
					dic = dict.fromkeys(sw_column, '')

					tag = attributes.tag.replace('{http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm}', '')
					tag = tag.replace('{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#eutranNrm}', '')

					if tag == 'attributes':

						for attribute in attributes:
							tag = attribute.tag.replace(
								'{http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm}', '')

							if tag == "swVersion":
								sw_version = attribute.text

							elif tag == "managedElementType":
								sw_netypename = attribute.text

								# break

							else:
								continue
					elif tag == 'ENBFunction':

						for attribute in attributes:

							tag = attribute.tag.replace(
								'{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#eutranNrm}', '')

							if tag == 'attributes':
								for attributeItem in attribute:
									tag = attributeItem.tag.replace(
										'{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#eutranNrm}', '')

									if tag == 'userLabel':
										sw_result = {}

										manage_userlabel = attributeItem.text
										dic["NAME"] = manage_userlabel
										dic["REFERENCE_FIELD"] = manage_userlabel

										dic["SWVERSION"] = sw_version
										dic["NETYPENAME"] = sw_netypename

										dic["FILENAME"] = filename
										dic["NEFUNCTION"] = "eNodeB"

										sw_key = "SW_" + ZTE_TABLE_PREFIX + "_" + frequency_type

										sw_result[sw_key] = []
										sw_result[sw_key].append(dic)

										ran_baseline_oracle.push(oracle_cur, sw_key, sw_result[sw_key])
										oracle_con.commit()

			enb_group_collection = manage_group.xpath('.//en:ENBFunction',
													  namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF,
																  ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})

			for enb_moo in enb_group_collection:

				enbid = enb_moo.xpath('@id')[0]

				base_xml = etree.fromstring(etree.tostring(enb_moo))

				for parameter_group, valuedic in field_mapping_dic.items():
					# mongo_result[parameter_group] = []
					# oracle_result[parameter_group] = []

					cell_type = cell_level_dic[parameter_group]

					if cell_type == 'CELL Level':
						# log.i('This is Cell Level')
						mo_xml = base_xml.xpath('.//en:EUtranCellFDD',
												namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF,
															ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})


					else:
						mo_xml = base_xml
						# log.i('This is eNodeB Level')

					for enb_mo in mo_xml:

						mongo_value_pair_dic = {}
						oracle_value_pair_dic = dict.fromkeys(valuedic, '')

						if cell_type == 'eNodeB Level':
							for v1 in mo_xml:
								for atts in v1:
									tag = atts.tag.replace(
										'{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#eutranNrm}', '')
									if tag == 'userLabel':
										valuess = atts.text
										# log.i(cell_type + ' || ' + valuess)
										break

						else:
							# log.i(cell_type)
							# log.i(parameter_group)
							# log.i('utranId:')
							eutrancellid = enb_mo.xpath('@id')[0]
							# log.i(eutrancellid)

							eutranatt = enb_mo.xpath('.//en:attributes',
													 namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF,
																 ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})

							if len(eutranatt) > 0:
								for attribute in eutranatt[0]:
									tag = attribute.tag.replace(
										'{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#eutranNrm}', '')

									valueTmp = attribute.text
									if tag == 'userLabel':
										valuess = attribute.text
										# log.i(cell_type +' || ' + valuess)

									mongo_value_pair_dic[tag.upper()] = valueTmp

									if tag.upper() in oracle_value_pair_dic:
										oracle_value_pair_dic[tag.upper()] = valueTmp

										# break

						if parameter_group == 'EUtranRelation':
							eutranrelations = enb_mo.xpath('.//en:EUtranRelation', namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF, ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})

							mongo_value_pair_dic = {}
							oracle_value_pair_dic = dict.fromkeys(valuedic, '')

							for eutranrelation in eutranrelations:

								for attributes in eutranrelation:
									tag = attributes.tag.replace('{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#eutranNrm}', '')

									if tag == 'attributes':
										for attribute in attributes:
											tag = attribute.tag.replace('{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#eutranNrm}', '')
											value = attribute.text

											mongo_value_pair_dic[tag.upper()] = value

											if tag.upper() in oracle_value_pair_dic:
												oracle_value_pair_dic[tag.upper()] = value
									else:
										for nodes in attributes:
											for node in nodes:
												for vsdata in node:

													tag = vsdata.tag.replace('{http://ZTESpecificAttributes#ZTESpecificAttributes}', '')
													value = vsdata.text

													mongo_value_pair_dic[tag.upper()] = value

													if tag.upper() in oracle_value_pair_dic:
														oracle_value_pair_dic[tag.upper()] = value

							oracle_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = valuess
							oracle_value_pair_dic['FILENAME'] = filename

							mongo_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = valuess
							mongo_value_pair_dic['FILENAME'] = filename

							if KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group) not in COUNT_DATA:
								COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = 0
							COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] + 1

							if cell_type == 'eNodeB Level':
								oracle_value_pair_dic['MO'] = env_mo_path.format(subid, manageid, enbid)
								mongo_value_pair_dic['MO'] = env_mo_path.format(subid, manageid, enbid)
							else:
								oracle_value_pair_dic['MO'] = eu_cell_path.format(subid, manageid, enbid, eutrancellid)
								mongo_value_pair_dic['MO'] = eu_cell_path.format(subid, manageid, enbid, eutrancellid)

							if parameter_group in mongo_result:
								mongo_result[parameter_group].append(mongo_value_pair_dic)
							else:
								mongo_result[parameter_group] = []
								mongo_result[parameter_group].append(mongo_value_pair_dic)

							if parameter_group in oracle_result:
								oracle_result[parameter_group].append(oracle_value_pair_dic)
							else:
								oracle_result[parameter_group] = []
								oracle_result[parameter_group].append(oracle_value_pair_dic)




						else:

							xpath = './/zs:vsData{0}'.format(parameter_group)
							mo_group_collection = enb_mo.xpath(xpath,
															   namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF,
																		   ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN})

							extra_value = ""
							for enb_moo in mo_group_collection:

								for attribute in enb_moo:
									tag = attribute.tag.replace('{http://ZTESpecificAttributes#ZTESpecificAttributes}', '')

									tag = naming_helper.rule_column_name(tag)

									value = attribute.text
									mongo_value_pair_dic[tag.upper()] = value

									if parameter_group == 'ECellEquipmentFunction' and tag.upper() == 'DESCRIPTION':
										extra_value = "," + value.split(',')[0]

									if tag.upper() in oracle_value_pair_dic:
										oracle_value_pair_dic[tag.upper()] = value

								oracle_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = valuess
								oracle_value_pair_dic['FILENAME'] = filename

								mongo_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = valuess
								mongo_value_pair_dic['FILENAME'] = filename

								if KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group) not in COUNT_DATA:
									COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = 0
								COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] + 1

								if cell_type == 'eNodeB Level':
									oracle_value_pair_dic['MO'] = env_mo_path.format(subid, manageid, enbid) + extra_value
									mongo_value_pair_dic['MO'] = env_mo_path.format(subid, manageid, enbid) + extra_value
								else:
									oracle_value_pair_dic['MO'] = eu_cell_path.format(subid, manageid, enbid, eutrancellid)
									mongo_value_pair_dic['MO'] = eu_cell_path.format(subid, manageid, enbid, eutrancellid)

								if parameter_group in mongo_result:
									mongo_result[parameter_group].append(mongo_value_pair_dic)
								else:
									mongo_result[parameter_group] = []
									mongo_result[parameter_group].append(mongo_value_pair_dic)

								if parameter_group in oracle_result:
									oracle_result[parameter_group].append(oracle_value_pair_dic)
								else:
									oracle_result[parameter_group] = []
									oracle_result[parameter_group].append(oracle_value_pair_dic)

								mongo_value_pair_dic = {}
								oracle_value_pair_dic = dict.fromkeys(valuedic, '')

	log.i('---- Start pushing to oracle : ', ZTE_VENDOR, frequency_type)
	for result in oracle_result:

		table_name = naming_helper.get_table_name(ZTE_TABLE_PREFIX, frequency_type, result)
		# granite_mongo.push(table_name, mongo_result[result])
		try:
			ran_baseline_oracle.push(oracle_cur, table_name, oracle_result[result])
			oracle_con.commit()
		except Exception as e:
			log.e('#################################### Error occur (001): ', ZTE_VENDOR, frequency_type)
			log.e('Exception Into Table: ' + table_name, ZTE_VENDOR, frequency_type)
			log.e(e, ZTE_VENDOR, frequency_type)
			traceback.print_exc()
			log.e('#################################### Error ', ZTE_VENDOR, frequency_type)

			oracle_con.commit()
			oracle_con, oracle_cur = open_connection()
			continue

	log.i("Done :::: " + filename + " ::::::::", ZTE_VENDOR, frequency_type)
	log.i("<<<< Time : " + str(datetime.datetime.now() - start_parser_time), ZTE_VENDOR, frequency_type)

	close_connection(oracle_con, oracle_cur)

# CR2020-NR/L2600
def parse_itbbu(raw_file, frequency_type, field_mapping_dic, cell_level_dic):
	log.i(PARSING_FILE_STATEMENT.format(raw_file), ZTE_VENDOR, frequency_type)

	oracle_con, oracle_cur = open_connection()
	log.i("----- Start Parser : " + str(datetime.datetime.now()), ZTE_VENDOR, frequency_type)

	mongo_result = {}
	oracle_result = {}

	start_parser_time = datetime.datetime.now()
	filename = raw_file.split(PATH_SEPARATOR)[-1]

	# tree = ElementInclude.default_loader(raw_file, 'xml')
	parser = etree.XMLParser(recover=True, encoding='UTF-8')
	with open(raw_file, encoding="UTF-8", errors='ignore') as xml_file:
		try:
			# Create a parser
			tree = strip_ns_prefix(etree.parse(xml_file, parser=parser))
			# tree = etree.parse(xml_file, parser=parser)
			ns = {
				"xmlns": "http://www.zte.com/specs/nrm",
				"xsi": "http://www.w3.org/2001/XMLSchema-instance"
			}

			neData_collection = tree.xpath(u'//NEData', namespaces=ns)
			node_cnt = 0

			for neData in neData_collection:

				subNetwork = neData.get('SubNetwork')
				managedElement = neData.get('ManagedElement')
				# !!! not using namespace due to xpath "following-sibling" problem, unable to detect GNBname
				# nodes = neData.xpath(u'.//xmlns:module[@name="nr"]', namespaces=ns)

				if frequency_type == '5G':

					nodes = neData.xpath(u'.//module[@name="nr"]', namespaces=ns)
					if len(nodes) < 1:
						# print(f'NR Cell not found in subNetwork={subNetwork}, managedElement={managedElement}')
						continue

					for node in nodes:
						refNRPhysicalCellDU_dic = {}
						refCellCU_dic = {}
						cell_dic = {}
						gnb_dic = {}
						refNrCarrier_dic = {}

						# Get NR DU Cell
						nr_cell_dus = node.xpath('.//mo[@moc="NRCellDU"]', namespaces=ns)
						for nr_cell_du in nr_cell_dus:
							nr_cell_du_ldn = nr_cell_du.get('ldn')

							gNBId = None
							if nr_cell_du_ldn is not None:
								p1 = re.compile(REGEX_5G_LDN_GNBDUFUNC)
								m1 = p1.match(nr_cell_du_ldn)
								if m1:
									gNBId = m1.group(4)

							refNRPhysicalCellDU = parseData(nr_cell_du, f'.//refNRPhysicalCellDU/text()', 0, ns)
							du_cellname = parseData(nr_cell_du, f'.//userLabel/text()', 0, ns)
							cellLocalId = parseData(nr_cell_du, f'.//cellLocalId/text()', 0, ns)

							refNRPhysicalCellDU_dic[refNRPhysicalCellDU] = {
								'cellname': du_cellname,
								'ldn': nr_cell_du_ldn,
								'gNBId': gNBId,
								'cellLocalId': cellLocalId
							}
							cell_dic[du_cellname] = {
								'gNBId': gNBId,
								'cellLocalId': cellLocalId
							}

							node_cnt +=1

						# Get NR PhysicalCellDU
						physicalCellDu_collection = node.xpath('.//mo[@moc="NRPhysicalCellDU"]', namespaces=ns)
						for physicalCellDu in physicalCellDu_collection:
							ldn = physicalCellDu.get('ldn')
							if ldn in refNRPhysicalCellDU_dic:
								# refNRCarrier = physicalCellDu.xpath('.//refNRCarrier/text()', namespaces=ns)[0]
								refNRCarrier = parseData(physicalCellDu, f'.//refNRCarrier/text()', 0, ns)
								refNRPhysicalCellDU_dic[ldn]['refNRCarrier'] = refNRCarrier
								if refNRCarrier is not None:
									pattern = re.compile(REGEX_5G_LDN_NRCARRIER)
									match = pattern.match(refNRCarrier)
									if match:
										nrCarrierId = match.group(2)

										refNrCarrier_dic[refNRCarrier] = {
											'cellname': refNRPhysicalCellDU_dic[ldn]['cellname'],
											'ldn': refNRCarrier,
											'gNBId': refNRPhysicalCellDU_dic[ldn]['gNBId'],
											'cellLocalId': refNRPhysicalCellDU_dic[ldn]['cellLocalId'],
											'nrCarrierId': nrCarrierId
										}


						# Get NR CU Cell
						nr_cell_cus = node.xpath('.//mo[@moc="NRCellCU"]', namespaces=ns)
						for nr_cell_cu in nr_cell_cus:
							ldn = nr_cell_cu.get('ldn')
							# cu_cellname = nr_cell_cu.xpath('.//userLabel/text()', namespaces=ns)[0]
							cu_cellname = parseData(nr_cell_cu, f'.//userLabel/text()', 0, ns)
							refCellCU_dic[ldn] = {
								'cellname': cu_cellname,
								'ldn': ldn
							}

						# Get GNB
						for key, value in refNRPhysicalCellDU_dic.items():
							gNBId = value['gNBId']

							gnbCuFunction = neData.xpath(f".//mo[@moc='GNBCUCPFunction'][attributes/gNBId/text()='{gNBId}']/following-sibling::mo[1]", namespaces=ns)
							for gnb in gnbCuFunction:
								# gnbName = gnb.xpath('.//attributes/gNBDUName/text()', namespaces=ns)[0]
								gnbName = parseData(gnb, './/attributes/gNBDUName/text()', 0, ns)
								gnb_dic[gNBId] = {
									'gnb': gnbName,
									'gNBId': gNBId
								}

						# Check each MO under module = nr
						for parameter_group, valuedic in field_mapping_dic.items():

							level_type = cell_level_dic[parameter_group]
							# Except group Sctp need to search at root, special not under module = nr
							mo_group_collection = None
							if parameter_group.upper() == 'Sctp'.upper():
								# Go to module plat
								plats = neData.xpath(u'.//module[@name="plat"]', namespaces=ns)
								for plat in plats:
									# Find managedElement
									managedElement_ = parseData(plat, f'.//mo[@moc="ManagedElement"]', 0, ns, default=None)
									if managedElement_ is not None:
										userLabel = parseData(managedElement_, f'.//userLabel/text()', 0, ns)
										gbscId = parseData(managedElement_, f'.//GBSCID/text()', 0, ns)
										rncId = parseData(managedElement_, f'.//RNCID/text()', 0, ns)
										# find Sctp
										mo_group_collection = plat.xpath(f'.//mo[@moc="{parameter_group}"]', namespaces=ns)
										for mo in mo_group_collection:
											ldn = mo.get('ldn')
											reference_name = userLabel
											mo_name = f"SubNetwork={subNetwork},ManagedElement={managedElement},GBSCID={gbscId},RNCID={rncId}"
											mongo_value_pair_dic = {}
											oracle_value_pair_dic = dict.fromkeys(valuedic, '')
											insertData(
												parameter_group,
												mo_name,
												ldn,
												reference_name,
												level_type,
												frequency_type,
												mo,
												ns,
												mongo_value_pair_dic,
												mongo_result,
												oracle_value_pair_dic,
												oracle_result,
												filename
											)
							else:
								xpath = f'.//mo[@moc="{parameter_group}"]'
								mo_group_collection = node.xpath(xpath, namespaces=ns)

								for mo in mo_group_collection:
									ldn = mo.get('ldn')
									reference_name = None
									gNBId = ''
									cellLocalId = ''
									mo_name = None
									mongo_value_pair_dic = {}
									oracle_value_pair_dic = dict.fromkeys(valuedic, '')
									if level_type == 'CELL Level':
										p_cellcu = re.compile(REGEX_5G_LDN_NRCELLCU)
										p_physicaldu = re.compile(REGEX_5G_LDN_NRPHYSICALCELLDU)
										p_nrcarrier = re.compile(REGEX_5G_LDN_NRCARRIER)
										match_cellcu = p_cellcu.match(ldn)
										match_physicaldu = p_physicaldu.match(ldn)
										match_nrcarrier = p_nrcarrier.match(ldn)
										if match_cellcu:
											cellCu = match_cellcu.group(6)
											key = match_cellcu.group(1)
											if key in refCellCU_dic:
												reference_name = refCellCU_dic[key].get('cellname')
												cellLocalId = cellCu
												if reference_name in cell_dic:
													gNBId = cell_dic[reference_name].get('gNBId')

										elif match_physicaldu:
											physicalCellDu = match_physicaldu.group(1)
											if physicalCellDu in refNRPhysicalCellDU_dic:
												reference_name = refNRPhysicalCellDU_dic[physicalCellDu].get('cellname')
												cellLocalId = refNRPhysicalCellDU_dic[physicalCellDu].get('cellLocalId')
												gNBId = refNRPhysicalCellDU_dic[physicalCellDu].get('gNBId')

										elif match_nrcarrier:
											refNrCarrier = match_nrcarrier.group(1)
											if refNrCarrier in refNrCarrier_dic:
												reference_name = refNrCarrier_dic[refNrCarrier].get('cellname')
												cellLocalId = refNrCarrier_dic[refNrCarrier].get('cellLocalId')
												gNBId = refNrCarrier_dic[refNrCarrier].get('gNBId')

										mo_name = nr_cell_path.format(subNetwork, managedElement, gNBId, cellLocalId)
									else:
										#GNB level
										if parameter_group.upper() == 'EnDCPDCP'.upper():
											p_gnbcucpfunc = re.compile(REGEX_5G_LDN_GNBCUCPFUNC)
											match_gnbdufunc = p_gnbcucpfunc.match(ldn)
											if match_gnbdufunc:
												gnb = match_gnbdufunc.group(4)
												if gnb in gnb_dic:
													reference_name = gnb_dic[gnb].get('gnb')
													gNBId = gnb_dic[gnb].get('gNBId')

										else:
											p_gnbdufunc = re.compile(REGEX_5G_LDN_GNBDUFUNC)
											p_gnbcucpfunc = re.compile(REGEX_5G_LDN_GNBCUCPFUNC)
											match_gnbdufunc = p_gnbdufunc.match(ldn)
											match_gnbcucpfunc = p_gnbcucpfunc.match(ldn)
											if match_gnbdufunc:
												gnb = match_gnbdufunc.group(4)
												if gnb in gnb_dic:
													reference_name = gnb_dic[gnb].get('gnb')
													gNBId = gnb_dic[gnb].get('gNBId')

												mo_name = gnb_path.format(subNetwork, managedElement, gNBId)
											elif match_gnbcucpfunc:
												gnb = match_gnbcucpfunc.group(4)
												if gnb in gnb_dic:
													reference_name = gnb_dic[gnb].get('gnb')
													gNBId = gnb_dic[gnb].get('gNBId')
												mo_name = gnb_path.format(subNetwork, managedElement, gNBId)
									if parameter_group.upper() == 'EnDCPDCP'.upper():
										# Group EnDCPDCP ldn =  GNBCUCPFunction=520-04_550976,EnDCConfigCU=1,EnDCPDCP=1
										mo_name = gnb_path.format(subNetwork, managedElement, gNBId) + f',{ldn}'
									insertData(
										parameter_group,
										mo_name,
										ldn,
										reference_name,
										level_type,
										frequency_type,
										mo,
										ns,
										mongo_value_pair_dic,
										mongo_result,
										oracle_value_pair_dic,
										oracle_result,
										filename
									)
				elif frequency_type == '4G':
					nodes = neData.xpath(u'.//module[@name="lte"]', namespaces=ns)
					if len(nodes) < 1:
						# print(f'LTE Cell not found in subNetwork={subNetwork}, managedElement={managedElement}')
						continue

					for node in nodes:
						cellfdd_ldn_dic = {}
						celltdd_ldn_dic = {}
						nb_dic = {}

						# Get TDD
						cells = node.xpath('.//mo[@moc="CUEUtranCellTDDLTE"]', namespaces=ns)
						for cell in cells:
							ldn = cell.get('ldn')

							nodeBId = None
							nodeBLdn = None
							if ldn is not None:
								p1 = re.compile(REGEX_4G_LDN_ENBCUCPFUNC)
								m1 = p1.match(ldn)
								if m1:
									nodeBId = m1.group(2)
									nodeBLdn = m1.group(1)

							name = parseData(cell, f'.//userLabel/text()', 0, ns)
							cellLocalId = parseData(cell, f'.//cellLocalId/text()', 0, ns)
							data = {
								'cellname': name,
								'nbId': nodeBId,
								'cellLocalId': cellLocalId,
								'nodeBLdn': nodeBLdn
							}
							celltdd_ldn_dic[ldn] = data

							node_cnt +=1
						
						# Get FDD
						cells = node.xpath('.//mo[@moc="CUEUtranCellFDDLTE"]', namespaces=ns)
						for cell in cells:
							ldn = cell.get('ldn')

							nodeBId = None
							nodeBLdn = None
							if ldn is not None:
								p1 = re.compile(REGEX_4G_LDN_ENBCUCPFUNC)
								m1 = p1.match(ldn)
								if m1:
									nodeBId = m1.group(2)
									nodeBLdn = m1.group(1)

							name = parseData(cell, f'.//userLabel/text()', 0, ns)
							cellLocalId = parseData(cell, f'.//cellLocalId/text()', 0, ns)
							data = {
								'cellname': name,
								'nbId': nodeBId,
								'cellLocalId': cellLocalId,
								'nodeBLdn': nodeBLdn
							}
							cellfdd_ldn_dic[ldn] = data

							node_cnt +=1

						# Get NB
						nobeb_collection = node.xpath('.//mo[@moc="ENBCUCPFunction"]', namespaces=ns)
						for nodeb in nobeb_collection:
							ldn = nodeb.get('ldn')
							name = parseData(nodeb, f'.//attributes/enbName/text()', 0, ns)
							nodebId = parseData(nodeb, f'.//attributes/eNBId/text()', 0, ns)
							nb_dic[ldn] = {
								'name': name,
								'id': nodebId
							}

						for cell, value in cellfdd_ldn_dic.items():
							nbLdn = value['nodeBLdn']
							if nbLdn in nb_dic:
								cellfdd_ldn_dic[cell]['nodeBName'] = nb_dic[nbLdn]['name']
								cellfdd_ldn_dic[cell]['nodeBId'] = nb_dic[nbLdn]['id']
						for cell, value in celltdd_ldn_dic.items():
							nbLdn = value['nodeBLdn']
							if nbLdn in nb_dic:
								celltdd_ldn_dic[cell]['nodeBName'] = nb_dic[nbLdn]['name']
								celltdd_ldn_dic[cell]['nodeBId'] = nb_dic[nbLdn]['id']


						# Check each MO under module = nr
						for parameter_group, valuedic in field_mapping_dic.items():

							level_type = cell_level_dic[parameter_group]
							# Except group Sctp need to search at root
							if parameter_group.upper() == 'Sctp'.upper():
								# Go to module plat
								plats = neData.xpath(u'.//module[@name="plat"]', namespaces=ns)
								for plat in plats:
									# Find managedElement
									managedElement_ = parseData(plat, f'.//mo[@moc="ManagedElement"]', 0, ns, default=None)
									if managedElement_ is not None:
										userLabel = parseData(managedElement_, f'.//userLabel/text()', 0, ns)
										gbscId = parseData(managedElement_, f'.//GBSCID/text()', 0, ns)
										rncId = parseData(managedElement_, f'.//RNCID/text()', 0, ns)
										# find Sctp
										mo_group_collection = plat.xpath(f'.//mo[@moc="{parameter_group}"]', namespaces=ns)
										for mo in mo_group_collection:
											ldn = mo.get('ldn')
											reference_name = userLabel
											mo_name = f"SubNetwork={subNetwork},ManagedElement={managedElement},GBSCID={gbscId},RNCID={rncId}"
											mongo_value_pair_dic = {}
											oracle_value_pair_dic = dict.fromkeys(valuedic, '')
											insertData(
												parameter_group,
												mo_name,
												ldn,
												reference_name,
												level_type,
												frequency_type,
												mo,
												ns,
												mongo_value_pair_dic,
												mongo_result,
												oracle_value_pair_dic,
												oracle_result,
												filename
											)
							else:
								mo_group_collection = None
								xpath = f'.//mo[@moc="{parameter_group}"]'
								mo_group_collection = node.xpath(xpath, namespaces=ns)

								for mo in mo_group_collection:
									ldn = mo.get('ldn')
									reference_name = None
									nbId = ''
									cellLocalId = ''
									mo_name = None
									mongo_value_pair_dic = {}
									oracle_value_pair_dic = dict.fromkeys(valuedic, '')
									if level_type == 'CELL Level':
										p_cellfdd = re.compile(REGEX_4G_LDN_CELLFDDLTE)
										p_celltdd = re.compile(REGEX_4G_LDN_CELLTDDLTE)
										match_cellfdd = p_cellfdd.match(ldn)
										match_celltdd = p_celltdd.match(ldn)
										# Check FDD
										if match_cellfdd:
											cellId = match_cellfdd.group(5)
											key = match_cellfdd.group(1)
											if key in cellfdd_ldn_dic:
												reference_name = cellfdd_ldn_dic[key].get('cellname')
												cellLocalId = cellfdd_ldn_dic[key].get('cellLocalId')
												nbId = cellfdd_ldn_dic[reference_name].get('nbId')
											mo_name = eu_cell_path.format(subNetwork, managedElement, nbId, cellLocalId)
										# Check TDD
										elif match_celltdd:
											cellId = match_celltdd.group(5)
											key = match_celltdd.group(1)
											if key in celltdd_ldn_dic:
												reference_name = celltdd_ldn_dic[key].get('cellname')
												cellLocalId = celltdd_ldn_dic[key].get('cellLocalId')
												nbId = celltdd_ldn_dic[reference_name].get('nbId')
											mo_name = tdd_cell_path.format(subNetwork, managedElement, nbId, cellLocalId)
										
									else:
										#NB level
										p_nb = re.compile(REGEX_4G_LDN_ENBCUCPFUNC)
										match_nb = p_nb.match(ldn)
										if match_nb:
											ldn = match_nb.group(1)
											if ldn in nb_dic:
												reference_name = nb_dic[ldn].get('name')
												nbId = nb_dic[ldn].get('id')
											if parameter_group.upper() == 'NrRatMeasCfgLTE'.upper() or parameter_group.upper() == 'QoSServiceClassLTE'.upper() or parameter_group.upper() == 'UeEUtranMeasurementLTE'.upper():
												mo_name = env_mo_path.format(subNetwork, managedElement, nbId) + f',{ldn}'
											else:
												mo_name = env_mo_path.format(subNetwork, managedElement, nbId)
									insertData(
										parameter_group,
										mo_name,
										ldn,
										reference_name,
										level_type,
										frequency_type,
										mo,
										ns,
										mongo_value_pair_dic,
										mongo_result,
										oracle_value_pair_dic,
										oracle_result,
										filename
									)
		except Exception as e:
			log.e(f'---- ERROR: {str(e)}')

	log.i('---- Start pushing to oracle : ', ZTE_VENDOR, frequency_type)
	for result in oracle_result:

		table_name = naming_helper.get_table_name(ZTE_TABLE_PREFIX, frequency_type, result)
		# granite_mongo.push(table_name, mongo_result[result])
		try:
			ran_baseline_oracle.push(oracle_cur, table_name, oracle_result[result])
			oracle_con.commit()
		except Exception as e:
			log.e('#################################### Error occur (001): ', ZTE_VENDOR, frequency_type)
			log.e('Exception Into Table: ' + table_name, ZTE_VENDOR, frequency_type)
			log.e(e, ZTE_VENDOR, frequency_type)
			traceback.print_exc()
			log.e('#################################### Error ', ZTE_VENDOR, frequency_type)

			oracle_con.commit()
			oracle_con, oracle_cur = open_connection()
			continue

	log.i("Done :::: " + filename + " ::::::::", ZTE_VENDOR, frequency_type)
	log.i("<<<< Time : " + str(datetime.datetime.now() - start_parser_time), ZTE_VENDOR, frequency_type)
	close_connection(oracle_con, oracle_cur)

def insertData(parameter_group, mo_name, ldn, reference_name, level_type, frequency_type, node, namespace, mongo_value_pair_dic, mongo_result, oracle_value_pair_dic, oracle_result, filename):
	# Get all attribute
	attributes = node.xpath(f'.//attributes/*', namespaces=namespace)
	for attribute in attributes:

		tag = attribute.tag
		value = attribute.text

		mongo_value_pair_dic[str(tag).upper()] = value

		if str(tag).upper() in oracle_value_pair_dic:
			oracle_value_pair_dic[str(tag).upper()] = value

		if parameter_group.upper() == 'EnDCPDCP'.upper() and str(tag).upper() == 'qci'.upper():
			# Group EnDCPDCP ldn =  GNBCUCPFunction=520-04_550976,EnDCConfigCU=1,EnDCPDCP=1
			mo_name = mo_name + f',qci={value}'

	if mo_name is not None and reference_name is not None:
		if KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group) not in COUNT_DATA:
			COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = 0

		COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] = COUNT_DATA[KEY_TABLE.format(ZTE_TABLE_PREFIX, frequency_type, parameter_group)] + 1

		oracle_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = reference_name
		oracle_value_pair_dic['FILENAME'] = filename
		oracle_value_pair_dic['LV'] = level_type
		oracle_value_pair_dic['MO'] = mo_name

		mongo_value_pair_dic[REFERENCE_FIELD_COLUMN_NAME] = reference_name
		mongo_value_pair_dic['FILENAME'] = filename
		mongo_value_pair_dic['LV'] = level_type
		mongo_value_pair_dic['MO'] = mo_name

		if parameter_group in mongo_result:
			mongo_result[parameter_group].append(mongo_value_pair_dic)
		else:
			mongo_result[parameter_group] = []
			mongo_result[parameter_group].append(mongo_value_pair_dic)

		if parameter_group in oracle_result:
			oracle_result[parameter_group].append(oracle_value_pair_dic)
		else:
			oracle_result[parameter_group] = []
			oracle_result[parameter_group].append(oracle_value_pair_dic)
	else:
		log.e(f'---- ERROR: No MO name for {parameter_group} ldn = {ldn}')
