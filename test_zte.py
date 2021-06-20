#!/usr/bin/python3
import datetime

from re import L
import traceback
import re
import sys
import logging

import cx_Oracle
from lxml import ElementInclude
from lxml import etree
from lxml.etree import XPathEvalError

raw_file = sys.argv[1]
ns = {'en': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#eutranNrm', 'xn': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm', 'un': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#utranNrm', 'gn': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#gsmNrm', 'zs': 'http://ZTESpecificAttributes#ZTESpecificAttributes', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}

try:

    tree = ElementInclude.default_loader(raw_file, 'xml')

    subnet = tree.xpath('.//xn:SubNetwork', namespaces=ns)
    subid = subnet[1].xpath('@id')[0]

    start_parser_time = datetime.datetime.now()

    for e in subnet[1]:
        submo = etree.fromstring(etree.tostring(e))

        manage_group_collection = submo.xpath('.//xn:ManagedElement', namespaces=ns)

        for manage_group in manage_group_collection:

            manageid = manage_group.xpath('@id')[0]
            enb_group_collection = manage_group.xpath('.//en:ENBFunction',
                                                    namespaces=ns)

            for enb_moo in enb_group_collection:

                enbid = enb_moo.xpath('@id')[0]

                base_xml = etree.fromstring(etree.tostring(enb_moo))
                valuess = None
                mo_xml = base_xml

                for enb_mo in mo_xml:
                    # xpath = './/zs:vsData{0}'.format(parameter_group)
                    # if parameter_group == 'ECellEquipmentFunction':
                    #     test = f'vsData{parameter_group}'
                    #     print(base_xml.nsmap)
                    #     for node in enb_mo.xpath(f".//*/*/*[local-name() = '{test}']"):
                    #         print(node)
                    # ns = {'en': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#eutranNrm', 'xn': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm', 'un': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#utranNrm', 'gn': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.765#gsmNrm', 'zs': 'http://ZTESpecificAttributes#ZTESpecificAttributes', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}
                    # # mo_group_collection = enb_mo.xpath(xpath,
                    # # 								namespaces={ZTE_XML_DESCRIPTOR: ZTE_XML_DESCRIPTOR_REF,
                    # # 											ZTE_XML_DESCRIPTOR_REF_EN: ZTE_XML_DESCRIPTOR_EN })
                    # mo_group_collection = enb_mo.xpath(xpath,namespaces=ns)
                    xpath = f".//*[local-name() = 'vsDataECellEquipmentFunction']"
                    nodeB = ''
                    found = False
                    mo_group_collection = enb_mo.xpath(xpath)
                    for v1 in mo_xml:
                        if found:
                            break
                        for atts in v1:
                            tag = atts.tag.replace(
                                '{http://www.3gpp.org/ftp/specs/archive/32_series/32.765#eutranNrm}', '')
                            if tag == 'userLabel':
                                nodeB = atts.text
                                found = True
                                break

                    for enb_moo_ in mo_group_collection:

                        for attribute in enb_moo_:
                            tag = attribute.tag.replace('{http://ZTESpecificAttributes#ZTESpecificAttributes}', '')
                            value = attribute.text
                            if 'description' == tag:
                                print(f'{nodeB} ==> {str(tag)} = {str(value)}')



except Exception as e:
    print(e)
