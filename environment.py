# dev
# prod
# north
import socket

host_name = socket.gethostname()
host_ip = socket.gethostbyname(host_name)
print("Hostname :  ", host_name)
print("IP : ", host_ip)

if host_name == "adisits-MBP" or host_name == 'adisits-MacBook-Pro.local':
    environment = 'north'
elif host_name == 'HELLKITCHEN':
    environment = 'hell'
elif host_name == "MT8NGOSS-INVP12":
    environment = 'prod'
else:
    environment = 'dev'

from environment import environment

COUNT_FILE = 0

MAPPING_NAME = 'file_mapping.yaml'

HUAWEI_TABLE_PREFIX = 'HW'
ZTE_TABLE_PREFIX = 'ZTE'
ERICSSON_TABLE_PREFIX = 'ERC'

BASELINE = 'BL'
RED_ZONE = 'RZ'
BASELINE_TABLE_PREFIX = BASELINE + '_{0}'
BASELINE_TABLE_PREFIX_FEATURE = BASELINE + '_FEATURE_{0}'
RED_ZONE_BASELINE_TABLE_PREFIX = RED_ZONE + '_{0}'

HUAWEI_VENDOR = 'Huawei'
ZTE_VENDOR = 'ZTE'
ERICSSON_VENDOR = 'Ericsson'

ERICSSON_SW_VENDOR = 'Ericsson_SW'
ERICSSON_FEATURE_VENDOR = 'Ericsson_Feature'

ERICSSON_ANCHOR_VENDOR = 'Ericsson_Anchor'
HUAWEI_ANCHOR_VENDOR = 'Huawei_Anchor'
ZTE_ANCHOR_VENDOR = 'ZTE_Anchor'

ITBBU_FILE = 'ITBBU'

CREATED_TABLE = {}

MAX_RUNNING_PROCESS = 4

COUNT_DATA = {}

if environment == 'dev':
    # Configuration of Oracle
    ORACLE_HOST = '10.50.64.209'
    ORACLE_PORT = '1521'
    ORACLE_SID = 'xcom'
    ORACLE_USERNAME = 'ranbase'
    ORACLE_PASSWORD = 'ng055'

    # Configuration of MongoDB
    MONGO_HOST = 'mongodb://10.50.64.209:27017/'
    MONGO_NAME = 'ranbase'

    # config: point to config directory of zte
    CONFIGURATION_PATH = '/home/app/ngoss/RanBaselineApp/BaselineFile/BaselineUpload/'

    # config: (use only you want to generate json file) directory to store result of json file
    JSON_RESULT_PATH = '/app/ngoss/zte_result/{0}/{1}/'

    # config: zte mapping file
    MAPPING_FILE_PATH = "/home/app/ngoss/RanBaselineApp/source/config/dev/" + MAPPING_NAME

    # config: data source file
    RAW_FILE_PATH_COLLECTION = "/home/app/ngoss/RanBaselineApp/source/config/dev/RawFilePathCollection.xlsx"

    # file separator [window = '\\'], [linux = '/']
    PATH_SEPARATOR = '/'

    LOG_PATH = '/home/app/ngoss/RanBaselineApp/source/logs/'

if environment == 'prod':
    # Configuration of Oracle
    ORACLE_HOST = '10.50.64.207'
    ORACLE_PORT = '1521'
    ORACLE_SID = 'xcommt8'
    ORACLE_USERNAME = 'ranbase'
    ORACLE_PASSWORD = 'ng055'

    # Configuration of MongoDB
    MONGO_HOST = 'mongodb://10.50.64.207:27017/'
    MONGO_NAME = 'ranbase'

    # config: point to config directory of zte
    CONFIGURATION_PATH = '/home/app/ngoss/RanBaselineApp/BaselineFile/BaselineUpload/'

    # config: (use only you want to generate json file) directory to store result of json file
    JSON_RESULT_PATH = '/app/ngoss/zte_result/{0}/{1}/'

    # config: zte mapping file
    MAPPING_FILE_PATH = "/home/app/ngoss/RanBaselineApp/source/config/prod/" + MAPPING_NAME

    # config: data source file
    RAW_FILE_PATH_COLLECTION = "/home/app/ngoss/RanBaselineApp/source/config/prod/RawFilePathCollection.xlsx"

    # file separator [window = '\\'], [linux = '/']
    PATH_SEPARATOR = '/'

    LOG_PATH = '/home/app/ngoss/RanBaselineApp/source/logs/'

if environment == 'north':
    # Configuration of Oracle
    ORACLE_HOST = '10.50.64.209'
    ORACLE_PORT = '1521'
    ORACLE_SID = 'xcom'
    ORACLE_USERNAME = 'ranbase'
    ORACLE_PASSWORD = 'ng055'

    # Configuration of MongoDB
    MONGO_HOST = 'mongodb://10.50.64.209:27017/'
    MONGO_NAME = 'ranbase'

    # config: point to config directory of zte
    CONFIGURATION_PATH = '/Users/adisit/SourceCode/RanBaselineApp/source/config/north/'

    # config: (use only you want to generate json file) directory to store result of json file
    JSON_RESULT_PATH = '/app/ngoss/zte_result/{0}/{1}/'

    # config: zte mapping file
    MAPPING_FILE_PATH = CONFIGURATION_PATH + MAPPING_NAME

    # config: data source file
    RAW_FILE_PATH_COLLECTION = CONFIGURATION_PATH + 'RawFilePathCollection.xlsx'

    # file separator [window = '\\'], [linux = '/']
    PATH_SEPARATOR = '/'

    LOG_PATH = './logs/'

if environment == 'hell':
    # Configuration of Oracle
    ORACLE_HOST = '10.50.64.209'
    ORACLE_PORT = '1521'
    ORACLE_SID = 'xcom'
    ORACLE_USERNAME = 'ranbase'
    ORACLE_PASSWORD = 'ng055'

    # Configuration of MongoDB
    MONGO_HOST = 'mongodb://10.50.64.209:27017/'
    MONGO_NAME = 'ranbase'

    # config: point to config directory of zte
    CONFIGURATION_PATH = '/c/Users/next_/git/true/RanBaselineApp/source/config/dev'

    # config: (use only you want to generate json file) directory to store result of json file
    JSON_RESULT_PATH = '/app/ngoss/zte_result/{0}/{1}/'

    # config: zte mapping file
    MAPPING_FILE_PATH = CONFIGURATION_PATH + MAPPING_NAME

    # config: data source file
    RAW_FILE_PATH_COLLECTION = CONFIGURATION_PATH + 'RawFilePathCollection.xlsx'

    # file separator [window = '\\'], [linux = '/']
    PATH_SEPARATOR = '/'

    LOG_PATH = './logs/'