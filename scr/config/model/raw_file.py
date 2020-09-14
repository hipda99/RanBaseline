import math

NETWORK = 'Network'
VENDOR = 'Vendor'
TECHNOLOGY = 'Technology'
FREQUENCY = 'Frequency'
REGION = 'Region'
IP = 'IP'
PATH = 'Path'
FILE_FORMAT = 'File Format'
VERSION = 'Version'


class RawFile:
    def __init__(self, df_row):
        self.Network = df_row[NETWORK]
        self.Vendor = df_row[VENDOR]
        self.Technology = df_row[TECHNOLOGY]
        self.Frequency = df_row[FREQUENCY]
        self.Region = df_row[REGION]
        self.IP = df_row[IP]
        self.Path = df_row[PATH]
        self.FileFormat = df_row[FILE_FORMAT]
        self.Version = df_row[VERSION]
        self.FileMappingPath = ''
        self.FileMappingFeaturePath = ''
        self.Type = ''
        self.RawFileList = []
        self.FrequencyType = ''

        if str(self.Technology) == 'GSM':
            self.FrequencyType = '2G'
        elif str(self.Technology) == 'UMTS':
            self.FrequencyType = '3G'
        elif str(self.Technology) == 'LTE':
            self.FrequencyType = '4G'
        else:
            self.FrequencyType = '-'

        self.DisplayName = '{0}_{1}_{2}'.format(self.Vendor, self.Frequency, self.Type)

        if 'R3' in self.FileFormat:
            self.Type = 'r3'
        elif 'R4' in self.FileFormat:
            self.Type = 'r4'
        else:
            self.Type = 'default'

        self.FileFormat = self.FileFormat.replace('xx','.*')

        if str(self.Region) == 'nan':
            self.Region = 'No Region'
