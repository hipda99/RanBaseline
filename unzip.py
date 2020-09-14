import glob
import zipfile


path = "/Users/adisit/Downloads/ERC/CNA/"


# if raw_file.Path != '':
#
# print(df)

for filename in glob.glob(path + '*.zip'):
    archive = zipfile.ZipFile(filename, 'r')

    result = archive.extractall(path)

    archive.close()