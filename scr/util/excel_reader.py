from pandas import ExcelFile


def read_excel_mapping(file_mapping_path_name, sheet_index):
    xls = ExcelFile(file_mapping_path_name)
    df = xls.parse(xls.sheet_names[sheet_index])
    return df