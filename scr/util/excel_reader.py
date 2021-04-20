from pandas import ExcelFile


def read_excel_mapping(file_mapping_path_name, sheet_index):
    xls = ExcelFile(file_mapping_path_name)
    ncols = xls.book.sheet_by_index(sheet_index).ncols
    df = xls.parse(xls.sheet_names[sheet_index], converters={i : str for i in range(ncols)})
    return df