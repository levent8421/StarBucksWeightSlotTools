from openpyxl import load_workbook

__WORKBOOK_FILE_NAME = 'store.xlsx'
__IP_SHEET_NAME = 'IP'


def _get_cell_value(row, index):
    cell = row[index]
    value = cell.value
    if hasattr(value, 'strip'):
        return value.strip()
    return value


class Store:
    def __init__(self, row=None):
        if row is not None:
            self.id = _get_cell_value(row, 0)
            self.name = _get_cell_value(row, 1)
            self.server_ip = _get_cell_value(row, 5)
        else:
            self.id = None
            self.name = None
            self.server_ip = None

    def __str__(self):
        return '%s[%s]:%s' % (self.name, self.id, self.server_ip)


def load_stores():
    workbook = load_workbook(__WORKBOOK_FILE_NAME)
    sheet = workbook[__IP_SHEET_NAME]
    rows = []
    for row in sheet.rows:
        rows.append(row)
    store_list = [Store(row=row) for row in rows[2:]]
    workbook.close()
    return store_list
