'''
Update list of byers module
'''
import sys
import datetime
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

START_DATE_POS = 1
XLSX_LIST_NAME = 'pay_list.xlsx'
XLSX_SHEET_NAME = 'pay_list'

columns_names=['contract', 'date', 'pay']

final_list= pd.DataFrame()
temp_table_for_concat = pd.DataFrame(columns=columns_names)
pay_vtl_xlsm = pd.read_excel(XLSX_LIST_NAME, sheet_name=XLSX_SHEET_NAME)

list_contract = pay_vtl_xlsm['contract']
time_string = datetime.datetime.now().month+datetime.datetime.now().year*12-2018*12-1
columns_dates_pay = pay_vtl_xlsm.iloc[:,START_DATE_POS:START_DATE_POS+84]
def gen_row(y, work_table = temp_table_for_concat):
    '''date is pay column name, contract/pay => payment rows'''
    for x in range(0, list_contract.shape[0]):
        new_row = pd.DataFrame({"contract": [list_contract.iloc[x]],
                    "date" : [pay_vtl_xlsm.columns[START_DATE_POS+y].strftime('%d.%m.%Y')],
                    "pay": [columns_dates_pay.iloc[x, y]]})
        work_table = pd.concat([work_table, new_row], ignore_index=True)
    yield work_table

for axis_y in range(0, columns_dates_pay.shape[1]):
    gen = gen_row(axis_y)
    for z in gen:
        final_list = pd.concat([final_list, z], ignore_index=True)
final_list.to_excel('db_final.xlsx', sheet_name='to_db', index=False)
