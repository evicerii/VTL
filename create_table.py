from utils.table import TableExternalInteractions
if __name__ == "__main__":
    df1 = TableExternalInteractions('vtl_address', 'pay_list.xlsx', 'vtl')
    df2 = TableExternalInteractions('vtl_pay', 'db_final.xlsx', 'to_db')

    df1.create_table()
    df2.create_table()
