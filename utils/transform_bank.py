'''
update bank file function
'''
import pandas as pd

def update_bank_file(bank_list_name):
    '''prepare for uploading'''
    df = pd.read_excel(f'{bank_list_name}', header=None)

    result = pd.DataFrame()
    result['contract'] = df.iloc[8:-1,0]
    result['date'] = df.iloc[8:-1,4]
    result['pay'] = df.iloc[8:-1,3]

    result.to_excel('data.xlsx', index=False)
