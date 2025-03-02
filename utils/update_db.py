'''
update db
'''
import sys
import datetime
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
final_list= pd.read_excel('db_final.xlsx', sheet_name='to_db',)
df = pd.DataFrame(final_list)
for i in range(0, df.shape[0]):
    a = datetime.datetime.strptime(final_list.iloc[i,1], '%d.%m.%Y').date()
    df.iat[i,1] = a
df.to_excel('db_final1.xlsx', sheet_name='to_db', index=False)
