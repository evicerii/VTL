'''
    display information about person for contract
'''
import sys
import os
import datetime
import pandas as pd
from flask import Flask, render_template, request
from flaskwebgui import FlaskUI
from utils.table import TableInternalInteractions
from utils.table import TableExternalInteractions
from utils.table import TableCalculationInteraction
from utils.transform_bank import update_bank_file
from utils.df_excepions import LostDoc, DuplicateData

sys.stdout.reconfigure(encoding='utf-8')

app = Flask(__name__)

@app.route('/')
@app.route('/index')
def index():
    '''
    home page
    '''
    return render_template('index.html')

@app.route('/member_data', methods = ['GET', 'POST'])
def member_data():
    '''
    out info for call contrtact table format
    '''
    if request.method == 'POST':
        try:
            #input data
            contract = request.form.get('contract_num')
            name = request.form.get('name_person')
            address = request.form.get('address')
            house_number = request.form.get('house_num')
            apart_num = request.form.get('apart_num')
            phone = request.form.get('phone_num')
            inn = request.form.get('inn')
            comment = request.form.get('comment')
            connect_date = request.form.get('connect_date')
            disconnect_date = request.form.get('disconnect_date')
            add_pay = request.form.get('add_pay')
            req_list = (contract, name,
                         address, house_number,
                           apart_num, phone,
                             inn, comment, connect_date,
                               disconnect_date)
            work_table = TableInternalInteractions(table_name='vtl_address')
            res_search = work_table.search_by_data(req_list)[1]
            res = pd.DataFrame(res_search).fillna('')
            if add_pay == 'add_pay':
                pay_table = TableCalculationInteraction(table_name='vtl_pay')
                res_credit_table_out = pd.DataFrame(columns=['contract', 'credit'])
                contracts = res["contract"]
                for contract in contracts:
                    credit = pay_table.res_sum(contract, need_table="vtl_address")
                    res_credit_table_out.loc[-1] = [contract, credit]
                    res_credit_table_out.index = res_credit_table_out.index + 1
                    res_credit_table_out = res_credit_table_out.sort_index()
                res = pd.merge(res, res_credit_table_out, on='contract', how='left')
            res.index = res.index + 1
            date_now = datetime.datetime.now().strftime('%d-%m-%Y_%H-%M-%S')
            res.to_excel(f'Temp/{date_now}.xlsx', index=False)
            return f'''
{res.to_html()}
<br>
<form action="">
    <input type="submit" value="Вернуться" />
</form>'''
        except:
            return f'''{render_template('member_data.html')} <p>Ошибка обработки</p>'''
    else:
        return render_template('member_data.html')

@app.route('/bank_load', methods = ['GET', 'POST'])
def bank_load():
    '''
    load new bank data from excel to db
    '''
    if request.method == 'POST':
        try:
            files = os.listdir("bank_file")
            num_len = len(files)
            if num_len == 0:
                raise LostDoc()
            else:
                for file_index in range(0, num_len):
                    update_bank_file(f'bank_file/{files[file_index]}')
                    df = TableExternalInteractions('vtl_pay', 'data.xlsx', 'Sheet1')
                    raise_value = df.update_user_pay_table()
                    os.remove('data.xlsx')
                    if not raise_value.empty:
                        raise DuplicateData()
                    os.replace(f'bank_file/{files[file_index]}', str(f'excel_logs/{files[file_index]}'))
            return f'''{render_template('index.html')}
<p>Успешная загрузка</p>'''
        except DuplicateData:
            return f'''<p>Дублирование данных</p> {raise_value.to_html()}
<br>
<form action="">
    <input type="submit" value="Вернуться" />
</form>'''
        except LostDoc:
            return f'''{render_template('bank_load.html')} <p>Отсутствует документ</p>'''
    else:
        return render_template('bank_load.html')
@app.route('/users', methods = ['GET', 'POST'])
def users():
    '''
    load new users data from excel to db
    '''
    if request.method == 'POST':
        try:
            df = TableExternalInteractions('vtl_address', 'pay_list.xlsx', 'vtl')
            df.update_address_table()
            return f'''{render_template('index.html')}
<p>Успешная загрузка</p>'''
        except TypeError:
            return f'''{render_template('users.html')} <p>Ошибка обработки</p>'''
    else:
        return render_template('users.html')
if __name__ == '__main__':
    ui = FlaskUI(app=app, server='flask', width=900, height=500, port=5000)
    ui.run()
