'''
table manipulation
'''
import datetime
import  pandas as pd
import sqlalchemy as sa
from dateutil import relativedelta
from sqlalchemy.orm import Session
from utils.config import settings

# create engine
sync_engine = sa.create_engine(
    url=settings.database_url_mysql,
    echo=False
)


class SuperTableClass():
    '''
    base class for table manipulation
    '''
    def __init__(self, table_name='none'):
        self.table_name = table_name
        self.metadata_obj = sa.MetaData()
        self.metadata_obj.reflect(bind=sync_engine)
class TableExternalInteractions(SuperTableClass):
    '''
    create, update, upload
    '''
    def __init__(self, table_name, excel_link, excel_sheet_link):
        super().__init__(table_name)
        self.excel_link = excel_link
        self.excel_sheet_link = excel_sheet_link
    def create_table(self):
        '''
        create table 
        '''
        excel_sheet = pd.read_excel(self.excel_link, sheet_name=self.excel_sheet_link)
        with sync_engine.connect() as conn:
            excel_sheet.to_sql(self.table_name, con=conn, if_exists='replace', index=False)
        return TableExternalInteractions(self.table_name, self.excel_link, self.excel_sheet_link)
    def update_address_table(self):
        '''
        update data
        '''
        vtl_table = self.metadata_obj.tables[self.table_name]
        excel_sheet = pd.read_excel(self.excel_link, sheet_name=self.excel_sheet_link)
        excel_sheet.columns = ['contract', 'name',
                    'address', 'house_number',
                    'apart_num', 'phone',
                    'inn', 'comment',
                    'connect_date', 'disconnect_date']
        table_contracts = excel_sheet['contract'].tolist()
        with sync_engine.connect() as conn:
            db_table = pd.read_sql_table(self.table_name, conn)
            print(db_table)
            contracts = db_table['contract'].tolist()
            need_new_data = list(set(contracts) & set(table_contracts))
            print(need_new_data)
            for contract in need_new_data:
                stmt = sa.delete(vtl_table).where(vtl_table.c.contract == contract)
                conn.execute(stmt)
                conn.commit()
            excel_sheet.to_sql(self.table_name, con=conn, if_exists='append', index=False)
            conn.commit()
            print(1)
        header = excel_sheet.columns.tolist()
        clean_df = pd.DataFrame(columns=header)
        clean_df.to_excel(self.excel_link, sheet_name=self.excel_sheet_link, index=False)
        return TableExternalInteractions(self.table_name, self.excel_link, self.excel_sheet_link)
    def update_user_pay_table(self):
        '''
        update user pay table
        '''
        vtl_table = self.metadata_obj.tables[self.table_name]
        excel_sheet = pd.read_excel(self.excel_link, sheet_name=self.excel_sheet_link)
        excel_sheet['date'] = pd.to_datetime(excel_sheet['date']).dt.floor('s')
        col_name = excel_sheet.columns.tolist()
        new_value_data = pd.DataFrame(columns=col_name)
        raise_value = pd.DataFrame(columns=col_name)
        with sync_engine.connect() as conn:
            for i in range(0, excel_sheet.shape[0]):
                row = excel_sheet.iloc[i,:]
                stmt = sa.select(vtl_table).where(vtl_table.c.contract ==  row.iloc[0],
                                                   vtl_table.c.date == row.iloc[1])
                res_select = conn.execute(stmt).all()
                if res_select == []:
                    new_value_data = pd.concat([new_value_data, excel_sheet.iloc[i,:].to_frame().T])
                else:
                    raise_value = pd.concat([raise_value, excel_sheet.iloc[i,:].to_frame().T])
            if raise_value.empty:
                new_value_data.to_sql(self.table_name, con=conn, if_exists='append', index=False)
                conn.commit()
            return raise_value
class TableInternalInteractions(SuperTableClass):
    '''
    search and calculate
    '''
    def __init__(self, table_name,
                 payment_amount_list = None):
        super().__init__(table_name)
        self.metadata_obj = sa.MetaData()
        self.metadata_obj.reflect(bind=sync_engine)
        self.payment_amount_list = payment_amount_list
    def search_by_contract(self, target, contract):
        '''
        search pay by contract number and return obj with pay turple
        '''
        vtl_table = self.metadata_obj.tables[self.table_name]
        session = Session(sync_engine)
        stmt = sa.select(sa.text(target)).select_from(vtl_table).where(
            sa.text(f'contract = {contract}'))
        payment_amount_list = session.execute(stmt).scalars().all()
        session.close()
        return TableInternalInteractions(self.table_name,
                payment_amount_list = payment_amount_list), payment_amount_list
    def search_by_data(self, data_strict, data_like):
        '''
        search and return obj with pay turple
        '''
        vtl_table = self.metadata_obj.tables[self.table_name]
        session = Session(sync_engine)
        final_res = session.query(vtl_table)
        req_contract = 'contract'
        req_list = ('name',
                     'address', 'house_number',
                       'apart_num', 'phone',
                         'inn', 'comment',
                           'connect_date', 'disconnect_date')
        if data_strict != '':
            req_res = (sa.select(vtl_table).where(
                    getattr(vtl_table.c, req_contract) == data_strict))
            final_res = final_res.intersect(req_res)
        for count, req in enumerate(data_like):
            if req != '':
                req_res = (sa.select(vtl_table).where(
                    getattr(vtl_table.c, req_list[count]).like(f'%{req}%')
                ))
                final_res = final_res.intersect(req_res)
        final_results = final_res.all()
        session.close()
        return TableInternalInteractions(self.table_name), final_results
class TableCalculationInteraction(TableInternalInteractions):
    '''
    calculate price for active month
    '''
    def __init__(self, table_name):
        super().__init__(table_name)
        self.break_date=datetime.datetime.strptime('31-12-22',
                                            '%d-%m-%y').date()
    def active_month_200(self, contract, need_table):
        '''
        calculate active month with price 200
        '''
        connect_date=TableInternalInteractions(
            table_name=need_table).search_by_contract('connect_date',
                                                contract)[1][0].date()
        #after date price up to 300
        if connect_date>self.break_date:
            return [0,0] #heven't 200 price month
        disconnect_date=TableInternalInteractions(
            table_name=need_table).search_by_contract('disconnect_date',
                                                   contract)[1][0]
        if disconnect_date is not None:
            disconnect_date = disconnect_date.date()
        else:
            disconnect_date = datetime.datetime.strptime('01-01-23',
                                                        '%d-%m-%y').date()
        if disconnect_date>self.break_date:
            disconnect_date = datetime.datetime.strptime('01-01-23',
                                                        '%d-%m-%y').date()
        #disabled calculation
        disabled_date=TableInternalInteractions(
            table_name=need_table).search_by_contract('disabled',
                                                   contract)[1][0]
        if disabled_date is not None:
            disabled_date = disabled_date.date()
        else:
            regular = self.active_month(connect_date,disconnect_date)
            disabled = 0
            return regular, disabled
        if disabled_date < disconnect_date:
            regular = self.active_month(connect_date, disabled_date)
            disabled = self.active_month(disconnect_date, disabled_date)
        else:
            regular = self.active_month(connect_date,disconnect_date)
            disabled = 0
        return regular, disabled
    def active_month_300(self, contract, need_table):
        '''
        calculate active month with price 300
        '''
        connect_date=TableInternalInteractions(
            table_name=need_table).search_by_contract('connect_date',
                                                contract)[1][0].date()
        if connect_date < self.break_date:               #after date price up to 300
            connect_date = datetime.datetime.strptime('01-01-23',
                                                      '%d-%m-%y').date()
        disconnect_date=TableInternalInteractions(
            table_name=need_table).search_by_contract('disconnect_date',
                                                   contract)[1][0]
        if disconnect_date is not None:
            disconnect_date = disconnect_date.date()
        else:
            disconnect_date = datetime.datetime.now().date()
        if disconnect_date < self.break_date:
            return [0,0]
        #disabled calculation
        disabled_date=TableInternalInteractions(
            table_name=need_table).search_by_contract('disabled',
                                                   contract)[1][0]
        if disabled_date is not None:
            disabled_date = disabled_date.date()
        else:
            regular = self.active_month(connect_date,disconnect_date)
            disabled = 0
            return regular, disabled
        if disabled_date < connect_date:
            disabled_date = connect_date
        regular = self.active_month(connect_date, disabled_date)
        disabled = self.active_month(disconnect_date, disabled_date)
        return regular, disabled
    def price_per_service(self, contract, need_table):
        '''
        Calculates the price for all time service
        '''
        am200=self.active_month_200(contract, need_table)
        am300=self.active_month_300(contract, need_table)
        price_200=am200[0]*200+am200[1]*100
        price_300=am300[0]*300+am300[1]*150
        sum_price=price_200+price_300
        return sum_price
    def paid_sum(self, contract):
        '''
        calculates the total amount paid by the contract
        '''
        pay_list=super().search_by_contract('pay',
                                                contract)[1]
        total_pay = sum(filter(None, pay_list))
        return total_pay
    def res_sum(self, contract, need_table):
        '''
        calculates the remaining amount to pay by the contract
        '''
        paid_sum=self.paid_sum(contract)
        service_sum=self.price_per_service(contract, need_table)
        res_sum=service_sum-paid_sum
        return f"{res_sum:.2f}"
    @staticmethod
    def active_month(connect_date, disconnect_date):
        '''
        calculates the active month
        '''
        active_time_m=relativedelta.relativedelta(connect_date,
                                                  disconnect_date).months
        active_time_y=relativedelta.relativedelta(connect_date,
                                                  disconnect_date).years
        active_time=abs(active_time_m+active_time_y*12)
        return active_time
