from urllib import parse
import sys
import os.path
import datetime

directory = os.path.dirname(os.path.abspath("__file__"))
sys.path.insert(0, directory)
from data_engineering.database import db_functions as db_func
from data_engineering.eod_data import yahoo_functions as yf_func


connection_string = 'Driver={ODBC Driver 17 for SQL Server};\
                    Server=taryllsql.database.windows.net,1433;\
                    Database=iHub;\
                    Uid=taryll2001;\
                    Pwd=Twidle123@;\
                    Encrypt=yes;\
                    TrustServerCertificate=no;\
                    Connection Timeout=30;'
                    
connection_params = parse.quote_plus(connection_string)
engine = db_func.create_engine("mssql+pyodbc:///?odbc_connect=%s" % connection_params)
connection = engine.connect()
session = db_func.Session(engine)


df_securities = db_func.read_security_master(orm_session=session, orm_engine=engine)
df_eod = yf_func.fetch_latest_data(df_securities['Ticker'].tolist(), df_securities['SecID'].tolist())

db_func.write_dataframe_to_marketdata(df_eod,session,engine)