# Загрузка библиотек
import sys
import psycopg2
import pandas as pd
from datetime import datetime as dt
from py_scripts import upload_files
from py_scripts import transactions
from py_scripts import terminals
from py_scripts import blacklist
from py_scripts import cards
from py_scripts import accounts
from py_scripts import clients
from py_scripts import fraud

sys.path.append("py_scripts/")


# Создание подключение к DWH
def connect_to_dwh():
    conn = psycopg2.connect(database="",
                            host="",
                            user="",
                            password="",
                            port=""
                            )
    conn.autocommit = False
    cursor = conn.cursor()
    return conn, cursor

# Фиксация транзакции


def close_to_dwh(conn, cursor):
    conn.commit()
    cursor.close()
    conn.close()

# Создания всех необходимых объектов


def creat_tables(cursor):
    with open('main.ddl', 'r', encoding="utf-8") as f:
        cursor.execute(f.read())
        f.close()


# Удаление временных таблиц
def del_tmp_tables_main(cursor):
    sql_query = '''
        select table_name from information_schema.tables
        where table_schema = 'deaian' and table_name like '%tmp';
    '''
    cursor.execute(sql_query)
    records = cursor.fetchall()

    names = [x[0] for x in cursor.description]
    df = pd.DataFrame(records, columns=names)

    for table in df['table_name']:
        cursor.execute(f'drop table if exists deaian.{table}')


def to_date(date):
    return pd.to_datetime(dt.strptime(date, '%d%m%Y').strftime('%Y-%m-%d'))


def max_date(cursor, table_name):
    try:
        cursor.execute(
            'select table_name, max_update_dt from deaian.ptrc_meta_loads')
        records = cursor.fetchall()
        names = [x[0] for x in cursor.description]
        df = pd.DataFrame(records, columns=names)
        return pd.to_datetime(df['max_update_dt'][df['table_name'] == table_name].values[0])
    except:
        return pd.to_datetime('1900-01-01', format='%Y-%m-%d')


path_load = '/home/deaian/ptrc/project'
conn, cursor = connect_to_dwh()
creat_tables(cursor)

files = upload_files.current_date(path_load)

if files != []:
    for i in upload_files.current_date(path_load):
        transactions.downland_transactions(cursor, path_load, i)\
            if max_date(cursor, 'ptrc_stg_transactions') < to_date(i) else print(f'Дата файла transactions_{i}.txt меньше даты последний загрузки')

        terminals.downland_terminals(cursor, path_load, i)\
            if max_date(cursor, 'ptrc_stg_terminals') < to_date(i) else print(f'Дата файла terminals_{i}.xlsx меньше даты последний загрузки')

        blacklist.downland_passport_blacklist(cursor, path_load, i)\
            if max_date(cursor, 'ptrc_stg_blacklist') < to_date(i) else print(f'Дата файла passport_blacklist_{i}.xlsx меньше даты последний загрузки')

        cards.downland_cards(cursor, i)\
            if max_date(cursor, 'ptrc_stg_cards') < to_date(i) else print(f'Дата данных info.cards меньше даты последний загрузки')

        accounts.downland_accounts(cursor, i)\
            if max_date(cursor, 'ptrc_stg_accounts') < to_date(i) else print(f'Дата данных info.accounts меньше даты последний загрузки')

        clients.downland_clients(cursor, i)\
            if max_date(cursor, 'ptrc_stg_clients') < to_date(i) else print(f'Дата данных info.clients меньше даты последний загрузки')

        fraud.creat_fraud(cursor, i)\
            if max_date(cursor, 'ptrc_rep_fraud') < to_date(i) else print(f'Дата данных для отчета меньше даты формирования отчета')

        conn.commit()

else:
    print('Отсутсвуют файлы для загрузки')

del_tmp_tables_main(cursor)

close_to_dwh(conn, cursor)
