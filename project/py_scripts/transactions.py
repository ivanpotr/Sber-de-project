# Загрузка библиотек
import pandas as pd
import os
import psycopg2
import psycopg2.extras


def downland_transactions(cursor, path_load, cur_date):
    cur_date_old = cur_date
    try:
        # Загрузка данных
        df = pd.read_csv(
            f'{path_load}/transactions_{cur_date}.txt', sep=';', decimal=',')
        cur_date = cur_date_old[-4:] + '-' + \
            cur_date_old[-6:-4] + '-' + cur_date_old[:-6]

        # 0. Загрузка данным в мета таблицу, если она пустая
        sql_query = '''
        insert into deaian.ptrc_meta_loads ( 
                                                schema_name
                                                ,table_name
                                                ,max_update_dt
                                                ,processed_dt
                                            )
        select 
                'deaian'
                ,'ptrc_stg_transactions' 
                ,to_date ('1900-01-01','YYYY-MM-DD')
                ,now()
        where not exists ( select schema_name, table_name from deaian.ptrc_meta_loads 
                            where schema_name = 'deaian' and table_name = 'ptrc_stg_transactions'
                        );
        '''
        cursor.executemany(sql_query, df.values.tolist())

        # 1. Очистка staging
        cursor.execute("delete from deaian.ptrc_stg_transactions;")
        cursor.execute("delete from deaian.ptrc_stg_transactions_tmp;")

        # 2. Захват данных из источника в staging
        sql_query = f'''
        insert into deaian.ptrc_stg_transactions_tmp(
                                                    trans_id
                                                    ,trans_date
                                                    ,amt
                                                    ,card_num
                                                    ,oper_type
                                                    ,oper_result
                                                    ,terminal
                                                    ,update_dt
                                                    ,processed_dt
                                                ) 
            values(
                        %s
                        ,to_timestamp(%s, 'YYYY-MM-DD hh24:mi:ss')
                        ,%s
                        ,%s
                        ,%s
                        ,%s
                        ,%s
                        ,cast('{cur_date}' as date)
                        ,now()
                );
            '''
        cursor.executemany(sql_query, df.values.tolist())
        # psycopg2.extras.execute_batch(cursor, sql_query, df.values.tolist(), page_size=100)
        sql_query = '''
            insert into deaian.ptrc_stg_transactions ( 
                                                    trans_id
                                                    ,trans_date
                                                    ,amt
                                                    ,card_num
                                                    ,oper_type
                                                    ,oper_result
                                                    ,terminal
                                                    ,update_dt
                                                    ,processed_dt
                                                )
        select
                trans_id
                ,trans_date
                ,amt
                ,card_num
                ,oper_type
                ,oper_result
                ,terminal
                ,update_dt
                ,now()
        from deaian.ptrc_stg_transactions_tmp
        where update_dt > coalesce (
                                        (
                                            select max_update_dt
                                            from deaian.ptrc_meta_loads
                                            where schema_name = 'deaian' and table_name = 'ptrc_stg_transactions'
                                    ), to_date('1900-01-01','YYYY-MM-DD')
                                );
        '''
        cursor.execute(sql_query)

        # 3. Применение данных в приемник DDS (вставка)
        sql_query = '''
        insert into deaian.ptrc_dwh_fact_tracnsactions(
                                                        trans_id
                                                        ,trans_date
                                                        ,amt
                                                        ,card_num
                                                        ,oper_type
                                                        ,oper_result
                                                        ,terminal                                             
                                                        ,processed_dt
                                                    )
        select
                pst.trans_id
                ,pst.trans_date
                ,pst.amt
                ,pst.card_num
                ,pst.oper_type
                ,pst.oper_result
                ,pst.terminal
                ,now()
        from deaian.ptrc_stg_transactions as pst
        left join deaian.ptrc_dwh_fact_tracnsactions as pdft
        on pst.trans_id = pdft.trans_id
        where pdft.trans_id is null;
        '''
        cursor.execute(sql_query)

        # 4. Сохраняем состояние загрузки в метаданные.
        sql_query = '''
        update deaian.ptrc_meta_loads
        set max_update_dt = (select max( update_dt ) from deaian.ptrc_stg_transactions)
        ,processed_dt = now()
        where schema_name = 'deaian' and table_name = 'ptrc_stg_transactions';
        '''
        cursor.execute(sql_query)

        # Перемещение загруженного файла в архив
        try:
            os.rename(f'{path_load}/transactions_{cur_date_old}.txt',
                      f'{path_load}/archive/transactions_{cur_date_old}.txt.backup')
        except FileExistsError:
            print(
                f'Такой файл transactions_{cur_date_old}.txt.backup уже существует в архиве')
            os.remove(f'{path_load}/transactions_{cur_date_old}.txt')

        print(f'Загрузка файла завершина transactions_{cur_date_old}.txt')
    except:
        print(f'Ошибка загрузки файла transactions_{cur_date_old}.txt')
