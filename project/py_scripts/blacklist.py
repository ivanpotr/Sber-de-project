# Загрузка библиотек
import pandas as pd
import os


def downland_passport_blacklist(cursor, path_load, cur_date):
    cur_date_old = cur_date
    try:
        df = pd.read_excel(f'{path_load}/passport_blacklist_{cur_date}.xlsx',
                           sheet_name='blacklist', header=0, index_col=None)

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
                ,'ptrc_stg_blacklist' 
                ,to_date ('1900-01-01','YYYY-MM-DD')
                ,now()
        where not exists ( select schema_name, table_name from deaian.ptrc_meta_loads 
                            where schema_name = 'deaian' and table_name = 'ptrc_stg_blacklist'
                        );
        '''
        cursor.execute(sql_query)

        # 1. Очистка staging
        cursor.execute("delete from deaian.ptrc_stg_blacklist_tmp;")
        cursor.execute("delete from deaian.ptrc_stg_blacklist;")

        # 2. Захват данных из источника в staging
        sql_query = f'''
        insert into deaian.ptrc_stg_blacklist_tmp(
                                                entry_dt
                                                ,passport_num
                                                ,update_dt
                                                ,processed_dt
                                            ) 
            values(
                    cast(%s as date)
                    ,%s
                    ,cast('{cur_date}' as date)
                    ,now()
                );
        '''
        cursor.executemany(sql_query, df.values.tolist())

        sql_query = '''
        insert into deaian.ptrc_stg_blacklist ( 
                                                entry_dt
                                                ,passport_num
                                                ,update_dt
                                                ,processed_dt
                                            )
        select
                entry_dt
                ,passport_num
                ,update_dt
                ,now()
        from deaian.ptrc_stg_blacklist_tmp
        where update_dt > coalesce (
                                        (
                                            select max_update_dt
                                            from deaian.ptrc_meta_loads
                                            where schema_name = 'deaian' and table_name = 'ptrc_stg_blacklist'
                                    ), to_date('1900-01-01','YYYY-MM-DD')
                                );
        '''
        cursor.execute(sql_query)

        # 3. Применение данных в приемник DDS (вставка)
        sql_query = '''
        insert into deaian.ptrc_dwh_fact_passport_blacklist(
                                                            passport_num
                                                            ,entry_dt
                                                            ,processed_dt
                                                        )
        select 
            psb.passport_num
            ,cast(psb.entry_dt as date)
            ,now()
        from deaian.ptrc_stg_blacklist as psb
        left join deaian.ptrc_dwh_fact_passport_blacklist as pdfpb
        on psb.passport_num = pdfpb.passport_num
        where pdfpb.passport_num is null;
        '''
        cursor.execute(sql_query)

        # 4. Сохраняем состояние загрузки в метаданные.
        sql_query = '''
        update deaian.ptrc_meta_loads
        set max_update_dt = (select max( update_dt ) from deaian.ptrc_stg_blacklist)
        ,processed_dt = now()
        where schema_name = 'deaian' and table_name = 'ptrc_stg_blacklist';
        '''
        cursor.execute(sql_query)

        # Перемещение загруженного файла в архив
        try:
            os.rename(f'{path_load}/passport_blacklist_{cur_date_old}.xlsx',
                      f'{path_load}/archive/passport_blacklist_{cur_date_old}.xlsx.backup')
        except FileExistsError:
            print(
                f'Такой файл passport_blacklist_{cur_date_old}.xlsx.backup уже существует в архиве')
            os.remove(f'{path_load}/passport_blacklist_{cur_date_old}.xlsx')

        print(
            f'Загрузка файла завершина passport_blacklist_{cur_date_old}.xlsx')
    except:
        print(f'Ошибка загрузки файла passport_blacklist_{cur_date_old}.xlsx')
