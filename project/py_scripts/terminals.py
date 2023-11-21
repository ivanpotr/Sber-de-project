# Загрузка библиотек
import pandas as pd
import os


def downland_terminals(cursor, path_load, cur_date):
    cur_date_old = cur_date
    try:
        df = pd.read_excel(f'{path_load}/terminals_{cur_date}.xlsx',
                           sheet_name='terminals', header=0, index_col=None)
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
				,'ptrc_stg_terminals' 
				,to_date ('1900-01-01','YYYY-MM-DD')
				,now()
		where not exists ( select schema_name, table_name from deaian.ptrc_meta_loads 
							where schema_name = 'deaian' and table_name = 'ptrc_stg_terminals'
						);
		'''
        cursor.execute(sql_query)

        # 1. Очистка staging
        cursor.execute('delete from deaian.ptrc_stg_terminals_tmp;')
        cursor.execute('delete from deaian.ptrc_stg_terminals;')
        cursor.execute('delete from deaian.ptrc_stg_terminals_del;')

        # 2. Захват данных из источника в staging
        sql_query = f'''
		insert into deaian.ptrc_stg_terminals_tmp(
													terminal_id
													,terminal_type
													,terminal_city
													,terminal_address
													,update_dt
													,processed_dt
												) 
			values(
					%s
					,%s
					,%s
					,%s
					,cast('{cur_date}' as date)
					,now()
				);
		'''
        cursor.executemany(sql_query, df.values.tolist())

        sql_query = '''
		insert into deaian.ptrc_stg_terminals ( 
												terminal_id
												,terminal_type
												,terminal_city
												,terminal_address
												,update_dt
												,processed_dt
											)
		select
				terminal_id
				,terminal_type
				,terminal_city
				,terminal_address
				,update_dt
				,now()
		from deaian.ptrc_stg_terminals_tmp
		where update_dt > coalesce (
										(
											select max_update_dt
											from deaian.ptrc_meta_loads
											where schema_name = 'deaian' and table_name = 'ptrc_stg_terminals'
									), to_date('1900-01-01','YYYY-MM-DD')
								);
		'''
        cursor.execute(sql_query)

        sql_query = '''
		insert into deaian.ptrc_stg_terminals_del (
													terminal_id
													,processed_dt
												)
		select 
			terminal_id
			,now()
		from deaian.ptrc_stg_terminals_tmp
		where update_dt > coalesce (
										(
											select max_update_dt
											from deaian.ptrc_meta_loads
											where schema_name = 'deaian' and table_name = 'ptrc_stg_terminals'
									), to_date('1900-01-01','YYYY-MM-DD')
								);
		'''
        cursor.execute(sql_query)

        # 3. Применение данных в приемник DDS (вставка)
        sql_query = '''
		insert into deaian.ptrc_dwh_dim_terminals_hist (
												terminal_id
												,terminal_type
												,terminal_city
												,terminal_address
												,effective_from
												,effective_to
												,deleted_flg
												,processed_dt
											)
		select
			pst.terminal_id
			,pst.terminal_type
			,pst.terminal_city
			,pst.terminal_address
			,pst.update_dt
			,to_date('9999-12-31','YYYY-MM-DD')
			,'N'
			,now()
		from deaian.ptrc_stg_terminals as pst
		left join deaian.ptrc_dwh_dim_terminals_hist as pddth
		on pst.terminal_id = pddth.terminal_id
		where pddth.terminal_id is null;
		'''
        cursor.execute(sql_query)

        # 4. Применение данных в приемник DDS (обновление)
        sql_query = '''
		update deaian.ptrc_dwh_dim_terminals_hist
		set 
			effective_to = tmp.update_dt - interval '1 day',
			processed_dt = now()
		from (
			select
				pst.terminal_id
				,pst.terminal_type
				,pst.terminal_city
				,pst.terminal_address
				,pst.update_dt
			from deaian.ptrc_stg_terminals as pst
			inner join deaian.ptrc_dwh_dim_terminals_hist as pddth
			on pst.terminal_id = pddth.terminal_id
     			and pddth.effective_to = to_date('9999-12-31','YYYY-MM-DD')
     			and pddth.deleted_flg = 'N'
			where 
				(pst.terminal_type <> pddth.terminal_type 
					or (pst.terminal_type is null and pddth.terminal_type is not null) 
					or (pst.terminal_type is not null and pddth.terminal_type is null)
				)
			or (pst.terminal_city <> pddth.terminal_city 
					or (pst.terminal_city is null and pddth.terminal_city is not null) 
					or (pst.terminal_city is not null and pddth.terminal_city is null)
				)
			or (pst.terminal_address <> pddth.terminal_address 
					or (pst.terminal_address is null and pddth.terminal_address is not null) 
					or (pst.terminal_address is not null and pddth.terminal_address is null)
				)
		) tmp
		where 1=1
  			and ptrc_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
  			and ptrc_dwh_dim_terminals_hist.effective_to = to_date('9999-12-31','YYYY-MM-DD')
     		and ptrc_dwh_dim_terminals_hist.deleted_flg = 'N';
		'''
        cursor.execute(sql_query)

        sql_query = '''
		insert into deaian.ptrc_dwh_dim_terminals_hist ( 
														terminal_id
														,terminal_type
														,terminal_city
														,terminal_address
														,effective_from
														,effective_to
														,deleted_flg
														,processed_dt
														)
		select
			pst.terminal_id
			,pst.terminal_type
			,pst.terminal_city
			,pst.terminal_address
			,pst.update_dt
			,to_date('9999-12-31','YYYY-MM-DD')
			,'N'
			,now()
		from deaian.ptrc_stg_terminals as pst
		inner join deaian.ptrc_dwh_dim_terminals_hist as pddth
		on pst.terminal_id= pddth.terminal_id
			and pddth.effective_to = pst.update_dt - interval '1 day'
			and pddth.deleted_flg = 'N'
		where 1=1
			and (
					(pst.terminal_type <> pddth.terminal_type 
								or (pst.terminal_type is null and pddth.terminal_type is not null) 
								or (pst.terminal_type is not null and pddth.terminal_type is null)
							)
						or (pst.terminal_city <> pddth.terminal_city 
								or (pst.terminal_city is null and pddth.terminal_city is not null) 
								or (pst.terminal_city is not null and pddth.terminal_city is null)
							)
						or (pst.terminal_address <> pddth.terminal_address 
								or (pst.terminal_address is null and pddth.terminal_address is not null) 
								or (pst.terminal_address is not null and pddth.terminal_address is null)
							)
   				)
			and pddth.effective_to = pst.update_dt - interval '1 day'
			and pddth.deleted_flg = 'N';   
   		'''
        cursor.execute(sql_query)

        # 5. Применение данных в приемник DDS (удаление)
        sql_query = f'''
		insert into deaian.ptrc_dwh_dim_terminals_hist (
										terminal_id
										,terminal_type
										,terminal_city
										,terminal_address
										,effective_from
										,effective_to
										,deleted_flg
										,processed_dt
									)
		select
			terminal_id
			,terminal_type
			,terminal_city
			,terminal_address
			,cast('{cur_date}' as date)
			,to_date('9999-12-31','YYYY-MM-DD')
			,'Y'
			,now()
		from deaian.ptrc_dwh_dim_terminals_hist
		where terminal_id in (
				select 
					pddth.terminal_id
				from deaian.ptrc_dwh_dim_terminals_hist as pddth
				left join deaian.ptrc_stg_terminals_del as pstd
				on pddth.terminal_id = pstd.terminal_id
				where 1=1
						and pddth.effective_to = to_date('9999-12-31','YYYY-MM-DD')
						and pddth.deleted_flg = 'N'
						and pstd.terminal_id is null
			)
			and effective_to = to_date('9999-12-31','YYYY-MM-DD')
			and deleted_flg = 'N'
   			;
		'''
        cursor.execute(sql_query)

        sql_query = f'''
		update deaian.ptrc_dwh_dim_terminals_hist
		set
			effective_to = cast('{cur_date}' as date) - interval '1 day',
			processed_dt = now()
		where terminal_id in (
			select 
				pddth.terminal_id
			from deaian.ptrc_dwh_dim_terminals_hist as pddth
			left join deaian.ptrc_stg_terminals_del as pstd
			on pddth.terminal_id = pstd.terminal_id
			where 1=1
					and pddth.effective_to = to_date('9999-12-31','YYYY-MM-DD')
					and pddth.deleted_flg = 'N'
					and pstd.terminal_id is null
		)
		and effective_to = to_date('9999-12-31','YYYY-MM-DD')
		and deleted_flg = 'N';
		'''
        cursor.execute(sql_query)

        # 6. Сохраняем состояние загрузки в метаданные.
        sql_query = '''
		update deaian.ptrc_meta_loads
		set max_update_dt = (select max( update_dt ) from deaian.ptrc_stg_terminals)
		,processed_dt = now()
		where schema_name = 'deaian' and table_name = 'ptrc_stg_terminals';
		'''
        cursor.execute(sql_query)

        # Перемещение загруженного файла в архив
        try:
            os.rename(f'{path_load}/terminals_{cur_date_old}.xlsx',
                      f'{path_load}/archive/terminals_{cur_date_old}.xlsx.backup')
        except FileExistsError:
            print(
                f'Такой файл terminals_{cur_date_old}.xlsx.backup уже существует в архиве')
            os.remove(f'{path_load}/terminals_{cur_date_old}.xlsx')

        print(f'Загрузка файла завершена terminals_{cur_date_old}.xlsx')
    except:
        print(f'Ошибка загрузки файла terminals_{cur_date_old}.xlsx')
