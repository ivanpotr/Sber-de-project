# Загрузка библиотек
import psycopg2
import pandas as pd


def downland_clients(cursor, cur_date):
    try:
        # Загрузка данных
        conn_local = psycopg2.connect(database="",
                                      host="",
                                      user="",
                                      password="",
                                      port=""
                                      )

        cursor_local = conn_local.cursor()

        cursor_local.execute("select * from info.clients")
        records = cursor_local.fetchall()

        names = [x[0] for x in cursor_local.description]
        df = pd.DataFrame(records, columns=names)

        cursor_local.close()
        conn_local.close()

        cur_date = cur_date[-4:] + '-' + cur_date[-6:-4] + '-' + cur_date[:-6]

        # 0. Загрузка данным в мета таблицу, если она пустая
        sql_query = '''
		insert into deaian.ptrc_meta_loads ( 
											schema_name
											,table_name
											,max_update_dt
											,processed_dt )
		select 
				'deaian'
				,'ptrc_stg_clients' 
				,to_date ('1900-01-01','YYYY-MM-DD')
				,now()
		where not exists ( select schema_name, table_name from deaian.ptrc_meta_loads 
							where schema_name = 'deaian' and table_name = 'ptrc_stg_clients'
						);
		'''
        cursor.execute(sql_query)

        # 1. Очистка staging
        cursor.execute('delete from deaian.ptrc_stg_clients_tmp;')
        cursor.execute('delete from deaian.ptrc_stg_clients;')
        cursor.execute('delete from deaian.ptrc_stg_clients_del;')

        # 2. Захват данных из источника в staging
        sql_query = f'''
		insert into deaian.ptrc_stg_clients_tmp(
													client_id
													,last_name
													,first_name
													,patronymic
													,date_of_birth
													,passport_num
													,passport_valid_to
													,phone
													,create_dt
													,update_dt
													,processed_dt
												) 
			values(
					%s
					,%s
					,%s
					,%s
					,%s
					,%s
					,%s
					,%s
					,%s
					,coalesce( %s, cast('{cur_date}' as date))
					,now()
				);
		'''
        cursor.executemany(sql_query, df.values.tolist())

        sql_query = '''
		insert into deaian.ptrc_stg_clients ( 
												client_id
												,last_name
												,first_name
												,patronymic
												,date_of_birth
												,passport_num
												,passport_valid_to
												,phone
												,create_dt
												,update_dt
												,processed_dt
											)
		select
				client_id
				,last_name
				,first_name
				,patronymic
				,date_of_birth
				,passport_num
				,passport_valid_to
				,phone
				,create_dt
				,update_dt
				,now()
		from deaian.ptrc_stg_clients_tmp
		where update_dt > coalesce (
										(
											select max_update_dt
											from deaian.ptrc_meta_loads
											where schema_name = 'deaian' and table_name = 'ptrc_stg_clients'
									), to_date('1900-01-01','YYYY-MM-DD')
								);
		'''
        cursor.execute(sql_query)

        sql_query = '''
		insert into deaian.ptrc_stg_clients_del (
													client_id
													,processed_dt
												)
		select 
			client_id
			,now()
		from deaian.ptrc_stg_clients_tmp
		where update_dt > coalesce (
										(
											select max_update_dt
											from deaian.ptrc_meta_loads
											where schema_name = 'deaian' and table_name = 'ptrc_stg_clients'
									), to_date('1900-01-01','YYYY-MM-DD')
								);
		'''
        cursor.execute(sql_query)

        # 3. Применение данных в приемник DDS (вставка)last_name
        sql_query = '''
		insert into deaian.ptrc_dwh_dim_clients_hist (
												client_id
												,last_name
												,first_name
												,patronymic
												,date_of_birth
												,passport_num
												,passport_valid_to
												,phone
												,effective_from
												,effective_to
												,deleted_flg
												,processed_dt
											)
		select
			psc.client_id
			,psc.last_name
			,psc.first_name
			,psc.patronymic
			,psc.date_of_birth
			,psc.passport_num
			,psc.passport_valid_to
			,psc.phone
			,psc.create_dt
			,to_date('9999-12-31','YYYY-MM-DD')
			,'N'
			,now()
		from deaian.ptrc_stg_clients as psc
		left join deaian.ptrc_dwh_dim_clients_hist as pddch
		on psc.client_id = pddch.client_id
		where pddch.client_id is null;
		'''
        cursor.execute(sql_query)

        # 4. Применение данных в приемник DDS (обновление)
        sql_query = '''
		update deaian.ptrc_dwh_dim_clients_hist
		set 
			effective_to = tmp.update_dt - interval '1 day',
			processed_dt = now()
		from (
			select
				psc.client_id
				,psc.last_name
				,psc.first_name
				,psc.patronymic
				,psc.date_of_birth
				,psc.passport_num
				,psc.passport_valid_to
				,psc.phone
				,psc.create_dt
				,psc.update_dt
			from deaian.ptrc_stg_clients as psc
			inner join deaian.ptrc_dwh_dim_clients_hist as pddch
			on psc.client_id = pddch.client_id
				and pddch.effective_to = to_date('9999-12-31','YYYY-MM-DD')
				and pddch.deleted_flg = 'N'
			where 
				(psc.last_name <> pddch.last_name 
					or (psc.last_name is null and pddch.last_name is not null) 
					or (psc.last_name is not null and pddch.last_name is null)
				)
			or (psc.first_name <> pddch.first_name 
					or (psc.first_name is null and pddch.first_name is not null) 
					or (psc.first_name is not null and pddch.first_name is null)
				)
			or (psc.patronymic <> pddch.patronymic 
					or (psc.patronymic is null and pddch.patronymic is not null) 
					or (psc.patronymic is not null and pddch.patronymic is null)
				)
			or (psc.date_of_birth <> pddch.date_of_birth 
					or (psc.date_of_birth is null and pddch.date_of_birth is not null) 
					or (psc.date_of_birth is not null and pddch.date_of_birth is null)
				)
			or (psc.passport_num <> pddch.passport_num 
					or (psc.passport_num is null and pddch.passport_num is not null) 
					or (psc.passport_num is not null and pddch.passport_num is null)
				)
			or (psc.passport_valid_to <> pddch.passport_valid_to 
					or (psc.passport_valid_to is null and pddch.passport_valid_to is not null) 
					or (psc.passport_valid_to is not null and pddch.passport_valid_to is null)
				)
			or (psc.phone <> pddch.phone 
					or (psc.phone is null and pddch.phone is not null) 
					or (psc.phone is not null and pddch.phone is null)
				)
			or (psc.create_dt <> pddch.effective_from 
					or (psc.create_dt is null and pddch.effective_from is not null) 
					or (psc.create_dt is not null and pddch.effective_from is null)
				)
		) tmp
		where 1=1
  			and ptrc_dwh_dim_clients_hist.client_id = tmp.client_id
			and ptrc_dwh_dim_clients_hist.effective_to = to_date('9999-12-31','YYYY-MM-DD')
     		and ptrc_dwh_dim_clients_hist.deleted_flg = 'N';
		'''
        cursor.execute(sql_query)

        sql_query = '''
		insert into deaian.ptrc_dwh_dim_clients_hist ( 
														client_id
														,last_name
														,first_name
														,patronymic
														,date_of_birth
														,passport_num
														,passport_valid_to
														,phone
														,effective_from
														,effective_to
														,deleted_flg
														,processed_dt
														)
		select
			psc.client_id
			,psc.last_name
			,psc.first_name
			,psc.patronymic
			,psc.date_of_birth
			,psc.passport_num
			,psc.passport_valid_to
			,psc.phone
			,psc.update_dt
			,to_date('9999-12-31','YYYY-MM-DD')
			,'N'
			,now()
		from deaian.ptrc_stg_clients as psc
		inner join deaian.ptrc_dwh_dim_clients_hist as pddch
		on psc.client_id= pddch.client_id
			and pddch.effective_to = psc.update_dt - interval '1 day'
			and pddch.deleted_flg = 'N'
		where 1=1
			and (
					(psc.last_name <> pddch.last_name 
						or (psc.last_name is null and pddch.last_name is not null) 
						or (psc.last_name is not null and pddch.last_name is null)
					)
				or (psc.first_name <> pddch.first_name 
						or (psc.first_name is null and pddch.first_name is not null) 
						or (psc.first_name is not null and pddch.first_name is null)
					)
				or (psc.patronymic <> pddch.patronymic 
						or (psc.patronymic is null and pddch.patronymic is not null) 
						or (psc.patronymic is not null and pddch.patronymic is null)
					)
				or (psc.date_of_birth <> pddch.date_of_birth 
						or (psc.date_of_birth is null and pddch.date_of_birth is not null) 
						or (psc.date_of_birth is not null and pddch.date_of_birth is null)
					)
				or (psc.passport_num <> pddch.passport_num 
						or (psc.passport_num is null and pddch.passport_num is not null) 
						or (psc.passport_num is not null and pddch.passport_num is null)
					)
				or (psc.passport_valid_to <> pddch.passport_valid_to 
						or (psc.passport_valid_to is null and pddch.passport_valid_to is not null) 
						or (psc.passport_valid_to is not null and pddch.passport_valid_to is null)
					)
				or (psc.phone <> pddch.phone 
						or (psc.phone is null and pddch.phone is not null) 
						or (psc.phone is not null and pddch.phone is null)
					)
				or (psc.create_dt <> pddch.effective_from 
						or (psc.create_dt is null and pddch.effective_from is not null) 
						or (psc.create_dt is not null and pddch.effective_from is null)
					)
			)
   			and pddch.effective_to = psc.update_dt - interval '1 day'
			and pddch.deleted_flg = 'N';  
		'''
        cursor.execute(sql_query)

        # 5. Применение данных в приемник DDS (удаление)
        sql_query = f'''
		insert into deaian.ptrc_dwh_dim_clients_hist (
										client_id
										,last_name
										,first_name
										,patronymic
										,date_of_birth
										,passport_num
										,passport_valid_to
										,phone
										,effective_from
										,effective_to
										,deleted_flg
										,processed_dt
									)
		select
			client_id
			,last_name
			,first_name
			,patronymic
			,date_of_birth
			,passport_num
			,passport_valid_to
			,phone
			,cast('{cur_date}' as date)
			,to_date('9999-12-31','YYYY-MM-DD')
			,'Y'
			,now()
		from deaian.ptrc_dwh_dim_clients_hist
		where client_id in (
				select 
					pddch.client_id
				from deaian.ptrc_dwh_dim_clients_hist as pddch
				left join deaian.ptrc_stg_clients_del as pscd
				on pddch.client_id = pscd.client_id
				where 1=1
						and pddch.effective_to = to_date('9999-12-31','YYYY-MM-DD')
						and pddch.deleted_flg = 'N'
						and pscd.client_id is null
			)
			and effective_to = to_date('9999-12-31','YYYY-MM-DD')
			and deleted_flg = 'N';
		'''
        cursor.execute(sql_query)

        sql_query = f'''
		update deaian.ptrc_dwh_dim_clients_hist
		set
			effective_to = cast('{cur_date}' as date) - interval '1 day',
			processed_dt = now()
		where client_id in (
			select 
				pddch.client_id
			from deaian.ptrc_dwh_dim_clients_hist as pddch
			left join deaian.ptrc_stg_clients_del as pscd
			on pddch.client_id = pscd.client_id
			where 1=1
					and pddch.effective_to = to_date('9999-12-31','YYYY-MM-DD')
					and pddch.deleted_flg = 'N'
					and pscd.client_id is null
		)
		and effective_to = to_date('9999-12-31','YYYY-MM-DD')
		and deleted_flg = 'N';
		'''
        cursor.execute(sql_query)

        # 6. Сохраняем состояние загрузки в метаданные.
        sql_query = '''
		update deaian.ptrc_meta_loads
		set max_update_dt = (select max( update_dt ) from deaian.ptrc_stg_clients)
  		,processed_dt = now()
		where schema_name = 'deaian' and table_name = 'ptrc_stg_clients';
		'''
        cursor.execute(sql_query)

        print(f'Загрузка данных из info.clients завершена на {cur_date}')
    except:
        print(f'Ошибка загрузки данных из info.clients на {cur_date}')
