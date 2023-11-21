# Загрузка библиотек
import psycopg2
import pandas as pd


def downland_accounts(cursor, cur_date):
    try:
        # Загрузка данных
        conn_local = psycopg2.connect(database="",
                                      host="",
                                      user="",
                                      password="",
                                      port=""
                                      )

        cursor_local = conn_local.cursor()

        cursor_local.execute("select * from info.accounts")
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
				,'ptrc_stg_accounts' 
				,to_date ('1900-01-01','YYYY-MM-DD')
				,now()
		where not exists ( select schema_name, table_name from deaian.ptrc_meta_loads 
							where schema_name = 'deaian' and table_name = 'ptrc_stg_accounts'
						);
		'''
        cursor.execute(sql_query)

        # 1. Очистка staging
        cursor.execute('delete from deaian.ptrc_stg_accounts_tmp;')
        cursor.execute('delete from deaian.ptrc_stg_accounts;')
        cursor.execute('delete from deaian.ptrc_stg_accounts_del;')

        # 2. Захват данных из источника в staging
        sql_query = f'''
		insert into deaian.ptrc_stg_accounts_tmp(
													account_num
													,valid_to
													,client
													,create_dt
													,update_dt
													,processed_dt
												) 
			values(
					%s
					,%s
					,%s
					,%s
					,coalesce( %s, cast('{cur_date}' as date))
					,now()
				);
		'''
        cursor.executemany(sql_query, df.values.tolist())

        sql_query = '''
		insert into deaian.ptrc_stg_accounts ( 
												account_num
												,valid_to
												,client
												,create_dt
												,update_dt
												,processed_dt
											)
		select
				account_num
				,valid_to
				,client
				,create_dt
				,update_dt
				,now()
		from deaian.ptrc_stg_accounts_tmp
		where update_dt > coalesce (
										(
											select max_update_dt
											from deaian.ptrc_meta_loads
											where schema_name = 'deaian' and table_name = 'ptrc_stg_accounts'
									), to_date('1900-01-01','YYYY-MM-DD')
								);
		'''
        cursor.execute(sql_query)

        sql_query = '''
		insert into deaian.ptrc_stg_accounts_del (
													account_num
													,processed_dt
												)
		select 
			account_num
			,now()
		from deaian.ptrc_stg_accounts_tmp
		where update_dt > coalesce (
										(
											select max_update_dt
											from deaian.ptrc_meta_loads
											where schema_name = 'deaian' and table_name = 'ptrc_stg_accounts'
									), to_date('1900-01-01','YYYY-MM-DD')
								);
		'''
        cursor.execute(sql_query)

        # 3. Применение данных в приемник DDS (вставка)
        sql_query = '''
		insert into deaian.ptrc_dwh_dim_accounts_hist (
												account_num
												,valid_to
												,client
												,effective_from
												,effective_to
												,deleted_flg
												,processed_dt
											)
		select
			psa.account_num
			,psa.valid_to
			,psa.client
			,psa.create_dt
			,to_date('9999-12-31','YYYY-MM-DD')
			,'N'
			,now()
		from deaian.ptrc_stg_accounts as psa
		left join deaian.ptrc_dwh_dim_accounts_hist as pddah
		on psa.account_num = pddah.account_num
		where pddah.account_num is null;
		'''
        cursor.execute(sql_query)

        # 4. Применение данных в приемник DDS (обновление)
        sql_query = '''
		update deaian.ptrc_dwh_dim_accounts_hist
		set 
			effective_to = tmp.update_dt - interval '1 day',
			processed_dt = now()
		from (
			select
				psa.account_num
				,psa.valid_to
				,psa.client
				,psa.create_dt
				,psa.update_dt
			from deaian.ptrc_stg_accounts as psa
			inner join deaian.ptrc_dwh_dim_accounts_hist as pddah
			on psa.account_num = pddah.account_num
				and pddah.effective_to = to_date('9999-12-31','YYYY-MM-DD')
				and pddah.deleted_flg = 'N'
			where 
				(psa.valid_to <> pddah.valid_to 
					or (psa.valid_to is null and pddah.valid_to is not null) 
					or (psa.valid_to is not null and pddah.valid_to is null)
				)
			or (psa.client <> pddah.client 
					or (psa.client is null and pddah.client is not null) 
					or (psa.client is not null and pddah.client is null)
				)
			or (psa.create_dt <> pddah.effective_from 
					or (psa.create_dt is null and pddah.effective_from is not null) 
					or (psa.create_dt is not null and pddah.effective_from is null)
				)
		) tmp
		where 1=1
  			and ptrc_dwh_dim_accounts_hist.account_num = tmp.account_num
     		and ptrc_dwh_dim_accounts_hist.effective_to = to_date('9999-12-31','YYYY-MM-DD')
     		and ptrc_dwh_dim_accounts_hist.deleted_flg = 'N';
		'''
        cursor.execute(sql_query)

        sql_query = '''
		insert into deaian.ptrc_dwh_dim_accounts_hist ( 
														account_num
														,valid_to
														,client
														,effective_from
														,effective_to
														,deleted_flg
														,processed_dt
														)
		select
			psa.account_num
			,psa.valid_to
			,psa.client
			,psa.update_dt
			,to_date('9999-12-31','YYYY-MM-DD')
			,'N'
			,now()
		from deaian.ptrc_stg_accounts as psa
		inner join deaian.ptrc_dwh_dim_accounts_hist as pddah
		on psa.account_num= pddah.account_num
			and pddah.effective_to = psa.update_dt - interval '1 day'
			and pddah.deleted_flg = 'N'
		where 1=1
			and (
					(psa.valid_to <> pddah.valid_to 
						or (psa.valid_to is null and pddah.valid_to is not null) 
						or (psa.valid_to is not null and pddah.valid_to is null)
					)
				or (psa.client <> pddah.client 
						or (psa.client is null and pddah.client is not null) 
						or (psa.client is not null and pddah.client is null)
					)
				or (psa.create_dt <> pddah.effective_from 
						or (psa.create_dt is null and pddah.effective_from is not null) 
						or (psa.create_dt is not null and pddah.effective_from is null)
					)
			)
   			and pddah.effective_to = psa.update_dt - interval '1 day'
			and pddah.deleted_flg = 'N';  
		'''
        cursor.execute(sql_query)

        # 5. Применение данных в приемник DDS (удаление)
        sql_query = f'''
		insert into deaian.ptrc_dwh_dim_accounts_hist (
										account_num
										,valid_to
										,client
										,effective_from
										,effective_to
										,deleted_flg
										,processed_dt
									)
		select
			account_num
			,valid_to
			,client
			,cast('{cur_date}' as date)
			,to_date('9999-12-31','YYYY-MM-DD')
			,'Y'
			,now()
		from deaian.ptrc_dwh_dim_accounts_hist
		where account_num in (
				select 
					pddah.account_num
				from deaian.ptrc_dwh_dim_accounts_hist as pddah
				left join deaian.ptrc_stg_accounts_del as psad
				on pddah.account_num = psad.account_num
				where 1=1
						and pddah.effective_to = to_date('9999-12-31','YYYY-MM-DD')
						and pddah.deleted_flg = 'N'
						and psad.account_num is null
			)
			and effective_to = to_date('9999-12-31','YYYY-MM-DD')
			and deleted_flg = 'N';
		'''
        cursor.execute(sql_query)

        sql_query = f'''
		update deaian.ptrc_dwh_dim_accounts_hist
		set
			effective_to = cast('{cur_date}' as date) - interval '1 day',
			processed_dt = now()
		where account_num in (
			select 
				pddah.account_num
			from deaian.ptrc_dwh_dim_accounts_hist as pddah
			left join deaian.ptrc_stg_accounts_del as psad
			on pddah.account_num = psad.account_num
			where 1=1
					and pddah.effective_to = to_date('9999-12-31','YYYY-MM-DD')
					and pddah.deleted_flg = 'N'
					and psad.account_num is null
		)
		and effective_to = to_date('9999-12-31','YYYY-MM-DD')
		and deleted_flg = 'N';
		'''
        cursor.execute(sql_query)

        # 6. Сохраняем состояние загрузки в метаданные.
        sql_query = '''
		update deaian.ptrc_meta_loads
		set max_update_dt = (select max( update_dt ) from deaian.ptrc_stg_accounts)
		,processed_dt = now()
		where schema_name = 'deaian' and table_name = 'ptrc_stg_accounts';
		'''
        cursor.execute(sql_query)

        print(f'Загрузка данных из info.accounts завершена на {cur_date}')
    except:
        print(f'Ошибка загрузки данных из info.accounts на {cur_date}')
