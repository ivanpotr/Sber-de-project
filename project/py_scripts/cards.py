# Загрузка библиотек
import psycopg2
import pandas as pd


def downland_cards(cursor, cur_date):
    try:
        # Загрузка данных
        conn_local = psycopg2.connect(database="",
                                      host="",
                                      user="",
                                      password="",
                                      port=""
                                      )

        cursor_local = conn_local.cursor()

        cursor_local.execute("select * from info.cards")
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
				,'ptrc_stg_cards' 
				,to_date ('1900-01-01','YYYY-MM-DD')
				,now()
		where not exists ( select schema_name, table_name from deaian.ptrc_meta_loads 
							where schema_name = 'deaian' and table_name = 'ptrc_stg_cards'
						);
		'''
        cursor.execute(sql_query)

        # 1. Очистка staging
        cursor.execute('delete from deaian.ptrc_stg_cards_tmp;')
        cursor.execute('delete from deaian.ptrc_stg_cards;')
        cursor.execute('delete from deaian.ptrc_stg_cards_del;')

        # 2. Захват данных из источника в staging
        sql_query = f'''
		insert into deaian.ptrc_stg_cards_tmp(
													card_num
													,account_num
													,create_dt
													,update_dt
													,processed_dt
												) 
			values(
					%s
					,%s
					,%s
					,coalesce( %s, cast('{cur_date}' as date))
					,now()
				);
		'''
        cursor.executemany(sql_query, df.values.tolist())

        sql_query = '''
		insert into deaian.ptrc_stg_cards ( 
												card_num
												,account_num
												,create_dt
												,update_dt
												,processed_dt
											)
		select
				trim(card_num)
				,account_num
				,create_dt
				,update_dt
				,now()
		from deaian.ptrc_stg_cards_tmp
		where update_dt > coalesce (
										(
											select max_update_dt
											from deaian.ptrc_meta_loads
											where schema_name = 'deaian' and table_name = 'ptrc_stg_cards'
									), to_date('1900-01-01','YYYY-MM-DD')
								);
		'''
        cursor.execute(sql_query)

        sql_query = '''
		insert into deaian.ptrc_stg_cards_del (
													card_num
													,processed_dt
												)
		select 
			trim(card_num)
			,now()
		from deaian.ptrc_stg_cards_tmp
		where update_dt > coalesce (
										(
											select max_update_dt
											from deaian.ptrc_meta_loads
											where schema_name = 'deaian' and table_name = 'ptrc_stg_cards'
									), to_date('1900-01-01','YYYY-MM-DD')
								);
		'''
        cursor.execute(sql_query)

        # 3. Применение данных в приемник DDS (вставка)
        sql_query = '''
		insert into deaian.ptrc_dwh_dim_cards_hist (
												card_num
												,account_num
												,effective_from
												,effective_to
												,deleted_flg
												,processed_dt
											)
		select
			trim(psc.card_num)
			,psc.account_num
			,psc.create_dt
			,to_date('9999-12-31','YYYY-MM-DD')
			,'N'
			,now()
		from deaian.ptrc_stg_cards as psc
		left join deaian.ptrc_dwh_dim_cards_hist as pddch
		on psc.card_num = pddch.card_num
		where pddch.card_num is null;
		'''
        cursor.execute(sql_query)

        # 4. Применение данных в приемник DDS (обновление)
        sql_query = '''
		update deaian.ptrc_dwh_dim_cards_hist
		set 
			effective_to = tmp.update_dt - interval '1 day',
			processed_dt = now()
		from (
			select
				psc.card_num
				,psc.account_num
				,psc.create_dt
				,psc.update_dt
			from deaian.ptrc_stg_cards as psc
			inner join deaian.ptrc_dwh_dim_cards_hist as pddch
			on psc.card_num = pddch.card_num
				and pddch.effective_to = to_date('9999-12-31','YYYY-MM-DD')
				and pddch.deleted_flg = 'N'
			where 
				(psc.account_num <> pddch.account_num 
					or (psc.account_num is null and pddch.account_num is not null) 
					or (psc.account_num is not null and pddch.account_num is null)
				)
			or (psc.create_dt <> pddch.effective_from 
					or (psc.create_dt is null and pddch.effective_from is not null) 
					or (psc.create_dt is not null and pddch.effective_from is null)
				)
		) tmp
		where 1=1
  			and ptrc_dwh_dim_cards_hist.card_num = tmp.card_num
			and ptrc_dwh_dim_cards_hist.effective_to = to_date('9999-12-31','YYYY-MM-DD')
     		and ptrc_dwh_dim_cards_hist.deleted_flg = 'N';
		'''
        cursor.execute(sql_query)

        sql_query = '''
		insert into deaian.ptrc_dwh_dim_cards_hist ( 
														card_num
														,account_num
														,effective_from
														,effective_to
														,deleted_flg
														,processed_dt
														)
		select
			psc.card_num
			,psc.account_num
			,psc.update_dt
			,to_date('9999-12-31','YYYY-MM-DD')
			,'N'
			,now()
		from deaian.ptrc_stg_cards as psc
		inner join deaian.ptrc_dwh_dim_cards_hist as pddch
		on psc.card_num= pddch.card_num
			and pddch.effective_to = psc.update_dt - interval '1 day'
			and pddch.deleted_flg = 'N'
		where 1=1
			and (
					(psc.account_num <> pddch.account_num 
						or (psc.account_num is null and pddch.account_num is not null) 
						or (psc.account_num is not null and pddch.account_num is null)
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
		insert into deaian.ptrc_dwh_dim_cards_hist (
										card_num
										,account_num
										,effective_from
										,effective_to
										,deleted_flg
										,processed_dt
									)
		select
			card_num
			,account_num
			,cast('{cur_date}' as date)
			,to_date('9999-12-31','YYYY-MM-DD')
			,'Y'
			,now()
		from deaian.ptrc_dwh_dim_cards_hist
		where card_num in (
				select 
					pddch.card_num
				from deaian.ptrc_dwh_dim_cards_hist as pddch
				left join deaian.ptrc_stg_cards_del as pscd
				on pddch.card_num = pscd.card_num
				where 1=1
						and pddch.effective_to = to_date('9999-12-31','YYYY-MM-DD')
						and pddch.deleted_flg = 'N'
						and pscd.card_num is null
			)
			and effective_to = to_date('9999-12-31','YYYY-MM-DD')
			and deleted_flg = 'N';
		'''
        cursor.execute(sql_query)

        sql_query = f'''
		update deaian.ptrc_dwh_dim_cards_hist
		set
			effective_to = cast('{cur_date}' as date) - interval '1 day',
			processed_dt = now()
		where card_num in (
			select 
				pddch.card_num
			from deaian.ptrc_dwh_dim_cards_hist as pddch
			left join deaian.ptrc_stg_cards_del as pscd
			on pddch.card_num = pscd.card_num
			where 1=1
					and pddch.effective_to = to_date('9999-12-31','YYYY-MM-DD')
					and pddch.deleted_flg = 'N'
					and pscd.card_num is null
		)
		and effective_to = to_date('9999-12-31','YYYY-MM-DD')
		and deleted_flg = 'N';
		'''
        cursor.execute(sql_query)

        # 6. Сохраняем состояние загрузки в метаданные.
        sql_query = '''
		update deaian.ptrc_meta_loads
		set max_update_dt = (select max( update_dt ) from deaian.ptrc_stg_cards)
		,processed_dt = now()
		where schema_name = 'deaian' and table_name = 'ptrc_stg_cards';
		'''
        cursor.execute(sql_query)

        print(f'Загрузка данных из info.cards завершена на {cur_date}')
    except:
        print(f'Ошибка загрузки данных из info.cards на {cur_date}')
