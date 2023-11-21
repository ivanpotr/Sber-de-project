# Загрузка библиотек
import pandas as pd


def creat_fraud(cursor, cur_date):
    try:

        # cur_date = cur_date[-4:] + '-' + cur_date[-6:-4] + '-' + cur_date[:-6]

        # Загрузка данным в мета таблицу, если она пустая
        sql_query = '''
		insert into deaian.ptrc_meta_loads ( 
											schema_name
											,table_name
											,max_update_dt
											,processed_dt 
           								)
		select 
				'deaian'
				,'ptrc_rep_fraud' 
				,to_date ('1900-01-01','YYYY-MM-DD')
				,now()
		where not exists ( select schema_name, table_name from deaian.ptrc_meta_loads 
							where schema_name = 'deaian' and table_name = 'ptrc_rep_fraud'
						);
		'''
        cursor.execute(sql_query)

        cur_date = cur_date[-4:] + '-' + cur_date[-6:-4] + '-' + cur_date[:-6]

        # Заполнение временной таблицы для отчета
        sql_query = f'''insert into deaian.ptrc_stg_rep_fraud_tmp (
										trans_id
										,event_dt
										,passport
										,fio
										,phone
										,passport_valid_to
										,acc_valid_to
										,acc_deleted_flg
										,processed_dt
									)
		select
			pdft.trans_id
			,pdft.trans_date
			,pddclh.passport_num
			,rtrim(concat(pddclh.last_name, ' ', pddclh.first_name, ' ', pddclh.patronymic))
			,pddclh.phone
			,pddclh.passport_valid_to
			,pddah.valid_to
			,pddah.deleted_flg
			,now()
		from deaian.ptrc_dwh_fact_tracnsactions as pdft
		left join deaian.ptrc_dwh_dim_cards_hist as pddch on pdft.card_num = pddch.card_num
			and pddch.deleted_flg = 'N' and pdft.trans_date between pddch.effective_from and pddch.effective_to 
		left join deaian.ptrc_dwh_dim_accounts_hist as pddah on pddch.account_num = pddah.account_num
			and pdft.trans_date between pddah.effective_from and pddah.effective_to
		left join deaian.ptrc_dwh_dim_clients_hist as pddclh on pddah.client = pddclh.client_id
			and pddclh.deleted_flg = 'N' and pdft.trans_date between pddclh.effective_from and pddclh.effective_to
		where cast(pdft.trans_date as date) = cast('{cur_date}' as date) and pdft.oper_result = 'SUCCESS';
		'''
        cursor.execute(sql_query)

        # 1 тип мошенничества
        sql_query = f'''
		insert into deaian.ptrc_rep_fraud (
											event_dt
											,passport
											,fio
											,phone
											,event_type
											,report_dt
											,processed_dt
		)
		select
			psrft.event_dt
			,psrft.passport
			,psrft.fio
			,psrft.phone
			,1
			,cast('{cur_date}' as date)
			,now()
		from deaian.ptrc_stg_rep_fraud_tmp as psrft
		left join deaian.ptrc_dwh_fact_passport_blacklist as pdfpb
		on psrft.passport = pdfpb.passport_num and pdfpb.entry_dt <= cast('{cur_date}' as date)
		where 1=1
			and (pdfpb.passport_num is not null
				or coalesce(psrft.passport_valid_to, cast('{cur_date}' as date)) < cast('{cur_date}' as date)
			);
		'''
        cursor.execute(sql_query)

        # 2 тип мошенничества
        sql_query = f'''
		insert into deaian.ptrc_rep_fraud (
											event_dt
											,passport
											,fio
											,phone
											,event_type
											,report_dt
											,processed_dt
		)
		select
			psrft.event_dt
			,psrft.passport
			,psrft.fio
			,psrft.phone
			,2
			,cast('{cur_date}' as date)
			,now()
		from deaian.ptrc_stg_rep_fraud_tmp as psrft
		where 1=1
			and (coalesce(acc_valid_to, cast('{cur_date}' as date)) < cast('{cur_date}' as date)
				or acc_deleted_flg = 'Y'
			);  
		'''
        cursor.execute(sql_query)

        # 3 тип мошенничества
        sql_query = f'''
		insert into deaian.ptrc_rep_fraud (
											event_dt
											,passport
											,fio
											,phone
											,event_type
											,report_dt
											,processed_dt
		)
		select
			psrft.event_dt
			,psrft.passport
			,psrft.fio
			,psrft.phone
			,3
			,cast('{cur_date}' as date)
			,now()
		from deaian.ptrc_stg_rep_fraud_tmp as psrft 
		where psrft.trans_id in  (
					select t1.trans_id 
					from deaian.ptrc_dwh_fact_tracnsactions as t1
					left join deaian.ptrc_dwh_fact_tracnsactions as t2
					on t1.card_num = t2.card_num
						and extract(epoch from t1.trans_date) - extract(epoch from t2.trans_date) between 1 and 3600
					left join deaian.ptrc_dwh_dim_terminals_hist as t3 on t2.terminal = t3.terminal_id
						and t3.deleted_flg = 'N' and t2.trans_date between t3.effective_from and t3.effective_to  
					where 1=1
						and cast(t1.trans_date as date) = cast('{cur_date}' as date)
						and t1.oper_result = 'SUCCESS' and t2.oper_result = 'SUCCESS'
					group by t1.trans_id
					having count(distinct t3.terminal_city) > 1
				);   
		'''
        cursor.execute(sql_query)

        # 4 тип мошенничества
        sql_query = f'''
		insert into deaian.ptrc_rep_fraud (
											event_dt
											,passport
											,fio
											,phone
											,event_type
											,report_dt
											,processed_dt
		)
		select
			psrft.event_dt
			,psrft.passport
			,psrft.fio
			,psrft.phone
			,4
			,cast('{cur_date}' as date)
			,now()
		from deaian.ptrc_stg_rep_fraud_tmp as psrft 
		where psrft.trans_id in  (
									select trans_id
									from (
										select 
												t1.trans_id
												,t1.amt as amt1
												,t2.oper_result
												,t2.amt as amt2
												,t2.trans_date
												,coalesce(lag(t2.amt) over (partition by t1.trans_id order by t2.trans_date), t2.amt + 1) as prev_amt
												,row_number() over (partition by t1.trans_id order by t2.trans_date desc) as nn
												,count(*) over (partition by t1.trans_id) as cnt
											from deaian.ptrc_dwh_fact_tracnsactions as t1
											left join deaian.ptrc_dwh_fact_tracnsactions as t2
											on t1.card_num = t2.card_num
												and extract(epoch from t1.trans_date) - extract(epoch from t2.trans_date) between 1 and 1200
											where cast(t1.trans_date as date) = cast('{cur_date}' as date)
												and t1.oper_result = 'SUCCESS'
										) t3
									where t3.cnt >= 3 and t3.nn <= 3
									group by t3.trans_id, t3.amt1
									having sum(case when t3.oper_result = 'SUCCESS' or t3.prev_amt - t3.amt2 <= 0 then 1 else 0 end) = 0
											and min(t3.amt2) > t3.amt1
								);
		'''
        cursor.execute(sql_query)

        # Сохраняем состояние загрузки в метаданные.
        sql_query = '''
		update deaian.ptrc_meta_loads
		set max_update_dt = (select max( report_dt ) from deaian.ptrc_rep_fraud)
		,processed_dt = now()
		where schema_name = 'deaian' and table_name = 'ptrc_rep_fraud';
		'''
        cursor.execute(sql_query)

        print(f'Отчет сформирован на {cur_date}')

    except:
        print(f'Ошибка формирования отчета на {cur_date}')
