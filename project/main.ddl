--Таблицы для размещения стейджинговых таблиц (первоначальная загрузка)
--
create table if not exists deaian.ptrc_stg_transactions( 
    trans_id varchar(11)
    ,trans_date timestamp(0)
    ,card_num varchar(20)
    ,oper_type varchar(8)
    ,amt decimal(12,2)
    ,oper_result varchar(8)
    ,terminal varchar(6)
    ,update_dt date
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_terminals ( 
    terminal_id varchar(6)
    ,terminal_type varchar(3)
    ,terminal_city varchar(50)
    ,terminal_address varchar(200)
    ,update_dt date
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_blacklist (
    passport_num varchar(20)
    ,entry_dt date
    ,update_dt date
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_cards (
    card_num varchar(20)
    ,account_num varchar(20)
    ,create_dt timestamp(0)
    ,update_dt date
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_accounts (
    account_num varchar(20)
    ,valid_to date
    ,client varchar(10)
    ,create_dt timestamp(0)
    ,update_dt date
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_clients (
    client_id varchar(10)
    ,last_name varchar(20)
    ,first_name varchar(20)
    ,patronymic varchar(20)
    ,date_of_birth date
    ,passport_num varchar(15)
    ,passport_valid_to date
    ,phone varchar(16)
    ,create_dt timestamp(0)
    ,update_dt date
    ,processed_dt timestamp(0)
);

--
create table if not exists deaian.ptrc_stg_terminals_del ( 
    terminal_id varchar(6)
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_cards_del (
    card_num varchar(20)
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_accounts_del (
    account_num varchar(20)
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_clients_del (
    client_id varchar(10)
    ,processed_dt timestamp(0)
);

--Таблицы фактов, загруженных в хранилище.
--
create table if not exists deaian.ptrc_dwh_fact_tracnsactions (
    trans_id varchar(11)
    ,trans_date timestamp(0)
    ,card_num varchar(20)
    ,oper_type varchar(8)
    ,amt decimal(12,2)
    ,oper_result varchar(8)
    ,terminal varchar(6)
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_dwh_fact_passport_blacklist (
    passport_num varchar(15)
    ,entry_dt date
    ,processed_dt timestamp(0)
);


--Таблицы измерений, хранящиеся в SCD2 формате
--
create table if not exists deaian.ptrc_dwh_dim_terminals_hist ( 
    terminal_id varchar(6)
    ,terminal_type varchar(3)
    ,terminal_city varchar(50)
    ,terminal_address varchar(200)
    ,effective_from timestamp(0)
    ,effective_to timestamp(0) default to_timestamp('9999-12-31', 'YYYY-MM-DD')
    ,deleted_flg char(1) default 'N'
    ,processed_dt timestamp(0)
);

--
create table if not exists deaian.ptrc_dwh_dim_cards_hist (
    card_num varchar(20)
    ,account_num varchar(20)
    ,effective_from timestamp(0)
    ,effective_to timestamp(0) default to_timestamp('9999-12-31', 'YYYY-MM-DD')
    ,deleted_flg char(1) default 'N'
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_dwh_dim_accounts_hist (
    account_num varchar(20)
    ,valid_to date
    ,client varchar(10)
    ,effective_from timestamp(0)
    ,effective_to timestamp(0) default to_timestamp('9999-12-31', 'YYYY-MM-DD')
    ,deleted_flg char(1) default 'N'
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_dwh_dim_clients_hist (
    client_id varchar(10)
    ,last_name varchar(20)
    ,first_name varchar(20)
    ,patronymic varchar(20)
    ,date_of_birth date
    ,passport_num varchar(15)
    ,passport_valid_to date
    ,phone varchar(16)
    ,effective_from timestamp(0)
    ,effective_to timestamp(0) default to_timestamp('9999-12-31', 'YYYY-MM-DD')
    ,deleted_flg char(1) default 'N'
    ,processed_dt timestamp(0)
);

--Таблица с отчетом
create table if not exists deaian.ptrc_rep_fraud (
    event_dt timestamp(0) 
    ,passport varchar(15)
    ,fio varchar(62)
    ,phone varchar(16)
    ,event_type smallint
    ,report_dt date
    ,processed_dt timestamp(0)
);

--Таблица для хранения метаданных.
create table if not exists deaian.ptrc_meta_loads (
    schema_name varchar(30)
    ,table_name varchar(50)
    ,max_update_dt timestamp(0)
    ,processed_dt timestamp(0)
);

--Временные таблицы для работы со стейджинговыми таблицами
--
create table if not exists deaian.ptrc_stg_transactions_tmp( 
    trans_id varchar(11)
    ,trans_date timestamp(0)
    ,card_num varchar(20)
    ,oper_type varchar(8)
    ,amt decimal(12,2)
    ,oper_result varchar(8)
    ,terminal varchar(6)
    ,update_dt date
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_terminals_tmp ( 
    terminal_id varchar(6)
    ,terminal_type varchar(3)
    ,terminal_city varchar(50)
    ,terminal_address varchar(200)
    ,update_dt date
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_blacklist_tmp (
    passport_num varchar(20)
    ,entry_dt date
    ,update_dt date
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_cards_tmp (
    card_num varchar(20)
    ,account_num varchar(20)
    ,create_dt timestamp(0)
    ,update_dt date
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_accounts_tmp (
    account_num varchar(20)
    ,valid_to date
    ,client varchar(10)
    ,create_dt timestamp(0)
    ,update_dt date
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_clients_tmp (
    client_id varchar(10)
    ,last_name varchar(20)
    ,first_name varchar(20)
    ,patronymic varchar(20)
    ,date_of_birth date
    ,passport_num varchar(15)
    ,passport_valid_to date
    ,phone varchar(16)
    ,create_dt timestamp(0)
    ,update_dt date
    ,processed_dt timestamp(0)
);
--
create table if not exists deaian.ptrc_stg_rep_fraud_tmp (
    trans_id varchar(11)
    ,event_dt timestamp(0)
    ,passport varchar(15)
    ,fio varchar(62)
    ,phone varchar(16)
    ,passport_valid_to date
    ,acc_valid_to date
    ,acc_deleted_flg char(1)
    ,processed_dt timestamp(0)
);