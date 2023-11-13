drop table deaise.zhii_stg_transactions;	 
drop table deaise.zhii_stg_terminals;	 
drop table deaise.zhii_stg_blacklist;	 
drop table deaise.zhii_stg_cards;	 
drop table deaise.zhii_stg_accounts;	 
drop table deaise.zhii_stg_clients;
drop table deaise.zhii_dwh_fact_transactions;
drop table deaise.zhii_dwh_fact_passport_blacklist;
drop table deaise.zhii_dwh_dim_terminals_hist;
drop table deaise.zhii_dwh_dim_cards_hist;
drop table deaise.zhii_dwh_dim_accounts_hist;
drop table deaise.zhii_dwh_dim_clients_hist;
drop table deaise.zhii_rep_fraud;
drop table deaise.zhii_stg_terminals_del;
drop table deaise.zhii_stg_cards_del;
drop table deaise.zhii_stg_accounts_del;
drop table deaise.zhii_stg_clients_del;
drop table deaise.zhii_meta_update_dt;


delete from deaise.zhii_stg_transactions;	 
delete from deaise.zhii_stg_terminals;	 
delete from deaise.zhii_stg_blacklist;	 
delete from deaise.zhii_stg_cards; 
delete from deaise.zhii_stg_accounts;	 
delete from deaise.zhii_stg_clients;
delete from deaise.zhii_dwh_fact_transactions;
delete from deaise.zhii_dwh_fact_passport_blacklist;
delete from deaise.zhii_dwh_dim_terminals_hist;
delete from deaise.zhii_dwh_dim_cards_hist;
delete from deaise.zhii_dwh_dim_accounts_hist;
delete from deaise.zhii_dwh_dim_clients_hist;
delete from deaise.zhii_rep_fraud;


delete from deaise.zhii_stg_terminals_del;
delete from deaise.zhii_dwh_dim_terminals_hist;
delete from deaise.zhii_stg_terminals;	
delete from deaise.zhii_meta_update_dt ;

insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_terminals', to_timestamp('1900-01-01','YYYY-MM-DD') );
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_terminals', to_timestamp('1900-01-01','YYYY-MM-DD') );
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_terminals', to_timestamp('1900-01-01','YYYY-MM-DD') );
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_terminals', to_timestamp('1900-01-01','YYYY-MM-DD') );
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_terminals', to_timestamp('1900-01-01','YYYY-MM-DD') );
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_terminals', to_timestamp('1900-01-01','YYYY-MM-DD') );

select to_timestamp('2021-03-01','YYYY-MM-DD');

select * from deaise.zhii_stg_transactions;	 
select * from deaise.zhii_stg_terminals;	 
select * from deaise.zhii_stg_blacklist;	 
select * from deaise.zhii_stg_cards; 
select * from deaise.zhii_stg_accounts;	 
select * from deaise.zhii_stg_clients where 1=0;
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'zhii_stg_clients';
select * from deaise.zhii_dwh_fact_transactions;
select * from deaise.zhii_dwh_fact_passport_blacklist;
select * from deaise.zhii_dwh_dim_terminals_hist;
select * from deaise.zhii_dwh_dim_cards_hist;
select * from deaise.zhii_dwh_dim_accounts_hist;
select * from deaise.zhii_dwh_dim_clients_hist;
select * from deaise.zhii_rep_fraud;
select * from deaise.zhii_meta_update_dt;


create table deaise.zhii_stg_terminals_del(
	terminal_id varchar(10)
);
create table deaise.zhii_stg_cards_del(
	card_num varchar(20)
);
create table deaise.zhii_stg_accounts_del(
	account varchar(20)
);
create table deaise.zhii_stg_clients_del(
	client_id varchar(10)
);

create table deaise.zhii_meta_update_dt(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

drop table deaise.zhii_stg_terminals

create table if not exists deaise.zhii_stg_terminals(
	terminal_id varchar(10) NULL,
	terminal_type varchar(50) NULL,
	terminal_city varchar(50) NULL,
	terminal_address varchar(100) NULL,
	create_dt timestamp(0) NULL,
	update_dt timestamp(0) NULL
);
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_terminals', to_timestamp('1900-01-01','YYYY-MM-DD') );

select * from deaise.zhii_meta_update_dt;
                        
                        
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_cards', to_timestamp('1800-01-01','YYYY-MM-DD') );
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_accounts', to_timestamp('1800-01-01','YYYY-MM-DD') );
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_clients', to_timestamp('1800-01-01','YYYY-MM-DD') );
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_terminals', to_timestamp('1800-01-01','YYYY-MM-DD') );

----------------------------------------------------------
create table deaise.zhii_stg_terminals_del(
	terminal_id varchar(10)
);
create table deaise.zhii_stg_cards_del(
	card_num varchar(20)
);
create table deaise.zhii_stg_accounts_del(
	account varchar(20)
);
create table deaise.zhii_stg_clients_del(
	client_id varchar(10)
);
----------------------------------------------------------
create table deaise.zhii_meta_update_dt(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

create table if not exists deaise.zhii_stg_transactions(
	transaction_id varchar(11),
	transaction_date varchar(20),
	amount varchar(10),
	card_num varchar(20),
	oper_type varchar(10),
	oper_result varchar(10),
	terminal varchar(10)
);
create table if not exists deaise.zhii_stg_terminals(
	terminal_id varchar(10) NULL,
	terminal_type varchar(50) NULL,
	terminal_city varchar(50) NULL,
	terminal_address varchar(100) NULL,
	create_dt timestamp(0) NULL,
	update_dt timestamp(0) NULL
);
create table if not exists deaise.zhii_stg_blacklist(
	date varchar(20),
	passport varchar(11)
);
create table if not exists deaise.zhii_stg_cards(
	card_num varchar(20) NULL,
	account varchar(20) NULL,
	create_dt timestamp(0) NULL,
	update_dt timestamp(0) NULL
);
create table if not exists deaise.zhii_stg_accounts(
	account varchar(20) NULL,
	valid_to date NULL,
	client varchar(10) NULL,
	create_dt timestamp(0) NULL,
	update_dt timestamp(0) NULL
);
create table if not exists deaise.zhii_stg_clients(
	client_id varchar(10) NULL,
	last_name varchar(20) NULL,
	first_name varchar(20) NULL,
	patronymic varchar(20) NULL,
	date_of_birth date NULL,
	passport_num varchar(15) NULL,
	passport_valid_to date NULL,
	phone varchar(16) NULL,
	create_dt timestamp(0) NULL,
	update_dt timestamp(0) NULL
);
create table if not exists deaise.zhii_dwh_fact_transactions(
	trans_id varchar(11),
	trans_date timestamp(0),
	card_num varchar(20),
	oper_type varchar(10),
	amt decimal(9, 2),	
	oper_result varchar(10),
	terminal varchar(10)
);
create table if not exists deaise.zhii_dwh_fact_passport_blacklist(
	passport_num varchar(11) NULL,
	entry_dt date NULL
);
create table if not exists deaise.zhii_dwh_dim_terminals_hist(
	terminal_id varchar(10) NULL,
	terminal_type varchar(50) NULL,
	terminal_city varchar(50) NULL,
	terminal_address varchar(100) NULL,
	effective_from timestamp(0) NULL, 
	effective_to timestamp(0) NULL,
	deleted_flg char
);
create table if not exists deaise.zhii_dwh_dim_cards_hist(
	card_num varchar(20) NULL,
	account_num varchar(20) NULL,
	effective_from timestamp(0) NULL, 
	effective_to timestamp(0) NULL,
	deleted_flg char
);
create table if not exists deaise.zhii_dwh_dim_accounts_hist(
	account_num varchar(20) NULL,
	valid_to date NULL,
	client varchar(10) NULL,
	effective_from timestamp(0) NULL, 
	effective_to timestamp(0) NULL,
	deleted_flg char
);
create table if not exists deaise.zhii_dwh_dim_clients_hist(
	client_id varchar(10) NULL,
	last_name varchar(20) NULL,
	first_name varchar(20) NULL,
	patronymic varchar(20) NULL,
	date_of_birth date NULL,
	passport_num varchar(15) NULL,
	passport_valid_to date NULL,
	phone varchar(16) NULL,
	effective_from timestamp(0) NULL, 
	effective_to timestamp(0) NULL,
	deleted_flg char
);
create table if not exists deaise.zhii_rep_fraud(
	event_dt effective_from timestamp(0),
	passport varchar(15),
	fio varchar(63),
	phone varchar(16),
	event_type char,
	report_dt timestamp(0);
	
----------------------------------------------------------
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_cards', to_timestamp('1900-01-01','YYYY-MM-DD') );
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_accounts', to_timestamp('1900-01-01','YYYY-MM-DD') );
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_clients', to_timestamp('1900-01-01','YYYY-MM-DD') );
insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
values( 'deaise','zhii_stg_terminals', to_timestamp('1900-01-01','YYYY-MM-DD') );
----------------------------------------------------------