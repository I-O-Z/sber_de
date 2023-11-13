#!/usr/bin/python3
import psycopg2
import pandas as pd
import os
import glob

from py_scripts import scd, file_read, conn, sql_query

# Создание подключения 
print('creating connections')
conn_bank = conn.create_bank(psycopg2)
conn_edu = conn.create_edu(psycopg2)
# Закрываю созданное подключение если в блоках try функций create_bank и create_edu вызывались исключения
if conn_bank == 'Eror' and conn_edu != 'Eror':
    conn_edu.close()
    print('failed to create connections bank!')
elif conn_edu == 'Eror' and conn_bank != 'Eror':
    conn_bank.close()
    print('failed to create connections edu!')
elif conn_edu == 'Eror' and conn_bank == 'Eror':
    print('failed to create connections edu and bank!')
else:
    # Отключение автокоммита
    conn_bank.autocommit = False
    conn_edu.autocommit = False
    # Создание курсора
    cursor_bank = conn_bank.cursor()
    cursor_edu = conn_edu.cursor()
    ####################################################################################
    # поиск файлов
    files = file_read.get_sort_list_files(file_read.get_list_files(glob))
    #загрузка файлов в таблици фактов
    file_read.process_file_passport(files, pd, cursor_edu, os)
    file_read.process_file_transactions(files, pd, cursor_edu, os)
    ###################################################################################
    #Загрузка файловтерминалов в формате scd2
    # обьявления переменных для таблицы терминалов
    colum_names = ('terminal_id', 'terminal_type', 'terminal_city', 'terminal_address', 'create_dt', 'update_dt') # список всех столбцов   
    stg_columns = ('terminal_id', 'terminal_type', 'terminal_city', 'terminal_address') #список не технических столбцов
    tgt_columns = ('terminal_id', 'terminal_type', 'terminal_city', 'terminal_address') #список не технических столбцов
    stg_columns_of_values = ('terminal_type', 'terminal_city', 'terminal_address') #изменяющиесь значения
    tgt_columns_of_values = ('terminal_type', 'terminal_city', 'terminal_address') #изменяющиесь значения
    stg_name = 'zhii_stg_terminals'
    sourse_name = stg_name
    stg_key = 'terminal_id'
    delete_name = 'zhii_stg_terminals_del'
    tgt_name = 'zhii_dwh_dim_terminals_hist'
    tgt_key = 'terminal_id'

    meta_dt = scd.get_meta_dt(stg_name, cursor_edu)
    count = 0

    for file in files[1]:   
        ddt = file[14:18] + '-' + file[12:14] + '-' + file[10:12] #дата файла
        ddt = f"to_timestamp('{ddt}','YYYY-MM-DD')"
        # 1. Очистка стейджинговых таблиц
        cursor_edu.execute(f"delete from deaise.{stg_name};")
        cursor_edu.execute(f"delete from deaise.{delete_name};")

        # 2. Захват в стейджинг данных из источника полным срезом. 
        file_read.terminal_to_stg(file, pd, cursor_edu)    
        
        # 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.                            
        scd.get_keys_to_del(stg_key, sourse_name, delete_name, cursor_edu, cursor_edu, scheme='deaise')                           
                                    
        # 4. Загрузка в приемник "вставок" на источнике (формат SCD2)
        scd.load_inserts(tgt_name, stg_name, tgt_key, stg_key, stg_columns, tgt_columns, cursor_edu)

        # 5. Обновление в приемнике "обновлений" на источнике (формат SCD2).
        scd.load_updates(tgt_name, stg_name, tgt_key, stg_key, stg_columns, tgt_columns, stg_columns_of_values, tgt_columns_of_values, cursor_edu)

        # 6. Обработка удалений в приемнике (формат SCD2).    
        scd.proces_deletion(tgt_name, delete_name, tgt_columns, tgt_key, stg_key, cursor_edu, del_dt=ddt)
         
        # 7. Обновление метаданных.
        scd.update_meta(stg_name, cursor_edu)                      
        
        os.rename(file, f'archive/{file}.backup')        
        count += 1
    print(f'{count} terminal files processed')
    ####################################################################################
    # обьявления переменных для таблицы карты
    colum_names = ('card_num', 'account', 'create_dt', 'update_dt') # список всех столбцов   
    stg_columns = ('card_num', 'account') #список не технических столбцов
    tgt_columns = ('card_num', 'account_num') #список не технических столбцов
    stg_columns_of_values = ('account',) #изменяющиесь значения
    tgt_columns_of_values = ('account_num',) #изменяющиесь значения
    stg_name = 'zhii_stg_cards'
    sourse_name = 'cards'
    stg_key = 'card_num'
    delete_name = 'zhii_stg_cards_del'
    tgt_name = 'zhii_dwh_dim_cards_hist'
    tgt_key = 'card_num'

    meta_dt = scd.get_meta_dt(stg_name, cursor_edu)

    # 1. Очистка стейджинговых таблиц
    cursor_edu.execute(f"delete from deaise.{stg_name};")
    cursor_edu.execute(f"delete from deaise.{delete_name};")

    # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг                                                      
    scd.get_sourse_date(colum_names, sourse_name, stg_name, cursor_edu, cursor_bank, meta_dt)                          
                            
    # 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.                            
    scd.get_keys_to_del(stg_key, sourse_name, delete_name, cursor_edu, cursor_bank)                           
                                
    # 4. Загрузка в приемник "вставок" на источнике (формат SCD2)
    scd.load_inserts(tgt_name, stg_name, tgt_key, stg_key, stg_columns, tgt_columns, cursor_edu)

    # 5. Обновление в приемнике "обновлений" на источнике (формат SCD2).
    scd.load_updates(tgt_name, stg_name, tgt_key, stg_key, stg_columns, tgt_columns, stg_columns_of_values, tgt_columns_of_values, cursor_edu)

    # 6. Обработка удалений в приемнике (формат SCD2).    
    scd.proces_deletion(tgt_name, delete_name, tgt_columns, tgt_key, stg_key, cursor_edu)
     
    # 7. Обновление метаданных.
    scd.update_meta(stg_name, cursor_edu)                      
    ####################################################################################
    # обьявления переменных для таблицы акаунты
    colum_names = ('account', 'valid_to', 'client','create_dt', 'update_dt') # список всех столбцов   
    stg_columns = ('account', 'valid_to', 'client') #список не технических столбцов
    tgt_columns = ('account_num', 'valid_to', 'client') #список не технических столбцов
    stg_columns_of_values = ('valid_to', 'client') #изменяющиесь значения
    tgt_columns_of_values = ('valid_to', 'client') #изменяющиесь значения
    stg_name = 'zhii_stg_accounts'
    sourse_name = 'accounts'
    stg_key = 'account'
    tgt_key = 'account_num'
    delete_name = 'zhii_stg_accounts_del'
    tgt_name = 'zhii_dwh_dim_accounts_hist'

    meta_dt = scd.get_meta_dt(stg_name, cursor_edu)

    # 1. Очистка стейджинговых таблиц
    cursor_edu.execute(f"delete from deaise.{stg_name};")
    cursor_edu.execute(f"delete from deaise.{delete_name};")

    # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг                                                      
    scd.get_sourse_date(colum_names, sourse_name, stg_name, cursor_edu, cursor_bank, meta_dt)                          
                            
    # 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.                            
    scd.get_keys_to_del(stg_key, sourse_name, delete_name, cursor_edu, cursor_bank)                           
                                
    # 4. Загрузка в приемник "вставок" на источнике (формат SCD2)
    scd.load_inserts(tgt_name, stg_name, tgt_key, stg_key, stg_columns, tgt_columns, cursor_edu)

    # 5. Обновление в приемнике "обновлений" на источнике (формат SCD2).
    scd.load_updates(tgt_name, stg_name, tgt_key, stg_key, stg_columns, tgt_columns, stg_columns_of_values, tgt_columns_of_values, cursor_edu)

    # 6. Обработка удалений в приемнике (формат SCD2).    
    scd.proces_deletion(tgt_name, delete_name, tgt_columns, tgt_key, stg_key, cursor_edu)
     
    # 7. Обновление метаданных.
    scd.update_meta(stg_name, cursor_edu)                      
    ####################################################################################
    # обьявления переменных для таблицы клиентов
    colum_names = ('client_id', 'last_name', 'first_name', 'patronymic', 'date_of_birth', 'passport_num', 'passport_valid_to', 'phone', 'create_dt', 'update_dt') # список всех столбцов   
    stg_columns = ('client_id', 'last_name', 'first_name', 'patronymic', 'date_of_birth', 'passport_num', 'passport_valid_to', 'phone') #список не технических столбцов
    tgt_columns = ('client_id', 'last_name', 'first_name', 'patronymic', 'date_of_birth', 'passport_num', 'passport_valid_to', 'phone') #список не технических столбцов
    stg_columns_of_values = ('last_name', 'first_name', 'patronymic', 'date_of_birth', 'passport_num', 'passport_valid_to', 'phone') #изменяющиесь значения
    tgt_columns_of_values = ('last_name', 'first_name', 'patronymic', 'date_of_birth', 'passport_num', 'passport_valid_to', 'phone') #изменяющиесь значения
    stg_name = 'zhii_stg_clients'
    sourse_name = 'clients'
    stg_key = 'client_id'
    tgt_key = 'client_id'
    delete_name = 'zhii_stg_clients_del'
    tgt_name = 'zhii_dwh_dim_clients_hist'

    meta_dt = scd.get_meta_dt(stg_name, cursor_edu)

    # 1. Очистка стейджинговых таблиц
    cursor_edu.execute(f"delete from deaise.{stg_name};")
    cursor_edu.execute(f"delete from deaise.{delete_name};")

    # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг                                                      
    scd.get_sourse_date(colum_names, sourse_name, stg_name, cursor_edu, cursor_bank, meta_dt)                          
                            
    # 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.                            
    scd.get_keys_to_del(stg_key, sourse_name, delete_name, cursor_edu, cursor_bank)                           
                                
    # 4. Загрузка в приемник "вставок" на источнике (формат SCD2)
    scd.load_inserts(tgt_name, stg_name, tgt_key, stg_key, stg_columns, tgt_columns, cursor_edu)

    # 5. Обновление в приемнике "обновлений" на источнике (формат SCD2).
    scd.load_updates(tgt_name, stg_name, tgt_key, stg_key, stg_columns, tgt_columns, stg_columns_of_values, tgt_columns_of_values, cursor_edu)

    # 6. Обработка удалений в приемнике (формат SCD2).    
    scd.proces_deletion(tgt_name, delete_name, tgt_columns, tgt_key, stg_key, cursor_edu)
     
    # 7. Обновление метаданных.
    scd.update_meta(stg_name, cursor_edu)
                         
    ####################################################################################
    # Заполнение витрины
    cursor_edu.execute(sql_query.fraud_search_request)
    print('loading to the fraud table is completed')
    # Закрываем соединение
    conn_edu.commit()
    print('closing connections')
    cursor_bank.close()
    cursor_edu.close()
    #cursor_bank.close()
    conn_edu.close()
    conn_bank.close()
