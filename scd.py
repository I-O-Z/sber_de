def get_meta_dt(table_name, cursor_edu):
    """
    Получение даты последней загрузки    
    """
    cursor_edu.execute(f"""select cast(max_update_dt as varchar) 
                            from deaise.zhii_meta_update_dt where schema_name = 'deaise' and table_name = '{table_name}'""")
    return str(cursor_edu.fetchone()[0])
    
    
def  get_sourse_date(colum_names, sourse_name, stg_name, cursor_edu, cursor_bank, meta_dt):
    """
    Захват данных из источника (измененных с момента последней загрузки) в стейджинг    
    """
    cursor_bank.execute(f""" Select   
                                {', '.join(colum_names)}
                            from info.{sourse_name}                            
                                where cast(coalesce(update_dt, create_dt)as timestamp) > cast('{str(meta_dt)}' as timestamp)""")
    cursor_edu.executemany(f"""INSERT INTO deaise.{stg_name}( 
                                {', '.join(colum_names)})
                            VALUES( {'%s' + ', %s' * (len(colum_names)-1)})""", cursor_bank.fetchall()) 


def get_keys_to_del(key, sourse_name, delete_name, cursor_edu, cursor_bank, scheme = 'info'):
    """
    Захват в стейджинг ключей из источника полным срезом для вычисления удалений.
    """
    cursor_bank.execute(f""" Select
                                {key}
                            from {scheme}.{sourse_name}""")
    cursor_edu.executemany(f"""INSERT INTO deaise.{delete_name}(
                                {key})
                            VALUES( %s)""", cursor_bank.fetchall())


def load_inserts(tgt_name, stg_name, tgt_key, stg_key, stg_columns, tgt_columns, cursor_edu):
    """
    Загрузка в приемник "вставок" на источнике (формат SCD2)
    """
    cursor_edu.execute(f"""insert into deaise.{tgt_name} ( {', '.join(tgt_columns)}, effective_from, effective_to, deleted_flg )
                            select 
                                stg.{', stg.'.join(stg_columns)},
                                coalesce(stg.update_dt, stg.create_dt) as effective_from,
                                to_date('9999-12-31','YYYY-MM-DD') as effective_to,
                                'N' as deleted_flg
                            from deaise.{stg_name} stg
                             left join deaise.{tgt_name} tgt
                             on stg.{stg_key} = tgt.{tgt_key}
                            where tgt.{tgt_key} is null;""")   


def write_condition(tgt_colums, stg_colums):
    """
    Возвращает строку с условием на неравнство переданых значений
    """
    s = '1=0 '
    for t, c in zip(tgt_colums, stg_colums):
        s += f'or ( stg.{c} <> tgt.{t} or ( stg.{c} is null and tgt.{t} is not null) or ( stg.{c} is not null and tgt.{t} is null) )'
    return (s + " or tgt.deleted_flg = 'Y'")                            


def load_updates(tgt_name, stg_name, tgt_key, stg_key, stg_columns, tgt_columns, stg_columns_of_values, tgt_columns_of_values, cursor_edu):
    """
    Обновление в приемнике "обновлений" на источнике (формат SCD2).
    """
    cursor_edu.execute(f"""insert into deaise.{tgt_name} ( {', '.join(tgt_columns)}, effective_from, effective_to, deleted_flg )
                            select 
                                stg.{', stg.'.join(stg_columns)},
                                coalesce(stg.update_dt, stg.create_dt) effective_from,
                                to_date('9999-12-31','YYYY-MM-DD') effective_to,
                                'N' deleted_flg
                            from deaise.{stg_name} stg inner join 
                                 deaise.{tgt_name} tgt
                              on stg.{stg_key} = tgt.{tgt_key}
                             and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
                            where {write_condition(tgt_columns_of_values, stg_columns_of_values)};""")

    cursor_edu.execute(f"""update deaise.{tgt_name} tgt 
                               set effective_to = tmp.update_dt - interval '1 second'
                            from (
                                select 
                                    stg.{', stg.'.join(stg_columns)},
                                    coalesce(stg.update_dt, stg.create_dt) as update_dt
                                from deaise.{stg_name} stg inner join 
                                     deaise.{tgt_name} tgt
                                  on stg.{stg_key} = tgt.{tgt_key}
                                 and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
                                where {write_condition(tgt_columns_of_values, stg_columns_of_values)}) tmp
                            where tgt.{tgt_key} = tmp.{stg_key}
                              and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
                              and ({(write_condition(tgt_columns_of_values, stg_columns_of_values).replace('stg', 'tmp'))[6::]}) ;""")
 

def proces_deletion(tgt_name, delete_name, tgt_columns, tgt_key, stg_key, cursor_edu, del_dt = 'now()'):
    """
    Обработка удалений в приемнике (формат SCD2).
    """
    cursor_edu.execute(f"""insert into deaise.{tgt_name} ( {', '.join(tgt_columns)}, effective_from, effective_to, deleted_flg )
                            select 
                                tgt.{', tgt.'.join(tgt_columns)},
                                {del_dt} effective_from,
                                to_date('9999-12-31','YYYY-MM-DD') effective_to,
                                'Y' deleted_flg
                            from deaise.{tgt_name} tgt left join 
                                 deaise.{delete_name} stg
                              on stg.{stg_key} = tgt.{tgt_key}
                            where stg.{stg_key} is null
                              and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
                              and tgt.deleted_flg = 'N';""")

    cursor_edu.execute(f"""update deaise.{tgt_name} tgt 
                               set effective_to = {del_dt} - interval '1 second'
                            where tgt.{tgt_key} in (
                                select 
                                    tgt.{tgt_key}
                                from deaise.{tgt_name} tgt left join 
                                     deaise.{delete_name} stg
                                  on stg.{stg_key} = tgt.{tgt_key}
                                where stg.{stg_key} is null
                                  and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
                                  and deleted_flg = 'N')
                              and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
                              and deleted_flg = 'N';""")   


def update_meta(stg_name, cursor_edu):
    """
    Обновляет метаданных для таблици 'stg_name'.
    """
    cursor_edu.execute(f"""insert into deaise.zhii_meta_update_dt( schema_name, table_name, max_update_dt )
                        select 
                            'deaise',
                            '{stg_name}', 
                            coalesce((select max(cast(coalesce(update_dt, create_dt)as timestamp)) from deaise.{stg_name}), to_date('1900-01-01','YYYY-MM-DD'))
                        where not exists (select 1 from deaise.zhii_meta_update_dt where schema_name = 'deaise' and table_name = '{stg_name}');""")
     
    cursor_edu.execute(f"""update deaise.zhii_meta_update_dt
                          set max_update_dt = coalesce((select max(cast(coalesce(update_dt, create_dt)as timestamp)) from deaise.{stg_name}), max_update_dt)
                        where schema_name = 'deaise'
                          and table_name = '{stg_name}';""")  
    print(f'loading to the {stg_name[9::]} table is completed')
