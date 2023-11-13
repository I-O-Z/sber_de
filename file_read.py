def get_list_files(glob):
    """
    Возвращает список файлов
    """
    files = []
    files.extend(glob.glob('passport_blacklist_*.xlsx'))
    files.extend(glob.glob('transactions_*.txt'))
    files.extend(glob.glob('terminals_*.xlsx'))
    return files

def get_sort_list_files(files):
    """
    Разбивает список на три отсортированных списка
    """
    terminals, transactions, passport = [], [], []
    for file in files:
        if 'passport' in file:
            passport.append(file)
        if 'terminals' in file:
            terminals.append(file)
        if 'transactions' in file:
            transactions.append(file)
    return (sorted(passport, key=lambda a: a[23:27] + a[21:23] + a[19:21]),
            sorted(terminals, key=lambda a: a[14:18] + a[12:14] + a[10:12]),
            sorted(transactions, key=lambda a: a[17:21] + a[15:17] + a[13:15]))


def terminal_to_stg(file_name, pd, cursor_edu):                          
    """
    Читает файл терминалов(с добавлением технических полей), пишет в stg
    """   
    
    df_terminal = pd.read_excel(f'{file_name}', sheet_name='terminals', header=0, index_col=None)
    df_terminal['create_dt'] = file_name[14:18] + '-' + file_name[12:14] + '-' + file_name[10:12]
    df_terminal['update_dt'] = file_name[14:18] + '-' + file_name[12:14] + '-' + file_name[10:12]
    cursor_edu.execute("delete from deaise.zhii_stg_terminals;")    
    cursor_edu.executemany("""INSERT INTO deaise.zhii_stg_terminals(
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address,
                                create_dt,
                                update_dt)
                              VALUES( %s, %s, %s, %s, %s, %s)""", df_terminal.values.tolist() )  


def passport_to_stg(file_name, pd, cursor_edu):                          
    """
    Читает файл паспорта, пишет в stg
    """    
    df_passport = pd.read_excel(f'{file_name}', sheet_name='blacklist', header=0, index_col=None) 
    cursor_edu.execute("delete from deaise.zhii_stg_blacklist;")
    cursor_edu.executemany("""INSERT INTO deaise.zhii_stg_blacklist(
                                date,
                                passport)
                              VALUES( %s, %s)""", df_passport.values.tolist()) 


def passport_to_fact(cursor_edu):
    """
    Заполняет deaise.zhii_dwh_fact_passport_blacklist 
    """
    cursor_edu.execute( """INSERT INTO deaise.zhii_dwh_fact_passport_blacklist(
                                passport_num,
                                entry_dt)
                            select 
                                passport,
                                cast(date as date)                              
                            from deaise.zhii_stg_blacklist zsb left join
                                deaise.zhii_dwh_fact_passport_blacklist zdfpb
                                on zsb.passport = zdfpb.passport_num 
                                    where zdfpb.passport_num is null""")  
    print('loading to the passport_blacklist table is completed')


def transactions_to_stg(file_name, pd, cursor_edu):                          
    """
    Читает файл транзакций, пишет в stg
    """   
    df_transactions = pd.read_csv(f'{file_name}', sep = ';')
    cursor_edu.execute("delete from deaise.zhii_stg_transactions;")
    cursor_edu.executemany("""INSERT INTO deaise.zhii_stg_transactions(
                                transaction_id,
                                transaction_date,
                                amount,
                                card_num,
                                oper_type,
                                oper_result,
                                terminal)
                              VALUES( %s, %s, %s, %s, %s, %s, %s )""", df_transactions.values.tolist() )

def transactions_to_fact(cursor_edu):
    """
    Заполняет deaise.zhii_dwh_fact_transactions 
    """
    cursor_edu.execute( """ INSERT INTO deaise.zhii_dwh_fact_transactions(
                                trans_id,
                                trans_date,
                                card_num,
                                oper_type,
                                amt,
                                oper_result,
                                terminal)
                            Select   
                                transaction_id,
                                cast(transaction_date as timestamp),
                                zst.card_num,
                                zst.oper_type,                            
                                CAST(replace(replace(amount, '.', ''), ',', '.') AS DECIMAL(9,2)),
                                zst.oper_result,
                                zst.terminal
                            from deaise.zhii_stg_transactions zst left join
                                deaise.zhii_dwh_fact_transactions zdft
                                on zst.transaction_id = zdft.trans_id
                                    where zdft.trans_id is null""")  
    print('loading to the transactions table is completed')



def process_file_passport(files, pd, cursor_edu, os):
    """
    Обработка файла(ов) паспортов
    """
    count = 0
    for file in files[0]:
        passport_to_stg(file, pd, cursor_edu)
        passport_to_fact(cursor_edu)
        os.rename(file, f'archive/{file}.backup')
        count += 1
    print(f'{count} passport_blacklist files processed')
    
    
    
def process_file_transactions(files, pd, cursor_edu, os):
    """
    Обработка файла(ов) транзакций
    """
    count = 0
    for file in files[2]:
        transactions_to_stg(file, pd, cursor_edu)
        transactions_to_fact(cursor_edu)
        os.rename(file, f'archive/{file}.backup')
        count += 1
    print(f'{count} transaction files processed')
