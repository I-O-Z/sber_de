fraud_search_request = """
        with bd as (
                select 
                    ft.trans_id,
                    ft.trans_date,
                    ft.card_num,
                    ft.oper_type,
                    ft.amt,
                    ft.oper_result,		
                    hter.terminal_city,
                    hacc.valid_to,
                    concat(hcl.last_name, ' ', hcl.first_name, ' ', hcl.patronymic) as fio,
                    hcl.passport_num,
                    hcl.passport_valid_to,
                    hcl.phone 
                from deaise.zhii_dwh_fact_transactions ft
                    left join deaise.zhii_dwh_dim_terminals_hist hter
                        on ft.terminal = hter.terminal_id
                        and hter.effective_to > trans_date 
                        and hter.effective_from <= trans_date
                        and hter.deleted_flg = 'N'
                    left join deaise.zhii_dwh_dim_cards_hist hcard
                        on trim(ft.card_num) = trim(hcard.card_num)
                        and hcard.effective_to > trans_date
                        and hcard.effective_from <= trans_date
                        and hcard.deleted_flg = 'N'
                    left join deaise.zhii_dwh_dim_accounts_hist hacc
                        on hcard.account_num = hacc.account_num 
                        and hacc.effective_to > trans_date
                        and hacc.effective_from <= trans_date
                        and hacc.deleted_flg = 'N'
                    left join deaise.zhii_dwh_dim_clients_hist hcl 
                        on hacc.client = hcl.client_id
                        and hcl.effective_to > trans_date
                        and hcl.effective_from <= trans_date
                        and hcl.deleted_flg = 'N'), 
            lag_bd as(
                select
                    trans_date,
                    passport_num as passport,
                    fio,
                    phone as phone,
                    '3' as event_type,
                    now() as report_dt,
                    terminal_city,
                    LAG(terminal_city) OVER (PARTITION BY card_num ORDER BY trans_date) lag_city,
                    LAG(trans_date) OVER (PARTITION BY card_num ORDER BY trans_date) lag_date
                from bd), 
            bd_4 as(
                select trans_id, trans_date, card_num, oper_type, oper_result, amt,
                    lag (oper_result) OVER (PARTITION BY card_num ORDER BY trans_date) tr_1,
                    lag(oper_result,2) OVER (PARTITION BY card_num ORDER BY trans_date) tr_2,
                    lag(oper_result,3) OVER (PARTITION BY card_num ORDER BY trans_date) tr_3,
                    lag (amt) OVER (PARTITION BY card_num ORDER BY trans_date) a_1,
                    lag(amt,2) OVER (PARTITION BY card_num ORDER BY trans_date) a_2,
                    lag(amt,3) OVER (PARTITION BY card_num ORDER BY trans_date) a_3,
                    lag(trans_date,3) OVER (PARTITION BY card_num ORDER BY trans_date) dt
                from deaise.zhii_dwh_fact_transactions 
                    where oper_type = 'PAYMENT' or oper_type = 'WITHDRAW')
        insert into deaise.zhii_rep_fraud ( 
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt)
        select 
            trans_date,
            bd.passport_num as passport,
            fio,
            phone as phone,
            '1' as event_type,
            now() as report_dt
        from bd
            where passport_valid_to < trans_date
        union all
        select 
            trans_date,
            bd.passport_num as passport,
            fio,
            phone as phone,
            '1' as event_type,
            now() as report_dt
        from bd
            inner join deaise.zhii_dwh_fact_passport_blacklist fpas
                on bd.passport_num = fpas.passport_num 
                and trans_date >= entry_dt
        union all
        select 
            trans_date,
            bd.passport_num as passport,
            fio,
            phone as phone,
            '2' as event_type,
            now() as report_dt
        from bd
            where valid_to <= trans_date
        union all 
        select 
            trans_date,
            passport,
            fio,
            phone as phone,
            '3' as event_type,
            now() as report_dt 
        from lag_bd
            where 1=1
                and lag_city <> terminal_city 
                and (trans_date - lag_date) < interval '1 hour'
        union all
        select 
            trans_date,
            bd.passport_num as passport,
            fio,
            phone as phone,
            '4' as event_type,
            now() as report_dt
        from bd
            where trans_id in (
                        select 
                            trans_id 
                        from bd_4 
                            where 1=1
                                and oper_result = 'SUCCESS' 
                                and tr_1 = 'REJECT' and tr_2 = 'REJECT' and tr_3 = 'REJECT'
                                and amt < a_1 and a_1 < a_2 and a_2 < a_3 
                                and (trans_date - dt) <=interval '20 minute');
        """
        
