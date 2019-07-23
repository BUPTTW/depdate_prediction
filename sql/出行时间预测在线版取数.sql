--sudo -uflightdev /home/q/big_hive/apache-hive-1.0.0-bin/bin/hive -e"
--set mapred.reduce.slowstart.completed.maps=0.9;
--set hive.cli.print.header=true;
--set hive.resultset.use.unique.column.names=false;
--set hive.cli.print.header=true;
select 
    t.qunar_username
    ,dep_date_of_search
    ,log_time
    ,date_cnt
    ,search_date
    ,pre_days
    ,max(pre_days) over(partition by t.qunar_username,log_time) as pre_days_max
    ,min(pre_days) over(partition by t.qunar_username,log_time) as pre_days_min
    ,avg(pre_days) over(partition by t.qunar_username,log_time) as pre_days_avg
    ,percentile(pre_days,0.5) over(partition by t.qunar_username,log_time) as pre_days_median
    ,pre_days-(percentile(pre_days,0.5) over(partition by t.qunar_username,log_time)) as difference_between_dep_date_median
    ,dep_date_span
    ,search_date_cnt
    ,current_date_search_cnt
    ,if(search_date_cnt>0,current_date_search_cnt*1.0/search_date_cnt,0) as current_date_search_rate
    ,ota_date_cnt
    ,current_date_ota_cnt
    ,if(ota_date_cnt>0,current_date_ota_cnt*1.0/ota_date_cnt,0) as current_date_ota_rate
    ,search_date_is_weekend
    ,ota_date_is_weekend
    ,ota_date_on_friday_night
    ,ota_date_on_saturday_morning
    ,dep_time_before_dawn_cnt
    ,dep_time_morning_cnt
    ,dep_time_afternoon_cnt
    ,dep_time_night_cnt
    ,history_weekend_rate
    ,is_student
    ,is_trader
    ,if(array_contains(order_dep_date_distinct,dep_date_of_search),1,0) as label
    from
    (
    select
        qunar_username
        ,log_time
        ,dep_date_of_search
        ,max(search_date) as search_date
        ,max(pre_days) as pre_days
        ,count(1) over(partition by qunar_username,log_time) as date_cnt
        ,max(datediff(dep_date_of_search_max,dep_date_of_search_min)) as dep_date_span
        ,max(search_date_cnt) as search_date_cnt
        ,count(if(process='list',1,NULL)) as current_date_search_cnt
        ,max(ota_date_cnt) as ota_date_cnt
        ,count(if(process='ota',1,NULL)) as current_date_ota_cnt
        ,max(order_dep_date_distinct) as order_dep_date_distinct
        ,max(search_date_is_weekend) as search_date_is_weekend
        ,max(ota_date_is_weekend) as ota_date_is_weekend
        ,max(ota_date_on_friday_night) as ota_date_on_friday_night
        ,max(ota_date_on_saturday_morning) as ota_date_on_saturday_morning
        ,max(dep_time_before_dawn_cnt) as dep_time_before_dawn_cnt
        ,max(dep_time_morning_cnt) as dep_time_morning_cnt
        ,max(dep_time_afternoon_cnt) as dep_time_afternoon_cnt
        ,max(dep_time_night_cnt) as dep_time_night_cnt
        from
        (
        select
            log.qunar_username as qunar_username
            ,log.log_time as log_time
            ,log.period as period
            ,log.process as process
            ,log.search_date as search_date
            ,log.dep_date_of_search as dep_date_of_search
            ,log.pre_days as pre_days
            ,wide.order_dep_date_distinct as order_dep_date_distinct
            ,if(process='list' and (week=0 or week=6),1,0) as search_date_is_weekend
            ,if(process='ota' and (week=0 or week=6),1,0) as ota_date_is_weekend
            ,if(process='ota' and week=5 and period between 18 and 22,1,0) as ota_date_on_friday_night
            ,if(process='ota' and week=6 and period between 8 and 11,1,0) as ota_date_on_saturday_morning
            ,count(if(process='ota' and period between 0 and 5,1,NULL)) over(partition by log.qunar_username,log.log_time) as dep_time_before_dawn_cnt
            ,count(if(process='ota' and period between 6 and 11,1,NULL)) over(partition by log.qunar_username,log.log_time) as dep_time_morning_cnt
            ,count(if(process='ota' and period between 12 and 17,1,NULL)) over(partition by log.qunar_username,log.log_time) as dep_time_afternoon_cnt
            ,count(if(process='ota' and period between 18 and 23,1,NULL)) over(partition by log.qunar_username,log.log_time) as dep_time_night_cnt
            ,count(if(process='list',1,NULL)) over(partition by log.qunar_username,log.log_time) as search_date_cnt
            ,count(if(process='ota',1,NULL)) over(partition by log.qunar_username,log.log_time) as ota_date_cnt
            ,max(dep_date_of_search) over(partition by log.qunar_username,log.log_time) as dep_date_of_search_max
            ,min(dep_date_of_search) over(partition by log.qunar_username,log.log_time) as dep_date_of_search_min            
            from
            (
            select 
                wide1.qunar_username as qunar_username
                ,wide1.dep_date as dep_date
                ,unix_timestamp(wide1.create_time_max)*1000 as create_time_max
                ,wide1.order_dep_date_distinct as order_dep_date_distinct
                from
                (
                select 
                    qunar_username 
                    ,to_date(dep_date) as dep_date
                    ,max(create_time) as create_time_max
                    ,collect_set(to_date(dep_date)) over(partition by qunar_username) as order_dep_date_distinct
                    from f_wide.wide_order 
                    where pay_ok=1 and dt='20190603' and qunar_username is not null and  qunar_username not in ('','NULL','null')
                    group by qunar_username,dep_date
                    having count(1)<20
                ) wide1
                left join
                (
                select 
                    qunar_username
                    from f_wide.wide_order 
                    where pay_ok=1 and dt between '20190601' and '20190602' and qunar_username is not null and qunar_username not in ('','NULL','null')
                    group by qunar_username
                ) wide2
                on (wide1.qunar_username = wide2.qunar_username) where wide2.qunar_username is null
            ) wide
            inner join
            (
            select
                qunar_username
                ,log_time
                ,period
                ,process
                ,search_date
                ,dep_date_of_search
                ,week
                ,pre_days
                ,cnt
                ,cnt_all
                from
                (
                select 
                    log4.qunar_username as qunar_username
                    ,log3.log_time as log_time
                    ,log4.period as period
                    ,log4.process as process
                    ,log4.search_date as search_date
                    ,log4.dep_date_of_search as dep_date_of_search
                    ,log4.week as week
                    ,log4.pre_days as pre_days
                    ,count(if(log4.process in ('list','ota'),1,NULL)) over (partition by log4.qunar_username,log3.log_time) as cnt
                    ,count(1) over (partition by log4.qunar_username,log3.log_time) as cnt_all
                    from
                    (
                    select
                        qunar_username
                        ,log_time
                        ,lead_log_time
                        ,process
                        ,search_date
                        ,dep_date_of_search
                        ,week
                        from
                        (
                        select
                            qunar_username
                            ,log_time
                            ,lead(log_time,1) over (partition by qunar_username order by log_time) as lead_log_time
                            ,process
                            ,search_date
                            ,dep_date_of_search
                            ,week
                            from
                            (
                            select
                                p_qunar_username as qunar_username
                                ,cast(time as bigint) as log_time
                                ,p_process as process
                                ,date_add('1970-01-01',day) as search_date
                                ,p_dep_date as dep_date_of_search
                                ,pmod(datediff(p_dep_date, '1920-01-01') - 3, 7) as week                                
                                from qlibra.flight_server_log
                                where p_qunar_username is not null and p_qunar_username not in ('','NULL','null') and dt between '20190602' and '20190603'
                            ) log1
                        ) log2
                        where lead_log_time - log_time>15*60*1000 or lead_log_time is null and process in ('list','ota')
                    ) log3
                    inner join
                    (
                    select
                        p_qunar_username as qunar_username
                        ,cast(time as bigint) as log_time
                        ,p_process as process
                        ,date_add('1970-01-01',day) as search_date
                        ,p_dep_date as dep_date_of_search
                        ,datediff(p_dep_date,'1970-01-01')-day as pre_days
                        ,pmod(datediff(p_dep_date, '1920-01-01') - 3, 7) as week
                        ,if(p_process='ota', cast(substr(get_json_object(p_response, '$.searchRecord.goTime'), 1, 2) as int), NULL) as period                      
                        from qlibra.flight_server_log
                        where p_qunar_username is not null and p_qunar_username not in ('','NULL','null') and dt between '20190602' and '20190603'
                    ) log4
                    on log3.qunar_username=log4.qunar_username
                    where log3.log_time-log4.log_time between 0 and 60*60*1000
                ) log5
                where cnt=cnt_all
            ) log
            on wide.qunar_username=log.qunar_username
            where log.log_time<=wide.create_time_max     
        ) tlog
        group by qunar_username,dep_date_of_search,log_time
    ) t
    left join 
    (
    select
        qunar_username
        ,if(all_history_date>0,weekend_history_date*1.0/all_history_date,0) as history_weekend_rate
        from
        (
        select 
            qunar_username
            ,count(1) as all_history_date
            ,count(if(pmod(datediff(to_date(dep_date),'1920-01-01')-3,7)=0 or pmod(datediff(to_date(dep_date),'1920-01-01')-3,7)=6,1,NULL)) as weekend_history_date
            from f_wide.wide_order where pay_ok=1 and dt>='20170603' and dt<='20190602' and qunar_username is not null and  qunar_username not in ('','NULL','null')
            group by qunar_username
        ) history
    ) history_date
    on t.qunar_username=history_date.qunar_username
    left join
    (
    select
        key  
        ,max(if(tag='is_trader',1,0)) as is_trader
        ,max(if(tag='is_student',1,0)) as is_student
        from
        user.wide_user_tag
        where tag in('is_trader','is_student')
        group by key  
    ) tag
    on t.qunar_username=tag.key
--" > /home/q/tmp/wenjin.li/tmp/dep_date_prediction_online_newdata_20190606.csv

