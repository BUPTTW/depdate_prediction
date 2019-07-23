--sudo -uflightdev /home/q/big_hive/apache-hive-1.0.0-bin/bin/hive -e"
set mapred.reduce.slowstart.completed.maps=0.9;
set mapred.reduce.tasks=500;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;
select * from
(
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
    ,label
    ,row_number() over(partition by label order by rand(1234)) as rank
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
        ,max(search_date_is_weekend) as search_date_is_weekend
        ,max(ota_date_is_weekend) as ota_date_is_weekend
        ,max(ota_date_on_friday_night) as ota_date_on_friday_night
        ,max(ota_date_on_saturday_morning) as ota_date_on_saturday_morning
        ,max(dep_time_before_dawn_cnt) as dep_time_before_dawn_cnt
        ,max(dep_time_morning_cnt) as dep_time_morning_cnt
        ,max(dep_time_afternoon_cnt) as dep_time_afternoon_cnt
        ,max(dep_time_night_cnt) as dep_time_night_cnt
        ,max(label) as label
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
            ,case when log.log_time between wide2.create_time-7*24*60*60*1000 and wide2.create_time then 1 else 0 end as label
            ,case when (wide1.create_time>=(log.log_time-2*24*60*60*1000) and wide1.create_time<log.log_time) then 1 else 0 end as filter_order_2days            
            from
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
                                where p_qunar_username is not null and p_qunar_username not in ('','NULL','null') and p_process=p_subprocess and dt between '20190615' and '20190618'
                            ) log1
                        ) log2
                        where lead_log_time - log_time>10*60*1000 or lead_log_time is null and process in ('list','ota')
                    ) log3
                    left join
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
                        where p_qunar_username is not null and p_qunar_username not in ('','NULL','null') and p_process=p_subprocess and dt between '20190615' and '20190618'
                    ) log4
                    on log3.qunar_username=log4.qunar_username
                    where log4.log_time < log3.log_time and log4.log_time >= log3.log_time-60*60*1000
                ) log5
            ) log
            left join  --  去掉 T 时刻两天前之内有下单的用户  ----------------------------------要不要加 where wide1.qunar_username is NULL----------------------------------------------
            (
            select
                qunar_username
                ,unix_timestamp(create_time)*1000 as create_time
                from f_wide.wide_order
                where pay_ok=1 and dt between '20190613' and '20190618' and qunar_username is not null and qunar_username not in ('','NULL','null')
            ) wide1
            on log.qunar_username=wide1.qunar_username
            left join  -- 取 T 未来7天下单的用户为正样本
            (
            select
                qunar_username
                ,to_date(dep_date) as dep_date
                ,unix_timestamp(create_time)*1000 as create_time
                from f_wide.wide_order
                where pay_ok=1 and dt between '20190617' and '20190630' and qunar_username is not null and qunar_username not in ('','NULL','null')
            ) wide2
            on wide2.qunar_username=log.qunar_username and wide2.dep_date=log.dep_date_of_search  
            where wide1.qunar_username is NULL            
        ) tlog
        group by qunar_username,dep_date_of_search,log_time
        having (max(filter_order_2days)=0 and sum(filter_order_2days)<20)
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
            from f_wide.wide_order where pay_ok=1 and dt>='20170615' and dt<='20190615' and qunar_username is not null and  qunar_username not in ('','NULL','null')
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
) all
  where rank <= 1000000;
--" > /home/q/tmp/wenjin.li/tmp/dep_date_prediction_online_20190615.csv






--------写临时表---------
 
-- 1、选样本 ，所有在20190615到20190618有搜索行为的用户，随机选取200万条
select
    p_qunar_username as qunar_username
    ,cast(time as bigint) as log_time
    ,p_process as process
    ,date_add('1970-01-01',day) as search_date
    ,p_dep_date as dep_date_of_search
    ,pmod(datediff(p_dep_date, '1920-01-01') - 3, 7) as week                                
    from qlibra.flight_server_log
    where p_qunar_username is not null and p_qunar_username not in ('','NULL','null') and p_process=p_subprocess and dt between '20190615' and '20190618'
    limit 2000000
    