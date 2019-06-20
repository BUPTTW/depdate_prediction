select 
    t.qunar_username
    ,dep_date_of_search
    ,date_cnt
    ,search_date
    ,pre_days
    ,max(pre_days) over(partition by t.qunar_username) as pre_days_max
    ,min(pre_days) over(partition by t.qunar_username) as pre_days_min
    ,avg(pre_days) over(partition by t.qunar_username) as pre_days_avg
    ,percentile(pre_days,0.5) over(partition by t.qunar_username) as pre_days_median
    ,pre_days-(percentile(pre_days,0.5) over(partition by t.qunar_username)) as difference_between_dep_date_median
    ,dep_date_span
    ,search_date_cnt
    ,search_date_single_cnt
    ,if(search_date_cnt>0,search_date_single_cnt*1.0/search_date_cnt,0) as search_date_rate
    ,ota_date_cnt
    ,ota_date_single_cnt
    ,if(ota_date_cnt>0,ota_date_single_cnt*1.0/ota_date_cnt,0) as ota_date_rate
    ,search_date_is_weekend
    ,ota_date_is_weekend
    ,ota_date_on_friday_night
    ,ota_date_on_saturday_morning
    ,history_weekend_rate
    ,is_student
    ,is_trader
    ,if(array_contains(order_dep_date_distinct,dep_date_of_search),1,0) as label
    from
    (
    select
        qunar_username
        ,dep_date_of_search
        ,max(pre_days) as pre_days
        ,count(1) over(partition by qunar_username) as date_cnt
        ,max(datediff(dep_date_of_search_max,dep_date_of_search_min)) as dep_date_span
        ,max(search_date) as search_date
        ,max(search_date_cnt) as search_date_cnt
        ,count(if(process='list',1,NULL)) as search_date_single_cnt
        ,max(ota_date_cnt) as ota_date_cnt
        ,count(if(process='ota',1,NULL)) as ota_date_single_cnt
        ,max(order_dep_date_distinct) as order_dep_date_distinct
        ,max(search_date_is_weekend) as search_date_is_weekend
        ,max(ota_date_is_weekend) as ota_date_is_weekend
        ,max(ota_date_on_friday_night) as ota_date_on_friday_night
        ,max(ota_date_on_saturday_morning) as ota_date_on_saturday_morning
        from
        (
        select
            log.qunar_username as qunar_username
            ,search_date
            ,dep_date_of_search
            ,datediff(dep_date_of_search,(max(log.search_date) over(partition by log.qunar_username))) as pre_days
            ,process
            ,order_dep_date_distinct
            ,if(process='list' and (week=0 or week=6),1,0) as search_date_is_weekend
            ,if(process='ota' and (week=0 or week=6),1,0) as ota_date_is_weekend
            ,if(dep_period='晚间' and week=5,1,0) as ota_date_on_friday_night
            ,if(dep_period='上午' and week=6,1,0) as ota_date_on_saturday_morning
            ,count(if(process='list',1,NULL)) over(partition by log.qunar_username) as search_date_cnt
            ,count(if(process='ota',1,NULL)) over(partition by log.qunar_username) as ota_date_cnt
            ,max(dep_date_of_search) over(partition by log.qunar_username) as dep_date_of_search_max
            ,min(dep_date_of_search) over(partition by log.qunar_username) as dep_date_of_search_min
            ,max(log.search_date) over(partition by log.qunar_username) as search_date_max
            from
            (
            select
                wide1.qunar_username as qunar_username
                ,dep_date
                ,create_time_max
                ,order_dep_date_distinct
                from
                (
                select 
                    qunar_username
                    ,to_date(dep_date) as dep_date
                    ,max(create_time) as create_time_max
                    ,collect_set(to_date(dep_date)) over(partition by qunar_username) as order_dep_date_distinct
                    from f_wide.wide_order where pay_ok=1 and dt='20190526' and qunar_username is not null and  qunar_username not in ('','NULL','null')
                    group by qunar_username,dep_date
                    having count(1)<20
                ) wide1
                left join
                (
                select 
                    qunar_username
                    from f_wide.wide_order where pay_ok=1 and dt between '201905024' and '201905025' and qunar_username is not null and  qunar_username not in ('','NULL','null')
                    group by qunar_username
                ) wide2
                on (wide1.qunar_username = wide2.qunar_username) where wide2.qunar_username is null
            ) wide
            inner join
            (
            select 
                username as qunar_username
                ,search_date
                ,dep_date_of_search
                ,pmod(datediff(dep_date_of_search, '1920-01-01') - 3, 7) as week
                ,'NULL' as dep_period
                ,concat(search_date,' ',search_time) as log_time
                ,'list' as process
                from f_analysis.user_search2list_behavior
                where dt between '20190524' and '20190526' and username is not  null and  username not in ('','NULL','null')
            union all         
            select 
                username as qunar_username
                ,search_date
                ,dep_date as dep_date_of_search
                ,pmod(datediff(dep_date, '1920-01-01') - 3, 7) as week
                ,dep_period
                ,concat(search_date,' ',search_time) as log_time
                ,'ota' as process
                from f_analysis.user_search2ota_behavior
                where dt between '20190524' and '20190526' and username is not  null and  username not in ('','NULL','null')
            ) log
            on wide.qunar_username=log.qunar_username
            where log.log_time<=wide.create_time_max 
        ) t
        where t.pre_days>=0 group by qunar_username,dep_date_of_search
        
    ) t
    left join 
    (
    select
        key  
        ,max(if(tag='is_trader',1,NULL)) as is_trader
        ,max(if(tag='is_student',1,NULL)) as is_student
        from
        user.wide_user_tag
        where tag in('is_trader','is_student')
        group by key  
    ) tag
    on t.qunar_username=tag.key
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
            from f_wide.wide_order where pay_ok=1 and dt>='20170524' and dt<='20190523' and qunar_username is not null and  qunar_username not in ('','NULL','null')
            group by qunar_username
        ) history
    ) history_date
    on t.qunar_username=history_date.qunar_username
    
    
    
    
    
    
    
  
    
    
