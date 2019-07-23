--sudo -uflightdev /home/q/big_hive/apache-hive-1.0.0-bin/bin/hive -e"
set mapred.reduce.slowstart.completed.maps=0.9;
set mapred.reduce.tasks=500;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;
select * from
(select
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
    ,label
    ,row_number() over(partition by label order by rand(1234)) as rank
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
        ,max(search_date_is_weekend) as search_date_is_weekend
        ,max(ota_date_is_weekend) as ota_date_is_weekend
        ,max(ota_date_on_friday_night) as ota_date_on_friday_night
        ,max(ota_date_on_saturday_morning) as ota_date_on_saturday_morning
        ,max(dt) as dt
        ,max(label) as label
        from
        (
        select
            log.qunar_username as qunar_username
            ,search_date
            ,dep_date_of_search
            ,datediff(dep_date_of_search,(max(log.search_date) over(partition by log.qunar_username))) as pre_days
            ,process
            ,if(process='list' and (week=0 or week=6),1,0) as search_date_is_weekend
            ,if(process='ota' and (week=0 or week=6),1,0) as ota_date_is_weekend
            ,if(dep_period='晚间' and week=5,1,0) as ota_date_on_friday_night
            ,if(dep_period='上午' and week=6,1,0) as ota_date_on_saturday_morning
            ,count(if(process='list',1,NULL)) over(partition by log.qunar_username) as search_date_cnt
            ,count(if(process='ota',1,NULL)) over(partition by log.qunar_username) as ota_date_cnt
            ,max(dep_date_of_search) over(partition by log.qunar_username) as dep_date_of_search_max
            ,min(dep_date_of_search) over(partition by log.qunar_username) as dep_date_of_search_min
            ,max(log.search_date) over(partition by log.qunar_username) as search_date_max
            ,log.dt
            ,case when wide.create_time between date_add(log.search_date,1) and date_add(log.search_date,7) then 1 else 0 end as label --date_sub() 只适用于2018-01-01类型的日期
            from
            (
            select
                username as qunar_username
                ,search_date
                ,dep_date_of_search
                ,pmod(datediff(dep_date_of_search, '1920-01-01') - 3, 7) as week
                ,'NULL' as dep_period
                ,concat(search_date,' ',search_time) as log_time
                ,'list' as process
                ,dt
                from f_analysis.user_search2list_behavior
                where dt between '20190521' and '20190527' and username is not  null and  username not in ('','NULL','null')
            union all
            select
                username as qunar_username
                ,search_date
                ,dep_date as dep_date_of_search
                ,pmod(datediff(dep_date, '1920-01-01') - 3, 7) as week
                ,dep_period
                ,concat(search_date,' ',search_time) as log_time
                ,'ota' as process
                ,dt
                from f_analysis.user_search2ota_behavior
                where dt between '20190521' and '20190527' and username is not  null and  username not in ('','NULL','null')
            ) log
            left join  -- 取 T 未来7天下单的用户为正样本
            (
            select
                ta.qunar_username ,dep_date ,dt ,create_time
                ,to_date(tb.dep_date) as dep_date
                ,ta.dt
                ,to_date(ta.create_time) as create_time
                from f_wide.wide_order ta inner join f_wide.wide_flight_segment tb
                on ta.order_no = tb.order_no
                and ta.dt = tb.dt
                where ta.pay_ok=1 and ta.dt between '20190528' and '20190603' and ta.qunar_username is not null and ta.qunar_username not in ('','NULL','null')
                and tb.dt between '20190528' and '20190603'
            ) wide
           on wide.qunar_username=log.qunar_username and wide.dep_date = log.dep_date_of_search
        ) t
        where t.pre_days>=0
        group by qunar_username,dep_date_of_search
    ) t
    left join
    (
    select
        w.qunar_username
        ,if(all_history_date>0,weekend_history_date*1.0/all_history_date,0) as history_weekend_rate
        from
        (select
            qunar_username
            ,count(1) over(partition by qunar_username) as all_history_date
            ,count(if(pmod(datediff(to_date(dep_date),'1920-01-01')-3,7)=0 or pmod(datediff(to_date(dep_date),'1920-01-01')-3,7)=6,1,NULL)) over(partition by qunar_username) as weekend_history_date
            ,dt
            ,order_no
            from f_wide.wide_order where dt between '20170521' and '20190521'and pay_ok=1 and qunar_username is not null and qunar_username not in ('','NULL','null')
        ) w
        inner join
        (
        select
            *
            from f_wide.wide_flight_segment
            where dt between '20170521' and '20190521'
        ) s
        on w.dt = s.dt and w.order_no = s.order_no
    ) history_date
    on t.qunar_username=history_date.qunar_username
    left join
    (select
        key
        ,max(if(tag='is_trader',1,NULL)) as is_trader
        ,max(if(tag='is_student',1,NULL)) as is_student
        ,max(dt) as dt
        from
        user.wide_user_tag_history
        where dt = '20190527' and tag in('is_trader','is_student')
        group by key
    ) tag
    on t.qunar_username=tag.key and t.dt = tag.dt
  ) all
  where rank <= 1000000;
--"> /home/q/tmp/wenjin.li/tmp/dep_date_prediction_offline_20190521.csv









 --------写临时表---------
 
-- 1、选样本 ，所有在20190521到20190527有搜索行为的用户，随机选取200万条
 
create table dep_date_offline_sample(
    qunar_username string
    ,search_date string
    ,dep_date_of_search string
    ,week int
    ,dep_period string
    ,log_time string
    ,process string
)
PARTITIONED BY ( 
    dt string 
)

insert overwrite table dep_date_offline_sample
    select
        username as qunar_username
        ,search_date
        ,dep_date_of_search
        ,pmod(datediff(dep_date_of_search, '1920-01-01') - 3, 7) as week
        ,'NULL' as dep_period
        ,concat(search_date,' ',search_time) as log_time
        ,'list' as process
        ,dt
        from f_analysis.user_search2list_behavior
        where dt between '20190521' and '20190527' and username is not  null and  username not in ('','NULL','null')
    union all
    select
        username as qunar_username
        ,search_date
        ,dep_date as dep_date_of_search
        ,pmod(datediff(dep_date, '1920-01-01') - 3, 7) as week
        ,dep_period
        ,concat(search_date,' ',search_time) as log_time
        ,'ota' as process
        ,dt
        from f_analysis.user_search2ota_behavior
        where dt between '20190521' and '20190527' and username is not  null and  username not in ('','NULL','null')
    order by rand(1234)
    limit 2000000;


-- 2、选取正样本
create table dep_date_offline_positive_sample(
    qunar_username string
    ,dep_date string
    ,dt string
    ,create_time string
)

insert overwrite table dep_date_offline_positive_sample
    select
        ta.qunar_username ,dep_date ,dt ,create_time
        ,to_date(tb.dep_date) as dep_date
        ,ta.dt
        ,to_date(ta.create_time) as create_time
        from f_wide.wide_order ta inner join f_wide.wide_flight_segment tb
        on ta.order_no = tb.order_no
        and ta.dt = tb.dt
        where ta.pay_ok=1 and ta.dt between '20190528' and '20190603' and ta.qunar_username is not null and ta.qunar_username not in ('','NULL','null')
        and tb.dt between '20190528' and '20190603'



-- 3、打标并取特征
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
        ,max(search_date_is_weekend) as search_date_is_weekend
        ,max(ota_date_is_weekend) as ota_date_is_weekend
        ,max(ota_date_on_friday_night) as ota_date_on_friday_night
        ,max(ota_date_on_saturday_morning) as ota_date_on_saturday_morning
        ,max(dt) as dt
        ,max(label) as label
        from
        (
        select
            log.qunar_username as qunar_username
            ,search_date
            ,dep_date_of_search
            ,datediff(dep_date_of_search,(max(log.search_date) over(partition by log.qunar_username))) as pre_days
            ,process
            ,if(process='list' and (week=0 or week=6),1,0) as search_date_is_weekend
            ,if(process='ota' and (week=0 or week=6),1,0) as ota_date_is_weekend
            ,if(dep_period='晚间' and week=5,1,0) as ota_date_on_friday_night
            ,if(dep_period='上午' and week=6,1,0) as ota_date_on_saturday_morning
            ,count(if(process='list',1,NULL)) over(partition by log.qunar_username) as search_date_cnt
            ,count(if(process='ota',1,NULL)) over(partition by log.qunar_username) as ota_date_cnt
            ,max(dep_date_of_search) over(partition by log.qunar_username) as dep_date_of_search_max
            ,min(dep_date_of_search) over(partition by log.qunar_username) as dep_date_of_search_min
            ,max(log.search_date) over(partition by log.qunar_username) as search_date_max
            ,log.dt
            ,case when wide.create_time between date_add(log.search_date,1) and date_add(log.search_date,7) then 1 else 0 end as label --date_sub() 只适用于2018-01-01类型的日期
            from
            dep_date_offline_sample log
            left join
            dep_date_offline_positive_sample wide
            on wide.qunar_username=log.qunar_username and wide.dep_date = log.dep_date_of_search
        ) t
        where t.pre_days>=0
        group by qunar_username,dep_date_of_search
    ) t
    left join
    (
    select
        w.qunar_username
        ,if(all_history_date>0,weekend_history_date*1.0/all_history_date,0) as history_weekend_rate
        from
        (select
            qunar_username
            ,count(1) over(partition by qunar_username) as all_history_date
            ,count(if(pmod(datediff(to_date(dep_date),'1920-01-01')-3,7)=0 or pmod(datediff(to_date(dep_date),'1920-01-01')-3,7)=6,1,NULL)) over(partition by qunar_username) as weekend_history_date
            ,dt
            ,order_no
            from f_wide.wide_order where dt between '20170521' and '20190521'and pay_ok=1 and qunar_username is not null and qunar_username not in ('','NULL','null')
        ) w
        inner join
        (
        select
            *
            from f_wide.wide_flight_segment
            where dt between '20170521' and '20190521'
        ) s
        on w.dt = s.dt and w.order_no = s.order_no
    ) history_date
    on t.qunar_username=history_date.qunar_username
    left join
    (select
        key
        ,max(if(tag='is_trader',1,NULL)) as is_trader
        ,max(if(tag='is_student',1,NULL)) as is_student
        ,max(dt) as dt
        from
        user.wide_user_tag_history
        where dt = '20190527' and tag in('is_trader','is_student')
        group by key
    ) tag
    on t.qunar_username=tag.key and t.dt = tag.dt