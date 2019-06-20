#!/bin/sh
set -e
source /home/q/scripts/utils.sh $@

basepath=$(cd `dirname $0`; pwd)

hive_user=${hive_user}
update_date=$2
label_date=$3
filename=dep_date_pred_data_${label_date}
echo ${update_date}, ${filename}
base_dir="/home/q/tmp/f_algorithm_model/flight_growth"

outsql="
select
  qunar_username
  ,dep_date_of_search
  ,date_cnt
  ,search_date
  ,pre_days
  ,pre_days_max
  ,pre_days_min
  ,pre_days_avg
  ,difference_between_dep_date_median
  ,dep_date_span
  ,search_date_cnt
  ,search_date_single_cnt
  ,search_date_rate
  ,ota_date_cnt
  ,ota_date_single_cnt
  ,ota_date_rate
  ,search_date_is_weekend
  ,ota_date_is_weekend
  ,ota_date_on_friday_night
  ,ota_date_on_saturday_morning
  ,history_weekend_rate
  ,is_student
  ,is_trader
  ,dt
from f_analysis.dep_date_prediction_features_offline
where dt='${update_date}'
"
echo ${outsql}

sudo -uflightdev /home/q/big_hive/apache-hive-1.0.0-bin/bin/hive -e"
set mapred.reduce.slowstart.completed.maps=0.9;
set hive.cli.print.header=true;
set hive.resultset.use.unique.column.names=false;
${outsql}">${base_dir}/dataset/${filename}.csv