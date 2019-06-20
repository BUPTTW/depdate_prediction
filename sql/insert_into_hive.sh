#将结果写入到结果表中
#!/bin/sh
set -e
source /home/q/scripts/utils.sh $@
basepath=$(cd `dirname $0`; pwd)
update_date=$2
label_date=$3
base_dir="/home/q/tmp/f_algorithm_model/flight_growth"
sql="
load data local inpath
'${base_dir}/result/dep_date_pred_${label_date}_result.csv'
overwrite into table f_analysis.result_user_depdate_prediction
partition(dt = '${update_date}');
"

echo ${sql}
sudo -u${hive_user} hive -e "
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nostrict;
SET hive.exec.max.dynamic.partitions.pernode=1000;
${sql}
select dt ,count(1) as count from f_analysis.result_user_depdate_prediction group by dt order by dt desc limit 5;


"
