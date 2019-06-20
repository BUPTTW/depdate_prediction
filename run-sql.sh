#!/bin/sh
set -e
update_date=$(date -d"${_day} 1 days ago" +"%Y%m%d")
label_date=$(date -d"${_day} 1 days ago" +"%Y%m%d")
echo ${update_date},${label_date}
#step get predict data
sh ./sql/extract_predict_data.sh ${update_date} ${label_date}