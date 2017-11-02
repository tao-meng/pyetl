#!/bin/bash
source /etc/profile
today=`date +%Y%m%d`
echo $today
cur_dir=$(dirname $0)
cd $cur_dir

python3 vst_stat_state.py
python3 vst_stat_people.py