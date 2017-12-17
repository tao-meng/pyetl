#!/bin/bash
today=`date +%Y%m%d`
echo $today
cur_dir=$(dirname $0)
cd $cur_dir/..
home=`pwd`
# echo $home
export PYTHONPATH=$home
source /etc/profile
source /root/.bashrc

python3 ./vastio/etl/demo.py
