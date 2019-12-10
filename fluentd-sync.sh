apt-get update
apt-get install -y python curl vim
curl -O https://bootstrap.pypa.io/get-pip.py
export PATH=~/.local/bin:$PATH
python get-pip.py --user
pip install awscli --upgrade --user

aws s3 sync s3://edws-llap-perf/tpcds_bin_partitioned_orc_10000.db s3://dwxtpcds-9n4f-dwx-managed/clusters/env-gd9n4f/warehouse-1575919490-dwpb/warehouse/tablespace/managed/hive/tpcds_bin_partitioned_orc_10000.db


