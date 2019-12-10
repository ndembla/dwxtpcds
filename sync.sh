mkdir -p ~/.aws
vim ~/.aws/config
[default]
aws_access_key_id=<AWS-ACCESS-KEY-HERE>
aws_secret_access_key=<AWS-SECRET-KEY-HERE>
s3 =
  max_concurrent_requests = 200
  max_queue_size = 10000
  multipart_threshold = 64MB
  multipart_chunksize = 16MB

#NOTE: the aws credentials above should have sufficient permissions to write to destination bucket

aws s3 sync s3://edws-llap-perf/tpcds_bin_partitioned_orc_10000.db s3://dwxtpcds-9n4f-dwx-managed/clusters/env-gd9n4f/warehouse-1575919490-dwpb/warehouse/tablespace/managed/hive/tpcds_bin_partitioned_orc_10000.db

