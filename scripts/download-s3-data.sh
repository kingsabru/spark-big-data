#!/bin/bash

AWS_ACCESS_KEY_ID=<access-key-from-email>
AWS_SECRET_ACCESS_KEY=<secret-key-from-email>
DATA_DIR=../data


# check aws cli installed
if ! command -v aws &> /dev/null
then
    echo "AWS CLI must be installed for this script to work"
    exit
fi

# view all datasets available
aws s3 ls s3://bigspark.challenge.data

# download data
aws s3 sync s3://bigspark.challenge.data/tpcds_data_5g $DATA_DIR/tpcds_data_5g
aws s3 sync s3://bigspark.challenge.data/tpcds_data_5g_streaming $DATA_DIR/tpcds_data_5g_streaming
aws s3 sync s3://bigspark.challenge.data/tpcds_data_5g_batch $DATA_DIR/tpcds_data_5g_batch