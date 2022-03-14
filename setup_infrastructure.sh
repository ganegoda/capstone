#!/bin/bash

# run as ./setup_infra.sh {your-bucket-name}
# aws cli must be installed and configured

AWS_ID=$(aws sts get-caller-identity --query Account --output text | cat)
AWS_REGION=$(aws configure get region)

# Change
SERVICE_NAME=hg-capstone-cluster
IAM_ROLE_NAME=hg-capstone-role

REDSHIFT_USER=awsuser
REDSHIFT_PASSWORD=CapP0ssword0987
REDSHIFT_PORT=5439
EMR_NODE_TYPE=m4.xlarge

echo "Creating bucket "$1""
aws s3api create-bucket --acl public-read-write --bucket $1 --output text >> setup.log

echo "uploading data"
aws s3 cp ./I94_SAS_Labels_Descriptions.SAS s3://hg-dend/I94_SAS_Labels_Descriptions.SAS
aws s3 cp ./airport-codes_csv.csv s3://hg-dend/data/airport-codes_csv.csv
aws s3 cp ./us-cities-demographics.csv s3://hg-dend/data/us-cities-demographics.csv
aws s3 cp ./data2/GlobalLandTemperaturesByCity.csv s3://hg-dend/data/GlobalLandTemperaturesByCity.csv
aws s3 cp ./gender.csv s3://hg-dend/data/gender.csv
aws s3 cp ./visa_class.csv s3://hg-dend/data/visa_class.csv
echo "Data uploaded"


# List dataset and append to log file
aws s3 ls --recursive hg-dend --output text >> setup.log

# Create IAM role, attach s3 access policy
echo '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}' > ./trust-policy.json

echo "Creating AWS IAM role"
aws iam create-role --role-name $IAM_ROLE_NAME --assume-role-policy-document file://trust-policy.json --description 's3 access' >> setup.log
echo "Attaching AmazonS3ReadOnlyAccess Policy to our IAM role"
aws iam attach-role-policy --role-name $IAM_ROLE_NAME --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess --output text >> setup.log

# iam role hasitha_boto3

# # creating a redshift cluster
# Allow public accesss and allow the vpc security group acess from your pc
# echo "Creating an AWS Redshift Cluster named "$SERVICE_NAME""
# aws redshift create-cluster --cluster-identifier $SERVICE_NAME --node-type dc2.large --master-username $REDSHIFT_USER --master-user-password $REDSHIFT_PASSWORD --cluster-type single-node --publicly-accessible --iam-roles "arn:aws:iam::"$AWS_ID":role/"$IAM_ROLE_NAME"" >> setup.log

# while :
# do
#    echo "Waiting for Redshift cluster "$SERVICE_NAME" to start, sleeping for 60s before next check"
#    sleep 60
#    REDSHIFT_CLUSTER_STATUS=$(aws redshift describe-clusters --cluster-identifier $SERVICE_NAME --query 'Clusters[0].ClusterStatus' --output text)
#    if [[ "$REDSHIFT_CLUSTER_STATUS" == "available" ]]
#    then
# 	break
#    fi
# done

# REDSHIFT_HOST=$(aws redshift describe-clusters --cluster-identifier $SERVICE_NAME --query 'Clusters[0].Endpoint.Address' --output text)

echo "adding redshift connections to Airflow connection param"
docker exec -d airflow-webserver-1 airflow connections add 'redshift' --conn-type 'Postgres' --conn-login $REDSHIFT_USER --conn-password $REDSHIFT_PASSWORD --conn-host $REDSHIFT_HOST --conn-port $REDSHIFT_PORT --conn-schema 'dev'


#echo "Setting up AWS access for Airflow workers"
AWS_ID=$(aws configure get aws_access_key_id)
AWS_SECRET_KEY=$(aws configure get aws_secret_access_key)
AWS_REGION=$(aws configure get region)
#docker exec -d airflow-webserver_1 airflow connections add 'aws_default' --conn-type 'aws' --conn-login $AWS_ID --conn-password $AWS_SECRET_KEY --conn-extra '{"region_name":"'$AWS_REGION'"}'
docker exec -d airflow-webserver-1 airflow connections add 'aws_credentials' --conn-type 'Amazon Web Services' --conn-login $AWS_ID --conn-password $AWS_SECRET_KEY

echo "adding S3 bucket name to Airflow variables"
docker exec -d airflow-webserver-1 airflow variables set s3_bucket hg-dend
docker exec -d airflow-webserver-1 airflow variables set aws_iam_arn arn:aws:iam::132711944849:role/hg-capstone-role

# # installing psycopg2 on mac M1
# brew install libpq --build-from-source
# brew install postgresql
# export LDFLAGS="-L/opt/homebrew/opt/libpq/lib"
# pip install psycopg2-binary
