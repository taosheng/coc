
rm scoc.zip
zip scoc.zip -r lambda_scoc.py  nocheckin.py storeList.py awsconfig.py elasticsearch/  requests_aws4auth/ urllib3/ requests/

aws --profile ailine s3 cp scoc.zip s3://sandyai2/

#aws --profile ailine lambda create-function --function-name scoc --runtime python3.6 --role "arn:aws:iam::740157263594:role/sandyairun" --handler lambda_scoc.lambda_scochandler --timeout 20 --code "S3Bucket=sandyai2,S3Key=scoc.zip"


aws --profile ailine lambda update-function-code --function-name scoc --s3-bucket sandyai2 --s3-key scoc.zip
