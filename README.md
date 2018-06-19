# backfill-failed-data

## About
Sometimes Kinesis fails to send data to Amazon ES, and failed data is stored on S3 bucket. (If you set so)  
Failed data is like below.
```
{
 "esIndexName": "IndexName",
 "esDocumentId": "DocumentId",
 "esTypeName": "Typename",
 "rawData": "log data (encrypted by bas64)"
}
```

This jar file makes to be able to backfill these data by 2steps.
1. Get failed data from S3 object by using S3 Select.
2. Execute bulk api on elasticsearch via AmazonHttpClient.

## Usage
1. write aws access key, secret key, es endpoint and S3bucket name that is stored failed data to [aws.properties].
2. build jar file.
3. execute jar file with S3 path.  
   For example: `java -jar log-backfill.jar /elasticsearch-failed/yyyy/mm/dd/`  
   ※ this command'll backfill all data that is contained to S3 object stored under '/elasticsearch-failed/yyyy/mm/dd/' folder.