import boto3 
import json
import pandas as pd # adding layer on AWS

# Initialize S3 clients
s3 = boto3.client('s3')
sns = boto3.client('sns')
sns_arn = "arn:aws:sns:us-east-1:654654491149:doordash-aws"

def lambda_handler(event, context):
    try:
        # Get the source and destination bucket names and file keys from the event
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        source_file_key = event['Records'][0]['s3']['object']['key']
        destination_bucket = 'doordash-target-zn-de'
        destination_file_key = f'{source_file_key[:10]}-filtered.json'

        # Download the file from the source bucket
        source_file_obj = s3.get_object(Bucket=source_bucket, Key=source_file_key)
        file_content = source_file_obj['Body'].read().decode('utf-8')

        # Parse JSON data into a DataFrame
        df = pd.read_json(file_content)

        # Perform filtering based on order status
        filtered_df = df[df.status == 'delivered']

        # Convert the filtered DataFrame back to JSON
        filtered_json = filtered_df.to_json(orient='records')

        # Write the filtered JSON data to the destination bucket
        s3.put_object(Bucket=destination_bucket, Key=destination_file_key, Body=filtered_json)

        message = "Input S3 File {} has been processed successfully !!".format("s3://"+source_bucket+"/"+source_file_key)
        response = sns.publish(Subject="SUCCESS - DoorDash Daily Data Processing",TargetArn=sns_arn, Message=message, MessageStructure='text')

        return {
            'statusCode': 200,
            'body': json.dumps('Filtered data written to S3 successfully!')
        }
    except Exception as err:
        print(err)
        message = "Input S3 File {} processing is Failed !!".format("s3://"+destination_bucket+"/"+destination_file_key)
        response = sns.publish(Subject="FAILED - DoorDash Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')