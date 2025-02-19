# S3 Auto-Delete Lambda Function

This project sets up an AWS Lambda function that automatically deletes a file from an S3 bucket as soon as it is uploaded. The function is triggered by an S3 event and removes the newly uploaded file.

## How It Works

1. A file is uploaded to the specified S3 bucket.
2. An S3 event triggers the Lambda function.
3. The Lambda function receives the event, extracts the uploaded file's details, and deletes the file from the bucket.

## Prerequisites

- An AWS account
- An S3 bucket
- An IAM role with permissions for S3 and Lambda
- A Lambda function set up in AWS

## Setup Instructions

### 1. Create an S3 Bucket
Ensure you have an existing S3 bucket or create a new one.

### 2. Set Up IAM Role
Create an IAM role with the following permissions:
```json
{
    "Effect": "Allow",
    "Action": [
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket"
    ],
    "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
    ]
}
3. Create a Lambda Function
Use Python or another supported runtime.
Attach the IAM role with the required permissions.
Add the following Python code to your Lambda function:
python
Copy
Edit
import boto3
import json

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        
        # Delete the uploaded file
        s3.delete_object(Bucket=bucket_name, Key=object_key)
        print(f"Deleted {object_key} from {bucket_name}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('File deleted successfully!')
    }
4. Configure S3 Event Trigger
In the AWS Lambda console, navigate to your function.
Under Configuration > Triggers, add an S3 trigger.
Select your S3 bucket and choose the event type as PUT (Object Created).
Save the trigger.
Testing
Upload a file to the S3 bucket.
The Lambda function should trigger automatically and delete the uploaded file.
Check the Lambda logs in Amazon CloudWatch to verify execution.
Notes
Ensure versioning is disabled in the S3 bucket if you want permanent deletion.
Modify the function if you want to delete specific files based on conditions.
Conclusion
This setup ensures that any file uploaded to the S3 bucket is automatically deleted, helping with temporary storage and security requirements.

