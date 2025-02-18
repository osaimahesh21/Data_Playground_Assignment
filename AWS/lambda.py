import boto3


def delete_s3_objects(bucket_name, prefix=''):
    s3 = boto3.client('s3')

    # List objects in the bucket with the given prefix
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' in objects:
        # Create a list of objects to delete
        delete_objects = [{'Key': obj['Key']} for obj in objects['Contents']]

        # Delete the objects
        s3.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_objects})

        print(f"Deleted {len(delete_objects)} objects from {bucket_name}")
    else:
        print("No objects found to delete.")


# Example usage
def lambda_handler(event, context):
    bucket_name = 'lambda-s3-delete'  # Updated bucket name
    prefix = ''  # Optionally specify a prefix
    delete_s3_objects(bucket_name, prefix)
    return {
        'statusCode': 200,
        'body': 'S3 objects deleted successfully'
    }
