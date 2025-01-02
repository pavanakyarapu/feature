import boto3
import botocore
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # S3 bucket and prefix received from event (e.g., passed via API Gateway or event triggers)
    bucket_name = event.get('bucket_name')
    prefix = event.get('prefix', '')  # Optional prefix
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    try:
        # List objects under the given bucket and prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:  # No files in the bucket/prefix
            print("No files found in the specified S3 location.")
            return False
        
        for obj in response['Contents']:
            key = obj['Key']
            size = obj['Size']  # File size in bytes
            
            print(f"Checking file: {key}, Size: {size} bytes")
            
            # Check if file size is zero
            if size == 0:
                print(f"File {key} is of size 0 bytes. Returning False.")
                return False
            
            # Check if the file can be opened
            try:
                s3_client.get_object(Bucket=bucket_name, Key=key)
            except ClientError as e:
                print(f"Unable to open file {key}. Error: {str(e)}")
                return False

        print("All files are valid and non-zero in size. Returning True.")
        return True
    
    except botocore.exceptions.BotoCoreError as error:
        print(f"Error accessing S3: {str(error)}")
        return False