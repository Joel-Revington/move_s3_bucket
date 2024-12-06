import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Replace these with your actual bucket details
SOURCE_BUCKET = ""
DESTINATION_BUCKET = ""
SOURCE_REGION = ""
DESTINATION_REGION = ""

# Configure S3 clients
session = boto3.Session()
source_s3 = session.client("s3", region_name=SOURCE_REGION)
destination_s3 = session.client("s3", region_name=DESTINATION_REGION)


def copy_bucket_contents():
    try:
        logging.info(f"Starting to copy objects from {SOURCE_BUCKET} to {DESTINATION_BUCKET}...")
        continuation_token = None

        while True:
            # List objects in the source bucket
            list_kwargs = {"Bucket": SOURCE_BUCKET}
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token

            response = source_s3.list_objects_v2(**list_kwargs)

            if "Contents" in response:
                for obj in response["Contents"]:
                    key = obj["Key"]
                    logging.info(f"Copying object: {key}")

                    try:
                        # Copy object to the destination bucket
                        copy_source = {"Bucket": SOURCE_BUCKET, "Key": key}
                        destination_s3.copy_object(
                            Bucket=DESTINATION_BUCKET,
                            Key=key,
                            CopySource=copy_source,
                        )
                        logging.info(f"Successfully copied: {key}")
                    except ClientError as e:
                        logging.error(f"Failed to copy {key}: {e}")

            # Check if there are more objects to process
            if response.get("IsTruncated"):  # If truncated, fetch the next batch
                continuation_token = response["NextContinuationToken"]
            else:
                break

        logging.info("All objects copied successfully!")

    except (NoCredentialsError, PartialCredentialsError) as e:
        logging.error("AWS credentials error: %s", str(e))
    except Exception as e:
        logging.error(f"Error copying bucket contents: {str(e)}")

def delete_source_bucket_contents():
    try:
        print(f"Starting to delete objects from {SOURCE_BUCKET}...")

        continuation_token = None

        while True:
            # List objects in the source bucket
            list_kwargs = {"Bucket": SOURCE_BUCKET}
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token

            response = source_s3.list_objects_v2(**list_kwargs)

            if "Contents" in response:
                for obj in response["Contents"]:
                    key = obj["Key"]
                    print(f"Deleting object: {key}")

                    # Delete the object from the source bucket
                    source_s3.delete_object(Bucket=SOURCE_BUCKET, Key=key)
                    print(f"Successfully deleted: {key}")

            # Check if there are more objects to process
            if response.get("IsTruncated"):  # If truncated, fetch the next batch
                continuation_token = response["NextContinuationToken"]
            else:
                break

        print("All objects deleted successfully!")

    except (NoCredentialsError, PartialCredentialsError) as e:
        print("AWS credentials error:", str(e))
    except Exception as e:
        print(f"Error deleting bucket contents: {str(e)}")

# Main execution
if __name__ == "__main__":
    copy_bucket_contents()
