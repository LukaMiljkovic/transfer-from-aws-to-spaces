import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()


def connect_to_bucket(s3_client, bucket_name):
    """
    Attempts to connect to (check existence and permissions for) the bucket
    by calling head_bucket. Prints a success or error message.
    Exits the script if the connection fails.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Successfully connected to bucket '{bucket_name}'.")
    except ClientError as e:
        print(f"Error connecting to bucket '{bucket_name}': {e}")
        exit(1)


def transfer_object_streaming(
        aws_s3_client,
        aws_bucket_name,
        obj_key,
        do_s3_client,
        do_bucket_name,
        max_retries=3
):
    """
    Transfers a single object from AWS to DO Spaces with up to 'max_retries' attempts,
    streaming the file instead of loading it fully into memory.

    Returns True if the transfer eventually succeeds; False if it fails all retries.
    """
    for attempt in range(1, max_retries + 1):
        try:
            # 1) Get the AWS object (streaming, not read() into memory)
            aws_obj = aws_s3_client.get_object(Bucket=aws_bucket_name, Key=obj_key)
            body_stream = aws_obj['Body']  # File-like streaming object

            # 2) Upload to DigitalOcean Spaces using the streaming body
            do_s3_client.upload_fileobj(
                Fileobj=body_stream,
                Bucket=do_bucket_name,
                Key=obj_key
            )

            print(f"[Attempt {attempt}/{max_retries}] Transferred '{obj_key}' via streaming.")
            return True  # Success, break out of the loop
        except ClientError as e:
            print(f"[Attempt {attempt}/{max_retries}] Error transferring '{obj_key}': {e}")
            # If this was the last attempt, we give up
            if attempt == max_retries:
                return False


def main():
    # ------------------------------------------------------------------------
    # 1) Connect to AWS S3 (credentials & bucket info from environment)
    # ------------------------------------------------------------------------
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region_name = os.getenv("REGION_NAME")  # e.g., "eu-west-2"
    aws_bucket_name = os.getenv("BUCKET_NAME")

    aws_s3_client = boto3.client(
        "s3",
        region_name=aws_region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    # Verify we can connect to AWS S3
    connect_to_bucket(aws_s3_client, aws_bucket_name)

    # ------------------------------------------------------------------------
    # 2) Connect to DigitalOcean Spaces (using your endpoint + credentials)
    # ------------------------------------------------------------------------
    do_access_key = os.getenv("DO_ACCESS_KEY")
    do_secret_key = os.getenv("DO_SECRET_KEY")
    do_endpoint_url = os.getenv("DO_ENDPOINT_URL")
    do_bucket_name = os.getenv("DO_BUCKET_NAME")

    do_s3_client = boto3.client(
        "s3",
        endpoint_url=do_endpoint_url,
        aws_access_key_id=do_access_key,
        aws_secret_access_key=do_secret_key
    )

    # Verify we can connect to DO Spaces
    connect_to_bucket(do_s3_client, do_bucket_name)

    # ------------------------------------------------------------------------
    # 3) List ALL objects in AWS bucket IN CHUNKS, then transfer each
    # ------------------------------------------------------------------------
    paginator = aws_s3_client.get_paginator("list_objects_v2")
    # For example, get 10 objects per page (chunk):
    page_iterator = paginator.paginate(
        Bucket=aws_bucket_name,
        PaginationConfig={"PageSize": 10}
    )

    # Open logs in append mode
    transferred_log = open("transfered_all_aws_files.txt", "a")
    failed_log = open("failed_transfers.txt", "a")

    files_found = False

    for page_number, page in enumerate(page_iterator, start=1):
        print(f"\n--- Processing chunk #{page_number} ---")
        if "Contents" not in page:
            # Means either no objects or we've reached the end
            print("No more objects in this chunk.")
            continue

        files_found = True

        # Iterate through each object in the chunk
        for obj in page["Contents"]:
            obj_key = obj["Key"]
            print(f"Attempting to transfer: {obj_key}")

            # Transfer with streaming and up to 3 retries
            success = transfer_object_streaming(
                aws_s3_client,
                aws_bucket_name,
                obj_key,
                do_s3_client,
                do_bucket_name,
                max_retries=3
            )

            if success:
                transferred_log.write(obj_key + "\n")
            else:
                failed_log.write(obj_key + "\n")

    transferred_log.close()
    failed_log.close()

    if not files_found:
        print(f"No objects found in AWS bucket '{aws_bucket_name}'. Nothing to transfer.")
    else:
        print("\nAll chunked transfers completed!")
        print("Check 'transfered_all_aws_files.txt' and 'failed_transfers.txt' for details.")


if __name__ == "__main__":
    main()
