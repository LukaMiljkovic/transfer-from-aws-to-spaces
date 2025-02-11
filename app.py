import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    Also renames the file's root directory from "dentons_01" to "bl_01".

    Returns a tuple: (original_key, new_key, success)
    """
    # Replace the root directory "dentons_01" with "bl_01" in the object key.
    new_key = obj_key.replace("dentons_01", "bl_01", 1)
    for attempt in range(1, max_retries + 1):
        try:
            # 1) Get the AWS object as a streaming body
            aws_obj = aws_s3_client.get_object(Bucket=aws_bucket_name, Key=obj_key)
            body_stream = aws_obj['Body']  # This is a file-like streaming object

            # 2) Upload to DigitalOcean Spaces using the streaming upload,
            #    storing the file under the new key (with the new root)
            do_s3_client.upload_fileobj(
                Fileobj=body_stream,
                Bucket=do_bucket_name,
                Key=new_key
            )

            print(f"[Attempt {attempt}/{max_retries}] Transferred '{obj_key}' as '{new_key}' via streaming.")
            return (obj_key, new_key, True)
        except ClientError as e:
            print(f"[Attempt {attempt}/{max_retries}] Error transferring '{obj_key}' as '{new_key}': {e}")
            if attempt == max_retries:
                return (obj_key, new_key, False)


def main():
    # ------------------------------------------------------------------------
    # 1) Connect to AWS S3 (credentials & bucket info from environment)
    # ------------------------------------------------------------------------
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region_name = os.getenv("REGION_NAME")  # e.g., "eu-west-2"
    # In AWS the bucket holds files with a root directory "dentons_01"
    aws_bucket_name = os.getenv("BUCKET_NAME")

    aws_s3_client = boto3.client(
        "s3",
        region_name=aws_region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    connect_to_bucket(aws_s3_client, aws_bucket_name)

    # ------------------------------------------------------------------------
    # 2) Connect to DigitalOcean Spaces (using endpoint & credentials from environment)
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

    connect_to_bucket(do_s3_client, do_bucket_name)

    # ------------------------------------------------------------------------
    # 3) List ALL objects in AWS bucket IN CHUNKS and transfer them concurrently
    # ------------------------------------------------------------------------
    paginator = aws_s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=aws_bucket_name,
        PaginationConfig={"PageSize": 10}  # Adjust chunk size as needed
    )

    # Open log files in append mode
    transferred_log = open("transfered_all_aws_files.txt", "a")
    failed_log = open("failed_transfers.txt", "a")

    # Using ThreadPoolExecutor to speed up transfers
    futures = []
    with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers as needed
        for page_number, page in enumerate(page_iterator, start=1):
            print(f"\n--- Processing chunk #{page_number} ---")
            if "Contents" not in page:
                print("No more objects in this chunk.")
                continue

            for obj in page["Contents"]:
                obj_key = obj["Key"]
                print(f"Submitting transfer for: {obj_key}")
                future = executor.submit(
                    transfer_object_streaming,
                    aws_s3_client,
                    aws_bucket_name,
                    obj_key,
                    do_s3_client,
                    do_bucket_name,
                    3  # max_retries
                )
                futures.append(future)

    # Collect results from all futures and log accordingly.
    for future in as_completed(futures):
        orig_key, new_key, success = future.result()
        if success:
            transferred_log.write(f"{orig_key} -> {new_key}\n")
        else:
            failed_log.write(f"{orig_key} -> {new_key}\n")

    transferred_log.close()
    failed_log.close()

    print("\nAll transfers completed!")
    print("Check 'transfered_all_aws_files.txt' and 'failed_transfers.txt' for details.")


if __name__ == "__main__":
    main()
