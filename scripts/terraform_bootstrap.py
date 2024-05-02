import argparse
import logging
import re
from google.cloud import storage

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)


def validate_bucket_name(bucket_name: str) -> bool:
    """
    Validates the bucket name according to Google Cloud Storage naming guidelines.
    See https://cloud.google.com/storage/docs/buckets#naming.

    Args:
        bucket_name (str): The name of the bucket.

    Returns:
        bool: True if the name of the bucket is valid, False if not.
    """
    if (
        re.fullmatch(re.compile(r"^[a-z0-9]([-a-z0-9_]*[a-z0-9])?$"), bucket_name)
        and 3 <= len(bucket_name) <= 63
        and "--" not in bucket_name
        and not bucket_name.startswith("-")
        and not bucket_name.endswith("-")
    ):
        return True
    else:
        return False


def create_bucket(bucket_name: str, region: str) -> None:
    """
    Creates a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the bucket.
        region (str): Region where to deploy the bucket.

    Returns:
        None.
    """
    storage_client = storage.Client()
    storage_client.create_bucket(bucket_name, location=region)
    logging.info(f"Bucket {bucket_name} successfully created in region '{region}'.")


def delete_bucket(bucket_name: str) -> None:
    """
    Deletes a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the bucket.

    Returns:
        None.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    bucket.delete(force=True)
    logging.info(f"Bucket {bucket_name} deleted.")


def terraform_bootstrap(bucket_name: str, region: str) -> None:
    """
    Create a Google Cloud Storae Bucket to host Terraform state.

    Args:
        bucket_name (str): Name of the bucket.
        region (str): Region where to create the bucket.

    Returns:
        None.
    """
    if validate_bucket_name(bucket_name):
        logging.info(f"Bucket name: {bucket_name} is valid")
        storage_client = storage.Client()
        if storage_client.lookup_bucket(bucket_name):
            logging.info(f"The bucket {bucket_name} already exists.")
        else:
            create_bucket(bucket_name, region)
    else:
        logging.error(
            f"Error: Invalid bucket name: '{bucket_name}' "
            "ref: https://cloud.google.com/storage/docs/buckets#naming"
        )
        exit(1)


def terraform_bootstrap_destroy(bucket_name: str) -> None:
    """
    Destroy the Google Cloud Storage Bucket hosting the Terraform state.

    Args:
        bucket_name (str): Name of the bucket.

    Returns:
        None.
    """
    storage_client = storage.Client()
    if storage_client.lookup_bucket(bucket_name):
        delete_bucket(bucket_name)
    else:
        logging.info(f"The bucket {bucket_name} is not in your bucket list.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create or delete Google Cloud Storage bucket hosting the Terraorm state for ArmoniK Bench Cloud Composer deployment."
    )
    parser.add_argument("--name", required=True, help="Name of the bucket")
    parser.add_argument("--region", help="Region of the bucket (required for deployment)")
    parser.add_argument("--destroy", action="store_true", help="Weither to destroy the bucket.")
    args = parser.parse_args()

    if args.destroy:
        terraform_bootstrap_destroy(args.name)
    else:
        if not args.region:
            logging.error("Region argument is required for deployment.")
            exit(1)
        terraform_bootstrap(args.name, args.region)
