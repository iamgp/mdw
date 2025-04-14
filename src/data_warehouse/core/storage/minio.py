"""MinIO storage functionality for the data warehouse."""

import io
import json
import os
from collections.abc import Mapping
from typing import Any

from minio import Minio
from minio.error import S3Error

from data_warehouse.config.settings import settings
from data_warehouse.core.exceptions import StorageError
from data_warehouse.utils.logger import logger


class MinioClient:
    """MinIO client for object storage operations."""

    _instance = None

    def __new__(cls):
        """Implement singleton pattern for MinIO client."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize MinIO client with configuration settings."""
        if self._initialized:
            return

        self.client = Minio(
            f"{settings.MINIO_HOST}:{settings.MINIO_PORT}",
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )

        self._initialized = True
        logger.debug("MinIO client initialized")

    def initialize_buckets(self) -> None:
        """Initialize required MinIO buckets.

        Raises:
            StorageError: If bucket creation fails
        """
        required_buckets = [
            {"name": "raw", "description": "Storage for raw ingested data"},
            {"name": "processed", "description": "Storage for processed data"},
            {"name": "tmp", "description": "Temporary storage for data processing"},
            {"name": "export", "description": "Storage for data to be exported"},
        ]

        for bucket in required_buckets:
            try:
                if not self.client.bucket_exists(bucket["name"]):
                    self.client.make_bucket(bucket["name"])

                    # Add bucket tags
                    tags = {
                        "description": bucket["description"],
                        "created_by": "data-warehouse",
                        "environment": settings.ENVIRONMENT,
                    }

                    self.client.set_bucket_tags(bucket["name"], tags)  # type: ignore[arg-type]
                    # Pyright: set_bucket_tags expects Tags, dict[str, str] is valid for Minio SDK.
                    logger.info(f"Bucket '{bucket['name']}' created")
                else:
                    logger.debug(f"Bucket '{bucket['name']}' already exists")
            except S3Error as e:
                logger.error(f"Failed to create bucket '{bucket['name']}': {e}")
                raise StorageError(f"Failed to create bucket '{bucket['name']}'") from e

    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_data: bytes | io.BytesIO | str,
        content_type: str = "application/octet-stream",
        metadata: 'Mapping[str, str | list[str] | tuple[str, ...]] | None' = None,
    ) -> None:
        """Upload a file to MinIO.

        Args:
            bucket_name: Name of the bucket to upload to
            object_name: Name of the object to create in MinIO
            file_data: File data to upload (bytes, BytesIO, or filepath)
            content_type: MIME type of the object
            metadata: Optional metadata to attach to the object

        Raises:
            StorageError: If upload fails
        """
        try:
            # Handle different types of file_data
            # Handle bytes, BytesIO, or str (filepath)
            if isinstance(file_data, bytes):
                file = io.BytesIO(file_data)
                size = len(file_data)
                need_close = False
            elif isinstance(file_data, io.BytesIO):
                file = file_data
                size = file_data.getbuffer().nbytes
                need_close = False
            else:
                file = open(file_data, "rb")
                try:
                    size = os.path.getsize(file_data)
                except Exception:
                    file.seek(0, 2)
                    size = file.tell()
                    file.seek(0)
                need_close = True

            self.client.put_object(
                bucket_name,
                object_name,
                file,
                size,
                content_type=content_type,
                metadata=metadata if metadata is None else dict(metadata),  # type: ignore[arg-type]
                # Pyright: Minio expects Dict[str, str | List[str] | Tuple[str]], cast as needed.
            )
            if need_close:
                file.close()

                logger.debug(f"Uploaded object '{object_name}' to bucket '{bucket_name}'")
        except (OSError, S3Error, ValueError) as e:
            logger.error(f"Failed to upload object '{object_name}' to bucket '{bucket_name}': {e}")
            raise StorageError(f"Failed to upload object '{object_name}'") from e

    def download_file(
        self,
        bucket_name: str,
        object_name: str,
        to_file: str | None = None,
    ) -> bytes | None:
        """Download a file from MinIO.

        Args:
            bucket_name: Name of the bucket to download from
            object_name: Name of the object to download
            to_file: Optional filepath to save the downloaded object

        Returns:
            File content as bytes if to_file is None, otherwise None

        Raises:
            StorageError: If download fails
        """
        response = None
        try:
            response = self.client.get_object(bucket_name, object_name)

            if to_file:
                with open(to_file, "wb") as file:
                    for data in response.stream(32 * 1024):
                        file.write(data)
                logger.debug(f"Downloaded object '{object_name}' to '{to_file}'")
                return None
            else:
                # Read all data to memory
                data = response.read()
                logger.debug(f"Downloaded object '{object_name}' to memory")
                return data
        except S3Error as e:
            logger.error(f"Failed to download object '{object_name}' from bucket '{bucket_name}': {e}")
            raise StorageError(f"Failed to download object '{object_name}'") from e
        finally:
            if response is not None:
                response.close()
                response.release_conn()

    def list_objects(self, bucket_name: str, prefix: str = "", recursive: bool = True) -> list[dict[str, Any]]:
        """List objects in a bucket with an optional prefix.

        Args:
            bucket_name: Name of the bucket to list objects from
            prefix: Optional prefix to filter objects by
            recursive: Whether to list objects recursively (in subdirectories)

        Returns:
            List of object information dictionaries

        Raises:
            StorageError: If listing objects fails
        """
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=recursive)
            result: list[dict[str, Any]] = []

            for obj in objects:
                result.append(
                    {
                        "name": obj.object_name,
                        "size": obj.size,
                        "last_modified": obj.last_modified,
                        "etag": obj.etag,
                    }
                )

            return result
        except S3Error as e:
            logger.error(f"Failed to list objects in bucket '{bucket_name}': {e}")
            raise StorageError(f"Failed to list objects in bucket '{bucket_name}'") from e

    def remove_object(self, bucket_name: str, object_name: str) -> None:
        """Remove an object from a bucket.

        Args:
            bucket_name: Name of the bucket to remove object from
            object_name: Name of the object to remove

        Raises:
            StorageError: If removal fails
        """
        try:
            self.client.remove_object(bucket_name, object_name)
            logger.debug(f"Removed object '{object_name}' from bucket '{bucket_name}'")
        except S3Error as e:
            logger.error(f"Failed to remove object '{object_name}' from bucket '{bucket_name}': {e}")
            raise StorageError(f"Failed to remove object '{object_name}'") from e

    def upload_json(
        self,
        bucket_name: str,
        object_name: str,
        data: dict[str, Any],
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Upload JSON data to MinIO.

        Args:
            bucket_name: Name of the bucket to upload to
            object_name: Name of the object to create in MinIO
            data: JSON serializable dictionary
            metadata: Optional metadata to attach to the object

        Raises:
            StorageError: If upload fails
        """
        try:
            json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
            self.upload_file(
                bucket_name,
                object_name,
                json_bytes,
                content_type="application/json",
                metadata=metadata if metadata is None else dict(metadata),  # type: ignore[arg-type]
# Pyright: Minio expects Dict[str, str | List[str] | Tuple[str]], cast as needed.
            )
        except (TypeError, ValueError) as e:
            logger.error(f"Failed to serialize JSON data: {e}")
            raise StorageError("Failed to serialize JSON data") from e

    def download_json(self, bucket_name: str, object_name: str) -> dict[str, Any]:
        """Download and parse JSON data from MinIO.

        Args:
            bucket_name: Name of the bucket to download from
            object_name: Name of the object to download

        Returns:
            Parsed JSON data as a dictionary

        Raises:
            StorageError: If download or parsing fails
        """
        try:
            data = self.download_file(bucket_name, object_name)
            if data:
                return json.loads(data.decode("utf-8"))
            raise StorageError(f"Failed to download JSON object '{object_name}'")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON data from object '{object_name}': {e}")
            raise StorageError(f"Failed to parse JSON data from object '{object_name}'") from e


def get_minio_client() -> MinioClient:
    """Get MinIO client singleton instance.

    Returns:
        MinioClient: Instance of MinioClient
    """
    return MinioClient()


def initialize_minio() -> None:
    """Initialize MinIO storage with required buckets.

    Raises:
        StorageError: If initialization fails
    """
    try:
        client = get_minio_client()
        client.initialize_buckets()
        logger.info("MinIO storage initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize MinIO storage: {e}")
        raise StorageError("Failed to initialize MinIO storage") from e
