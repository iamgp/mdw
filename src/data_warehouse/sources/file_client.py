"""
File client for reading data from files.

This module provides a client for reading data from various file formats.
It handles different file formats and provides consistent error handling.
"""

import logging
import os
from pathlib import Path

import pandas as pd


class FileClient:
    """Client for reading data from files."""

    def __init__(self, data_dir: str | Path):
        """Initialize file client.

        Args:
            data_dir: Directory containing data files
        """
        self.data_dir = Path(data_dir)
        self.logger = logging.getLogger(__name__)

        if not self.data_dir.exists():
            self.logger.warning(f"Data directory does not exist: {self.data_dir}")
            os.makedirs(self.data_dir, exist_ok=True)
            self.logger.info(f"Created data directory: {self.data_dir}")

    def read_csv(self, filename: str, **kwargs) -> pd.DataFrame:
        """Read data from a CSV file.

        Args:
            filename: Name of the CSV file in the data directory
            **kwargs: Additional arguments to pass to pandas.read_csv

        Returns:
            DataFrame containing the CSV data

        Raises:
            FileNotFoundError: If the file does not exist
            ValueError: If the file cannot be read as CSV
        """
        file_path = self.data_dir / filename

        if not file_path.exists():
            self.logger.error(f"CSV file not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            self.logger.info(f"Reading CSV file: {file_path}")
            return pd.read_csv(file_path, **kwargs)
        except Exception as e:
            self.logger.error(f"Failed to read CSV file {file_path}: {str(e)}")
            raise ValueError(f"Failed to read CSV file: {str(e)}") from e

    def read_excel(
        self, filename: str, sheet_name: str | int | list[str | int] | None = None, **kwargs
    ) -> pd.DataFrame | dict[str, pd.DataFrame]:
        """Read data from an Excel file.

        Args:
            filename: Name of the Excel file in the data directory
            sheet_name: Sheet(s) to read (name, index, or list of names/indices)
            **kwargs: Additional arguments to pass to pandas.read_excel

        Returns:
            DataFrame containing the Excel data or dictionary of DataFrames (multiple sheets)

        Raises:
            FileNotFoundError: If the file does not exist
            ValueError: If the file cannot be read as Excel
        """
        file_path = self.data_dir / filename

        if not file_path.exists():
            self.logger.error(f"Excel file not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            self.logger.info(f"Reading Excel file: {file_path}")
            return pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
        except Exception as e:
            self.logger.error(f"Failed to read Excel file {file_path}: {str(e)}")
            raise ValueError(f"Failed to read Excel file: {str(e)}") from e

    def read_parquet(self, filename: str, **kwargs) -> pd.DataFrame:
        """Read data from a Parquet file.

        Args:
            filename: Name of the Parquet file in the data directory
            **kwargs: Additional arguments to pass to pandas.read_parquet

        Returns:
            DataFrame containing the Parquet data

        Raises:
            FileNotFoundError: If the file does not exist
            ValueError: If the file cannot be read as Parquet
        """
        file_path = self.data_dir / filename

        if not file_path.exists():
            self.logger.error(f"Parquet file not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            self.logger.info(f"Reading Parquet file: {file_path}")
            return pd.read_parquet(file_path, **kwargs)
        except Exception as e:
            self.logger.error(f"Failed to read Parquet file {file_path}: {str(e)}")
            raise ValueError(f"Failed to read Parquet file: {str(e)}") from e

    def save_csv(self, df: pd.DataFrame, filename: str, **kwargs) -> None:
        """Save DataFrame to a CSV file.

        Args:
            df: DataFrame to save
            filename: Name of the CSV file in the data directory
            **kwargs: Additional arguments to pass to DataFrame.to_csv

        Raises:
            ValueError: If the DataFrame cannot be saved as CSV
        """
        file_path = self.data_dir / filename

        try:
            self.logger.info(f"Saving DataFrame to CSV file: {file_path}")
            df.to_csv(file_path, **kwargs)
            self.logger.info(f"Successfully saved {len(df)} rows to {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to save CSV file {file_path}: {str(e)}")
            raise ValueError(f"Failed to save CSV file: {str(e)}") from e

    def list_files(self, pattern: str | None = None, extension: str | None = None) -> list[Path]:
        """List files in the data directory.

        Args:
            pattern: Optional glob pattern to filter files
            extension: Optional file extension to filter files

        Returns:
            List of file paths matching the criteria
        """
        if pattern:
            files = list(self.data_dir.glob(pattern))
        else:
            files = [f for f in self.data_dir.iterdir() if f.is_file()]

        if extension:
            ext = extension if extension.startswith(".") else f".{extension}"
            files = [f for f in files if f.suffix.lower() == ext.lower()]

        return files
