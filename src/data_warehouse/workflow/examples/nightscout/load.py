"""
Nightscout Data Loading Module.

This module provides classes for loading transformed Nightscout data into the data warehouse.
"""

from typing import Any

import pandas as pd
from loguru import logger
from sqlalchemy import Column, DateTime, Float, Integer, String, Table, create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import MetaData

from data_warehouse.core.exceptions import LoaderError
from data_warehouse.workflow.base import WorkflowContext
from data_warehouse.workflow.etl import LoaderBase


class NightscoutLoader(LoaderBase[dict[str, Any]]):
    """Loader for Nightscout data into the data warehouse."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the loader with configuration.

        Args:
            config: Configuration for the loader
        """
        super().__init__(config)
        self.engine = None
        self.metadata = MetaData()

        # Define tables
        self.entries_table = Table(
            "nightscout_entries",
            self.metadata,
            Column("id", String, primary_key=True),
            Column("device", String),
            Column("date", DateTime),
            Column("sgv", Integer),
            Column("sgv_mmol", Float),
            Column("direction", String),
            Column("type", String),
            Column("filtered", Float),
            Column("unfiltered", Float),
            Column("rssi", Integer),
            Column("noise", Integer),
            Column("sys_time", String),
            Column("utc_offset", Integer),
            Column("raw_data", JSONB),
        )

        self.treatments_table = Table(
            "nightscout_treatments",
            self.metadata,
            Column("id", String, primary_key=True),
            Column("type", String),
            Column("created_at", DateTime),
            Column("entered_by", String),
            Column("insulin", Float),
            Column("carbs", Float),
            Column("duration", Integer),
            Column("rate", Float),
            Column("percent", Integer),
            Column("absolute", Float),
            Column("notes", String),
            Column("raw_data", JSONB),
        )

        self.profiles_table = Table(
            "nightscout_profiles",
            self.metadata,
            Column("id", String, primary_key=True),
            Column("created_at", DateTime),
            Column("start_date", String),
            Column("timezone", String),
            Column("dia", Float),
            Column("units", String),
            Column("raw_data", JSONB),
        )

        self.devicestatus_table = Table(
            "nightscout_devicestatus",
            self.metadata,
            Column("id", String, primary_key=True),
            Column("created_at", DateTime),
            Column("device", String),
            Column("raw_data", JSONB),
        )

    def _initialize_db_connection(self) -> None:
        """Initialize the database connection."""
        if self.engine is None:
            connection_string = self.config.credentials.get("connection_string")
            if not connection_string:
                raise LoaderError("Database connection string not provided in configuration")

            try:
                self.engine = create_engine(connection_string)
                # Test connection
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("Database connection initialized successfully")
            except SQLAlchemyError as e:
                raise LoaderError(f"Failed to connect to database: {str(e)}") from e

    def _create_tables_if_not_exist(self) -> None:
        """Create tables if they don't exist."""
        try:
            if self.engine is None:
                raise LoaderError("Database engine not initialized")
            self.metadata.create_all(self.engine, checkfirst=True)
            logger.info("Database tables created or verified")
        except SQLAlchemyError as e:
            raise LoaderError(f"Failed to create database tables: {str(e)}") from e

    def load(self, data: dict[str, Any], context: WorkflowContext) -> int:
        """Load Nightscout data into the database.

        Args:
            data: The transformed Nightscout data
            context: The workflow context

        Returns:
            Number of records loaded

        Raises:
            LoaderError: If loading fails
        """
        try:
            self._initialize_db_connection()
            self._create_tables_if_not_exist()

            total_records = 0

            # Load entries
            if entries := data.get("entries"):
                total_records += self._load_entries(entries)

            # Load treatments
            if treatments := data.get("treatments"):
                total_records += self._load_treatments(treatments)

            # Load profiles
            if profiles := data.get("profiles"):
                total_records += self._load_profiles(profiles)

            # Load devicestatus
            if devicestatus := data.get("devicestatus"):
                total_records += self._load_devicestatus(devicestatus)

            logger.info(f"Successfully loaded {total_records} Nightscout records")
            return total_records

        except Exception as e:
            logger.error(f"Failed to load Nightscout data: {str(e)}")
            raise LoaderError(f"Failed to load Nightscout data: {str(e)}") from e

    def _load_entries(self, entries: list[dict[str, Any]]) -> int:
        """Load CGM entries into the database.

        Args:
            entries: The entries to load

        Returns:
            Number of entries loaded
        """
        if not entries:
            return 0

        try:
            if self.engine is None:
                raise LoaderError("Database engine not initialized")

            # Convert to DataFrame for efficient batch insertion
            df = pd.DataFrame(entries)

            # Add raw data column
            df["raw_data"] = df.to_dict("records")

            # Insert data
            with self.engine.begin() as conn:
                # Use batch size from config
                batch_size = self.config.batch_size

                # Load data in batches
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i : i + batch_size]

                    # If upsert is enabled, use it
                    if self.config.upsert:
                        insert_stmt = (
                            self.entries_table.insert()
                            .values(batch.to_dict("records"))
                            .prefix_with("ON CONFLICT (id) DO UPDATE SET ")
                        )

                        # Exclude id from update
                        for column in self.entries_table.columns:
                            if column.name != "id":
                                insert_stmt = insert_stmt.prefix_with(f"{column.name} = EXCLUDED.{column.name},")

                        # Remove trailing comma
                        insert_stmt = insert_stmt.prefix_with("", replace=True)
                    else:
                        insert_stmt = self.entries_table.insert().values(batch.to_dict("records"))

                    conn.execute(insert_stmt)

            logger.info(f"Loaded {len(df)} entries into database")
            return len(df)

        except Exception as e:
            logger.error(f"Failed to load entries: {str(e)}")
            raise LoaderError(f"Failed to load entries: {str(e)}") from e

    def _load_treatments(self, treatments: list[dict[str, Any]]) -> int:
        """Load treatments into the database.

        Args:
            treatments: The treatments to load

        Returns:
            Number of treatments loaded
        """
        if not treatments:
            return 0

        try:
            if self.engine is None:
                raise LoaderError("Database engine not initialized")

            # Convert to DataFrame for efficient batch insertion
            df = pd.DataFrame(treatments)

            # Add raw data column
            df["raw_data"] = df.to_dict("records")

            # Insert data
            with self.engine.begin() as conn:
                # Use batch size from config
                batch_size = self.config.batch_size

                # Load data in batches
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i : i + batch_size]

                    # If upsert is enabled, use it
                    if self.config.upsert:
                        insert_stmt = (
                            self.treatments_table.insert()
                            .values(batch.to_dict("records"))
                            .prefix_with("ON CONFLICT (id) DO UPDATE SET ")
                        )

                        # Exclude id from update
                        for column in self.treatments_table.columns:
                            if column.name != "id":
                                insert_stmt = insert_stmt.prefix_with(f"{column.name} = EXCLUDED.{column.name},")

                        # Remove trailing comma
                        insert_stmt = insert_stmt.prefix_with("", replace=True)
                    else:
                        insert_stmt = self.treatments_table.insert().values(batch.to_dict("records"))

                    conn.execute(insert_stmt)

            logger.info(f"Loaded {len(df)} treatments into database")
            return len(df)

        except Exception as e:
            logger.error(f"Failed to load treatments: {str(e)}")
            raise LoaderError(f"Failed to load treatments: {str(e)}") from e

    def _load_profiles(self, profiles: list[dict[str, Any]]) -> int:
        """Load profiles into the database.

        Args:
            profiles: The profiles to load

        Returns:
            Number of profiles loaded
        """
        if not profiles:
            return 0

        try:
            if self.engine is None:
                raise LoaderError("Database engine not initialized")

            # Convert to DataFrame for efficient batch insertion
            df = pd.DataFrame(profiles)

            # Add raw data column
            df["raw_data"] = df.to_dict("records")

            # Insert data
            with self.engine.begin() as conn:
                # If upsert is enabled, use it
                if self.config.upsert:
                    insert_stmt = (
                        self.profiles_table.insert()
                        .values(df.to_dict("records"))
                        .prefix_with("ON CONFLICT (id) DO UPDATE SET ")
                    )

                    # Exclude id from update
                    for column in self.profiles_table.columns:
                        if column.name != "id":
                            insert_stmt = insert_stmt.prefix_with(f"{column.name} = EXCLUDED.{column.name},")

                    # Remove trailing comma
                    insert_stmt = insert_stmt.prefix_with("", replace=True)
                else:
                    insert_stmt = self.profiles_table.insert().values(df.to_dict("records"))

                conn.execute(insert_stmt)

            logger.info(f"Loaded {len(df)} profiles into database")
            return len(df)

        except Exception as e:
            logger.error(f"Failed to load profiles: {str(e)}")
            raise LoaderError(f"Failed to load profiles: {str(e)}") from e

    def _load_devicestatus(self, devicestatus: list[dict[str, Any]]) -> int:
        """Load device status entries into the database.

        Args:
            devicestatus: The device status entries to load

        Returns:
            Number of device status entries loaded
        """
        if not devicestatus:
            return 0

        try:
            if self.engine is None:
                raise LoaderError("Database engine not initialized")

            # Convert to DataFrame for efficient batch insertion
            df = pd.DataFrame(devicestatus)

            # Add raw data column
            df["raw_data"] = df.to_dict("records")

            # Insert data
            with self.engine.begin() as conn:
                # Use batch size from config
                batch_size = self.config.batch_size

                # Load data in batches
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i : i + batch_size]

                    # If upsert is enabled, use it
                    if self.config.upsert:
                        insert_stmt = (
                            self.devicestatus_table.insert()
                            .values(batch.to_dict("records"))
                            .prefix_with("ON CONFLICT (id) DO UPDATE SET ")
                        )

                        # Exclude id from update
                        for column in self.devicestatus_table.columns:
                            if column.name != "id":
                                insert_stmt = insert_stmt.prefix_with(f"{column.name} = EXCLUDED.{column.name},")

                        # Remove trailing comma
                        insert_stmt = insert_stmt.prefix_with("", replace=True)
                    else:
                        insert_stmt = self.devicestatus_table.insert().values(batch.to_dict("records"))

                    conn.execute(insert_stmt)

            logger.info(f"Loaded {len(df)} device status entries into database")
            return len(df)

        except Exception as e:
            logger.error(f"Failed to load device status entries: {str(e)}")
            raise LoaderError(f"Failed to load device status entries: {str(e)}") from e

    def validate_target(self) -> bool:
        """Validate the database target configuration.

        Returns:
            True if the target is valid, False otherwise
        """
        try:
            self._initialize_db_connection()
            return True
        except LoaderError:
            return False
