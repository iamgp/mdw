"""
Nightscout Data Transformation Module.

This module provides classes for transforming Nightscout API data.
"""

from datetime import datetime
from typing import Any

from loguru import logger

from data_warehouse.core.exceptions import TransformerError
from data_warehouse.workflow.base import WorkflowContext
from data_warehouse.workflow.etl import TransformerBase


class NightscoutTransformer(TransformerBase[dict[str, Any], dict[str, Any]]):
    """Transformer for Nightscout API data."""

    def transform(self, data: dict[str, Any], context: WorkflowContext) -> dict[str, Any]:
        """Transform Nightscout API data.

        Args:
            data: The Nightscout API data to transform
            context: The workflow context

        Returns:
            Transformed data

        Raises:
            TransformerError: If transformation fails
        """
        if not data:
            return {}

        logger.info("Transforming Nightscout data")

        try:
            transformed_data = {
                "entries": self._transform_entries(data.get("entries", [])),
                "treatments": self._transform_treatments(data.get("treatments", [])),
                "profiles": self._transform_profiles(data.get("profiles", [])),
                "devicestatus": self._transform_devicestatus(data.get("devicestatus", [])),
            }

            return transformed_data

        except Exception as e:
            logger.error(f"Failed to transform Nightscout data: {str(e)}")
            raise TransformerError(f"Failed to transform Nightscout data: {str(e)}") from e

    def _transform_entries(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform CGM entries.

        Args:
            entries: The raw CGM entries

        Returns:
            Transformed entries
        """
        if not entries:
            return []

        logger.info(f"Transforming {len(entries)} CGM entries")
        transformed = []

        for entry in entries:
            # Standardize entry fields
            transformed_entry = {
                "id": entry.get("_id"),
                "device": entry.get("device"),
                "date": self._parse_nightscout_date(entry.get("date")),
                "dateString": entry.get("dateString"),
                "sgv": entry.get("sgv"),  # Blood glucose value in mg/dL
                "direction": entry.get("direction"),
                "type": entry.get("type"),
                "filtered": entry.get("filtered"),
                "unfiltered": entry.get("unfiltered"),
                "rssi": entry.get("rssi"),
                "noise": entry.get("noise"),
                "sys_time": entry.get("sysTime"),
                "utc_offset": entry.get("utcOffset"),
            }

            # Calculate mmol/L if sgv is available (conversion factor)
            sgv = entry.get("sgv")
            if sgv is not None:
                transformed_entry["sgv_mmol"] = round(sgv / 18.0, 1)

            transformed.append(transformed_entry)

        return transformed

    def _transform_treatments(self, treatments: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform treatments.

        Args:
            treatments: The raw treatments

        Returns:
            Transformed treatments
        """
        if not treatments:
            return []

        logger.info(f"Transforming {len(treatments)} treatments")
        transformed = []

        for treatment in treatments:
            # Extract treatment type
            treatment_type = treatment.get("eventType", "").lower()

            # Basic fields common to all treatments
            transformed_treatment = {
                "id": treatment.get("_id"),
                "type": treatment_type,
                "created_at": self._parse_nightscout_date(treatment.get("created_at")),
                "enteredBy": treatment.get("enteredBy"),
                "notes": treatment.get("notes"),
            }

            # Add specific fields based on treatment type
            if treatment_type == "bolus":
                transformed_treatment.update(
                    {
                        "insulin": float(treatment.get("insulin", 0)),
                        "duration": int(treatment.get("duration", 0)),
                    }
                )
            elif treatment_type == "carbs":
                transformed_treatment.update(
                    {
                        "carbs": float(treatment.get("carbs", 0)),
                    }
                )
            elif treatment_type == "combo bolus":
                transformed_treatment.update(
                    {
                        "insulin": float(treatment.get("insulin", 0)),
                        "carbs": float(treatment.get("carbs", 0)),
                    }
                )
            elif treatment_type == "temp basal":
                transformed_treatment.update(
                    {
                        "rate": float(treatment.get("rate", 0)),
                        "duration": int(treatment.get("duration", 0)),
                        "percent": int(treatment.get("percent", 0)),
                        "absolute": float(treatment.get("absolute", 0)),
                    }
                )
            elif treatment_type == "site change":
                transformed_treatment.update(
                    {
                        "device": treatment.get("device"),
                        "notes": treatment.get("notes"),
                    }
                )
            elif treatment_type == "sensor change":
                transformed_treatment.update(
                    {
                        "device": treatment.get("device"),
                        "notes": treatment.get("notes"),
                    }
                )
            elif treatment_type == "announcement":
                transformed_treatment.update(
                    {
                        "notes": treatment.get("notes"),
                    }
                )
            elif treatment_type == "exercise":
                transformed_treatment.update(
                    {
                        "duration": int(treatment.get("duration", 0)),
                        "notes": treatment.get("notes"),
                    }
                )
            else:
                # For other treatment types, copy all fields
                for key, value in treatment.items():
                    if key not in transformed_treatment and key != "_id":
                        transformed_treatment[key] = value

            transformed.append(transformed_treatment)

        return transformed

    def _transform_profiles(self, profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform profiles.

        Args:
            profiles: The raw profiles

        Returns:
            Transformed profiles
        """
        if not profiles:
            return []

        logger.info(f"Transforming {len(profiles)} profiles")
        transformed = []

        for profile in profiles:
            # Basic profile information
            transformed_profile = {
                "id": profile.get("_id"),
                "created_at": self._parse_nightscout_date(profile.get("created_at")),
                "startDate": profile.get("startDate"),
            }

            # Extract default profile
            default_profile_name = profile.get("defaultProfile")
            if default_profile_name and "store" in profile:
                store = profile.get("store", {})
                if default_profile_name in store:
                    default_profile = store[default_profile_name]

                    # Extract key settings
                    transformed_profile.update(
                        {
                            "dia": default_profile.get("dia"),  # Duration of insulin action
                            "carbs_hr": default_profile.get("carbs_hr"),  # Carbs per hour
                            "carbratio": default_profile.get("carbratio"),  # Carb ratio schedule
                            "sens": default_profile.get("sens"),  # Insulin sensitivity schedule
                            "basal": default_profile.get("basal"),  # Basal schedule
                            "target_low": default_profile.get("target_low"),  # Target BG low
                            "target_high": default_profile.get("target_high"),  # Target BG high
                            "units": default_profile.get("units"),  # BG units (mg/dl or mmol/L)
                            "timezone": default_profile.get("timezone"),
                        }
                    )

            transformed.append(transformed_profile)

        return transformed

    def _transform_devicestatus(self, devicestatus: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform device status.

        Args:
            devicestatus: The raw device status entries

        Returns:
            Transformed device status entries
        """
        if not devicestatus:
            return []

        logger.info(f"Transforming {len(devicestatus)} device status entries")
        transformed = []

        for status in devicestatus:
            transformed_status = {
                "id": status.get("_id"),
                "created_at": self._parse_nightscout_date(status.get("created_at")),
                "device": status.get("device"),
            }

            # Extract pump information if available
            if "pump" in status:
                pump = status.get("pump", {})
                transformed_status["pump"] = {
                    "clock": pump.get("clock"),
                    "reservoir": pump.get("reservoir"),
                    "battery": pump.get("battery"),
                    "status": pump.get("status"),
                    "iob": pump.get("iob", {}).get("bolusiob")
                    if isinstance(pump.get("iob"), dict)
                    else pump.get("iob"),
                }

            # Extract uploader information if available
            if "uploader" in status:
                uploader = status.get("uploader", {})
                transformed_status["uploader"] = {
                    "battery": uploader.get("battery"),
                    "name": uploader.get("name"),
                }

            # Extract loop information if available
            if "loop" in status:
                loop = status.get("loop", {})
                transformed_status["loop"] = {
                    "enacted": loop.get("enacted"),
                    "predicted": loop.get("predicted"),
                    "iob": loop.get("iob", {}).get("netInsulin") if isinstance(loop.get("iob"), dict) else None,
                    "cob": loop.get("cob", {}).get("cob") if isinstance(loop.get("cob"), dict) else None,
                }

            transformed.append(transformed_status)

        return transformed

    @staticmethod
    def _parse_nightscout_date(date_value: int | str | None) -> datetime | None:
        """Parse Nightscout date values to Python datetime.

        Args:
            date_value: The date value to parse (can be timestamp in ms or ISO string)

        Returns:
            Parsed datetime or None if parsing fails
        """
        if not date_value:
            return None

        try:
            if isinstance(date_value, int):
                # Timestamp in milliseconds
                return datetime.fromtimestamp(date_value / 1000.0)
            elif isinstance(date_value, str):
                # ISO format date string
                return datetime.fromisoformat(date_value.replace("Z", "+00:00"))
            else:
                logger.warning(f"Unknown date format: {date_value}")
                return None
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse date '{date_value}': {str(e)}")
            return None
