"""Data models for Nightscout workflow."""

from datetime import datetime

from pydantic import BaseModel, Field


class GlucoseReading(BaseModel):
    """Model for a single glucose reading."""

    timestamp: datetime = Field(..., description="Time of the reading")
    glucose: float = Field(..., description="Glucose value in mg/dL")
    device: str = Field(..., description="Device that took the reading")
    type: str = Field(..., description="Type of reading (e.g., 'sgv', 'mbg')")
    direction: str | None = Field(None, description="Trend direction")
    noise: int | None = Field(None, description="Noise level of the reading")
    filtered: float | None = Field(None, description="Filtered glucose value")
    unfiltered: float | None = Field(None, description="Unfiltered glucose value")
    rssi: int | None = Field(None, description="Signal strength")


class GlucoseData(BaseModel):
    """Collection of glucose readings."""

    readings: list[GlucoseReading]
    start_time: datetime
    end_time: datetime
    device_info: dict
