from enum import Enum


class ProcessingStatus(str, Enum):
    """Pocessing status schema."""

    in_progress = "in_progress"  # Isochrone computation is in progress
    success = "success"  # Isochrone computation was successful
    failure = "failure"  # Isochrone computation failed, reason unknown
    disconnected_origin = "disconnected_origin"  # Starting point(s) are not connected to the street network
