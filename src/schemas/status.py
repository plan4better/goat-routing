from enum import Enum


class ProcessingStatus(str, Enum):
    """Pocessing status schema."""

    in_progress = "in_progress"
    success = "success"
    failure = "failure"
