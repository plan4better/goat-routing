from enum import Enum

from pydantic import BaseModel, Field

from src.schemas.isochrone import TravelTimeCostActiveMobility


class RoutingActiveMobilityType(str, Enum):
    """Routing active mobility type schema."""

    walking = "walking"
    bicycle = "bicycle"
    pedelec = "pedelec"


ROUTING_COST_CONFIG = {
    RoutingActiveMobilityType.walking.value: TravelTimeCostActiveMobility(
        max_traveltime=30, traveltime_step=1, speed=5
    ),
    RoutingActiveMobilityType.bicycle.value: TravelTimeCostActiveMobility(
        max_traveltime=30, traveltime_step=1, speed=15
    ),
    RoutingActiveMobilityType.pedelec.value: TravelTimeCostActiveMobility(
        max_traveltime=30, traveltime_step=1, speed=23
    ),
}


class IHeatmapActiveMobility(BaseModel):
    """Model for the active mobility heatmap"""

    h3_6_cell: str = Field(
        ...,
        title="H3_6 Cell",
        description="H3_6 cell for which to compute isochrones.",
    )
    routing_type: RoutingActiveMobilityType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the heatmap.",
    )
