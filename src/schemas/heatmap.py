from src.schemas.catchment_area import (
    RoutingActiveMobilityType,
    TravelTimeCostActiveMobility,
)

ROUTING_COST_CONFIG = {
    RoutingActiveMobilityType.walking.value: TravelTimeCostActiveMobility(
        max_traveltime=30, steps=30, speed=5
    ),
    RoutingActiveMobilityType.bicycle.value: TravelTimeCostActiveMobility(
        max_traveltime=30, steps=30, speed=15
    ),
    RoutingActiveMobilityType.pedelec.value: TravelTimeCostActiveMobility(
        max_traveltime=30, steps=30, speed=23
    ),
}
