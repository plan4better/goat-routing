from src.schemas.isochrone import (
    RoutingActiveMobilityType,
    TravelTimeCostActiveMobility,
)

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
