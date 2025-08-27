from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class MotisMode(str, Enum):
    WALK = "WALK"
    BIKE = "BIKE"
    RENTAL = "RENTAL"
    CAR = "CAR"
    CAR_PARKING = "CAR_PARKING"
    CAR_DROPOFF = "CAR_DROPOFF"
    ODM = "ODM"
    FLEX = "FLEX"
    TRANSIT = "TRANSIT"
    TRAM = "TRAM"
    SUBWAY = "SUBWAY"
    FERRY = "FERRY"
    AIRPLANE = "AIRPLANE"
    METRO = "METRO"
    BUS = "BUS"
    COACH = "COACH"
    RAIL = "RAIL"
    HIGHSPEED_RAIL = "HIGHSPEED_RAIL"
    LONG_DISTANCE = "LONG_DISTANCE"
    NIGHT_RAIL = "NIGHT_RAIL"
    REGIONAL_FAST_RAIL = "REGIONAL_FAST_RAIL"
    REGIONAL_RAIL = "REGIONAL_RAIL"
    CABLE_CAR = "CABLE_CAR"
    FUNICULAR = "FUNICULAR"
    AREAL_LIFT = "AREAL_LIFT"
    OTHER = "OTHER"


class MotisPlace(BaseModel):
    name: str = Field(
        ...,
        title="Name",
        description="The name of the transit stop / PoI / address.",
    )
    lat: float = Field(
        ...,
        title="Latitude",
        description="The latitude of the place.",
    )
    lon: float = Field(
        ...,
        title="Longitude",
        description="The longitude of the place.",
    )
    level: float = Field(
        ...,
        title="Level",
        description="The level according to OpenStreetMap.",
    )


class IMotisPlan(BaseModel):
    """Model for the motis service request."""

    fromPlace: str = Field(
        ...,
        title="From Place",
        description="The starting place as a 'latitude,longitude[,level]' tuple OR stop ID.\
            (optional) level: the OSM level (default: 0).",
    )
    toPlace: str = Field(
        ...,
        title="To Place",
        description="The destination as a 'latitude,longitude[,level]' tuple OR stop ID.\
            (optional) level: the OSM level (default: 0).",
    )
    detailedTransfers: bool = Field(
        default=True,
        title="Detailed Transfers",
        description="true: Compute transfer polylines and step instructions (default).\
            false: Only return basic information (start time, end time, duration) for transfers.",
    )
    # Optional params
    time: Optional[str] = Field(
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        title="Time",
        description="Optional. Defaults to the current time <date-time>.\
              Departure time ($arriveBy=false) / arrival date ($arriveBy=true)",
    )
    arriveBy: Optional[bool] = Field(
        default=False,
        title="Arrive By time of arrival or time of departure",
        description="arriveBy=true: the parameters date and time refer to the arrival time.\
            arriveBy=false: the parameters date and time refer to the departure time",
    )
    transitModes: Optional[List[str]] = Field(
        default=MotisMode.TRANSIT.value,
        title="Modes",
        description="Array of strings representing the desired modes of transport (default=TRANSIT).",
    )
    ...


motis_request_examples = {
    "default": {
        "fromPlace": "50.7754385,6.0815102",
        "toPlace": "50.7753455,6.0838868",
        "detailedTransfers": "false",
    },
    "default_detailed": {
        "fromPlace": "50.7754385,6.0815102",
        "toPlace": "50.7753455,6.0838868",
        "detailedTransfers": "true",
        "time": "2025-08-28T08:00:00Z",
        "arriveBy": "true",
    },
    "default_bus": {
        "fromPlace": "50.7950,6.1260",
        "toPlace": "50.7651,6.0821",
        "detailedTransfers": "false",
        "time": "2025-08-28T08:00:00Z",
        "arriveBy": "false",
        "transitModes": [MotisMode.BUS.value],
    },
    "benchmark": {
        "fromPlace": "50.75082702571396,6.071697684563873",
        "toPlace": "50.78052725007913,6.070394639022425",
        "time": "2025-08-28T08:00:00Z",
        "arriveBy": "false",
    },
}


class DirectTrip(BaseModel):
    # The direct trip type (e.g., "WALK", "BIKE", "CAR")
    type: str = Field(
        ...,
        title="Direct Trip Type",
        description="The mode of transport for a direct, time-independent trip.",
    )


class LegGeometry(BaseModel):
    pass


class Rental(BaseModel):
    pass


class FareTransfer(BaseModel):
    pass


class Alert(BaseModel):
    pass


class Leg(BaseModel):
    mode: str = Field(
        ..., title="Mode", description="The mode of transport (e.g., 'WALK', 'BUS')."
    )
    from_place: MotisPlace = Field(
        ...,
        alias="from",
        title="From Place",
        description="The starting place of this leg.",
    )
    to_place: MotisPlace = Field(
        ..., alias="to", title="To Place", description="The destination of this leg."
    )
    duration: int = Field(
        ..., title="Duration", description="The duration of the leg in seconds."
    )
    start_time: datetime = Field(
        ...,
        alias="startTime",
        title="Start Time",
        description="The departure time of the leg.",
    )
    end_time: datetime = Field(
        ...,
        alias="endTime",
        title="End Time",
        description="The arrival time of the leg.",
    )
    scheduled_start_time: datetime = Field(
        ...,
        alias="scheduledStartTime",
        title="Scheduled Start Time",
        description="The scheduled departure time.",
    )
    scheduled_end_time: datetime = Field(
        ...,
        alias="scheduledEndTime",
        title="Scheduled End Time",
        description="The scheduled arrival time.",
    )
    real_time: bool = Field(
        ...,
        alias="realTime",
        title="Real Time",
        description="Indicates if the times are based on real-time data.",
    )
    scheduled: bool = Field(
        ...,
        title="Scheduled",
        description="Indicates if the trip is a scheduled service.",
    )
    distance: int = Field(..., title="Distance", description="The distance of the leg.")
    interline_with_previous_leg: bool = Field(
        ...,
        alias="interlineWithPreviousLeg",
        title="Interline with Previous Leg",
        description="Indicates an interline with the previous leg.",
    )
    headsign: str = Field(
        ..., title="Headsign", description="The headsign of the trip."
    )
    route_color: str = Field(
        ...,
        alias="routeColor",
        title="Route Color",
        description="The color of the route.",
    )
    route_text_color: str = Field(
        ...,
        alias="routeTextColor",
        title="Route Text Color",
        description="The text color of the route.",
    )
    route_type: int = Field(
        ...,
        alias="routeType",
        title="Route Type",
        description="The type of the route.",
    )
    agency_name: str = Field(
        ...,
        alias="agencyName",
        title="Agency Name",
        description="The name of the agency.",
    )
    agency_url: str = Field(
        ...,
        alias="agencyUrl",
        title="Agency URL",
        description="The URL of the agency.",
    )
    agency_id: str = Field(
        ..., alias="agencyId", title="Agency ID", description="The ID of the agency."
    )
    trip_id: str = Field(
        ..., alias="tripId", title="Trip ID", description="The ID of the trip."
    )
    route_short_name: str = Field(
        ...,
        alias="routeShortName",
        title="Route Short Name",
        description="The short name of the route.",
    )
    route_long_name: str = Field(
        ...,
        alias="routeLongName",
        title="Route Long Name",
        description="The long name of the route.",
    )
    trip_short_name: str = Field(
        ...,
        alias="tripShortName",
        title="Trip Short Name",
        description="The short name of the trip.",
    )
    display_name: str = Field(
        ...,
        alias="displayName",
        title="Display Name",
        description="The display name of the leg.",
    )
    cancelled: bool = Field(
        ..., title="Cancelled", description="Indicates if the trip is cancelled."
    )
    source: str = Field(..., title="Source", description="The source of the data.")
    intermediate_stops: List[MotisPlace] = Field(
        ...,
        alias="intermediateStops",
        title="Intermediate Stops",
        description="Array of intermediate stops.",
    )
    leg_geometry: LegGeometry = Field(
        ...,
        alias="legGeometry",
        title="Leg Geometry",
        description="The geometry of the leg.",
    )
    steps: List[dict] = Field(
        ..., title="Steps", description="Array of steps for the leg."
    )
    rental: Rental = Field(
        ..., title="Rental", description="Rental information for the leg."
    )
    fare_transfer_index: int = Field(
        ...,
        alias="fareTransferIndex",
        title="Fare Transfer Index",
        description="The index for fare transfer information.",
    )
    effective_fare_leg_index: int = Field(
        ...,
        alias="effectiveFareLegIndex",
        title="Effective Fare Leg Index",
        description="The effective fare leg index.",
    )
    alerts: List[Alert] = Field(
        ..., title="Alerts", description="Array of alerts for the leg."
    )
    looped_calendar_since: datetime = Field(
        ...,
        alias="loopedCalendarSince",
        title="Looped Calendar Since",
        description="Date and time when the calendar was looped.",
    )


class Itinerary(BaseModel):
    duration: int = Field(
        ...,
        title="Journey Duration",
        description="The total journey duration in seconds.",
    )
    start_time: datetime = Field(
        ..., title="Journey Departure Time", description="The journey departure time."
    )
    end_time: datetime = Field(
        ..., title="Journey Arrival Time", description="The journey arrival time."
    )
    transfers: int = Field(
        ...,
        title="Number of Transfers",
        description="The number of transfers this trip has.",
    )
    legs: List[Leg] = Field(
        ...,
        title="Journey Legs",
        description="Array of objects representing each leg of the journey.",
    )


class RequestParameters(BaseModel):
    __root__: Dict[str, str] = Field(
        ...,
        title="Request Parameters",
        description="A dictionary of variable string parameters.",
    )

    def __getitem__(self, item: str) -> str:
        return self.__root__[item]


class DebugStatistics(BaseModel):
    __root__: Dict[str, int] = Field(
        ...,
        title="Debug Output",
        description="A dictionary of variable integer parameters.",
    )

    def __getitem__(self, item: str) -> int:
        return self.__root__[item]


class MotisPlanResponse(BaseModel):
    request_parameters: RequestParameters = Field(
        ...,
        title="Request Parameters",
        description="The parameters of the routing request.",
    )
    debug_statistics: DebugStatistics = Field(
        ...,
        title="Debug Statistics",
        description="An object containing various debug statistics.",
    )
    from_place: MotisPlace = Field(
        ..., title="From Place", description="The starting place of the trip."
    )
    to_place: MotisPlace = Field(
        ..., title="To Place", description="The destination of the trip."
    )
    direct_trips: List[DirectTrip] = Field(
        ...,
        title="Direct Trips",
        description="Direct trips by WALK, BIKE, CAR, etc., without time-dependency. set to now if not set",
    )
    itineraries: List[Itinerary] = Field(
        ..., title="Itineraries", description="List of all available itineraries."
    )
    previous_page_cursor: str = Field(
        ...,
        title="Previous Page Cursor",
        description="A cursor to get the previous page of results.",
    )
    next_page_cursor: str = Field(
        ...,
        title="Next Page Cursor",
        description="A cursor to get the next page of results.",
    )
