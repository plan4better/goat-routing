from enum import Enum
from typing import List
from uuid import UUID

import polars as pl
from pydantic import BaseModel, Field, validator

SEGMENT_DATA_SCHEMA = {
    "id": pl.Int64,
    "length_m": pl.Float64,
    "length_3857": pl.Float64,
    "class_": pl.Utf8,
    "impedance_slope": pl.Float64,
    "impedance_slope_reverse": pl.Float64,
    "impedance_surface": pl.Float32,
    "coordinates_3857": pl.Utf8,
    "source": pl.Int64,
    "target": pl.Int64,
    "tags": pl.Utf8,
    "h3_3": pl.Int32,
    "h3_6": pl.Int32,
}

VALID_WALKING_CLASSES = [
    "secondary",
    "tertiary",
    "residential",
    "livingStreet",
    "trunk",
    "unclassified",
    "parkingAisle",
    "driveway",
    "pedestrian",
    "footway",
    "steps",
    "track",
    "bridleway",
    "unknown",
]

VALID_BICYCLE_CLASSES = [
    "secondary",
    "tertiary",
    "residential",
    "livingStreet",
    "trunk",
    "unclassified",
    "parkingAisle",
    "driveway",
    "pedestrian",
    "track",
    "cycleway",
    "bridleway",
    "unknown",
]


class IsochroneType(str, Enum):
    """Isochrone type schema."""

    polygon = "polygon"
    network = "network"
    rectangular_grid = "rectangular_grid"


class IsochroneStartingPoints(BaseModel):
    """Base model for isochrone attributes."""

    latitude: List[float] | None = Field(
        None,
        title="Latitude",
        description="The latitude of the isochrone center.",
    )
    longitude: List[float] | None = Field(
        None,
        title="Longitude",
        description="The longitude of the isochrone center.",
    )


class RoutingActiveMobilityType(str, Enum):
    """Routing active mobility type schema."""

    walking = "walking"
    bicycle = "bicycle"
    pedelec = "pedelec"


class TravelTimeCostActiveMobility(BaseModel):
    """Travel time cost schema."""

    max_traveltime: int = Field(
        ...,
        title="Max Travel Time",
        description="The maximum travel time in minutes.",
        ge=1,
        le=45,
    )
    traveltime_step: int = Field(
        ...,
        title="Travel Time Step",
        description="The travel time step in minutes.",
    )
    speed: int = Field(
        ...,
        title="Speed",
        description="The speed in km/h.",
        ge=1,
        le=25,
    )


# TODO: Check how to treat miles
class TravelDistanceCostActiveMobility(BaseModel):
    """Travel distance cost schema."""

    max_distance: int = Field(
        ...,
        title="Max Distance",
        description="The maximum distance in meters.",
        ge=50,
        le=20000,
    )
    distance_step: int = Field(
        ...,
        title="Distance Step",
        description="The distance step in meters.",
    )

    # Make sure that the distance step can be divided by 50 m
    @validator("distance_step", pre=True, always=True)
    def distance_step_divisible_by_50(cls, v):
        if v % 50 != 0:
            raise ValueError("The distance step must be divisible by 50 m.")
        return v


class IIsochroneActiveMobility(BaseModel):
    """Model for the active mobility isochrone"""

    starting_points: IsochroneStartingPoints = Field(
        ...,
        title="Starting Points",
        description="The starting points of the isochrone.",
    )
    routing_type: RoutingActiveMobilityType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the isochrone.",
    )
    travel_cost: TravelTimeCostActiveMobility | TravelDistanceCostActiveMobility = (
        Field(
            ...,
            title="Travel Cost",
            description="The travel cost of the isochrone.",
        )
    )
    scenario_id: UUID | None = Field(
        None,
        title="Scenario ID",
        description="The ID of the scenario that is used for the routing.",
    )
    isochrone_type: IsochroneType = Field(
        ...,
        title="Return Type",
        description="The return type of the isochrone.",
    )
    polygon_difference: bool | None = Field(
        None,
        title="Polygon Difference",
        description="If true, the polygons returned will be the geometrical difference of two following calculations.",
    )
    result_table: str = Field(
        ...,
        title="Result Table",
        description="The table name the results should be saved.",
    )
    layer_id: UUID | None = Field(
        ...,
        title="Layer ID",
        description="The ID of the layer the results should be saved.",
    )

    # Check that polygon difference exists if isochrone type is polygon
    @validator("polygon_difference", pre=True, always=True)
    def check_polygon_difference(cls, v, values):
        if values["isochrone_type"] == IsochroneType.polygon.value and v is None:
            raise ValueError(
                "The polygon difference must be set if the isochrone type is polygon."
            )
        return v

    # Check that polygon difference is not specified if isochrone type is not polygon
    @validator("polygon_difference", pre=True, always=True)
    def check_polygon_difference_not_specified(cls, v, values):
        if values["isochrone_type"] != IsochroneType.polygon.value and v is not None:
            raise ValueError(
                "The polygon difference must not be set if the isochrone type is not polygon."
            )
        return v


request_examples = {
    "isochrone_active_mobility": {
        # 1. Single isochrone for walking
        "single_point_walking": {
            "summary": "Single point isochrone walking",
            "value": {
                "starting_points": {"latitude": [13.4050], "longitude": [52.5200]},
                "routing_type": "walking",
                "travel_cost": {
                    "max_traveltime": 30,
                    "traveltime_step": 10,
                    "speed": 5,
                },
                "isochrone_type": "polygon",
                "polygon_difference": True,
                "result_table": "polygon_744e4fd1685c495c8b02efebce875359",
                "layer_id": "744e4fd1-685c-495c-8b02-efebce875359",
            },
        },
        # 2. Single isochrone for cycling
        "single_point_cycling": {
            "summary": "Single point isochrone cycling",
            "value": {
                "starting_points": {"latitude": [13.4050], "longitude": [52.5200]},
                "routing_type": "bicycle",
                "travel_cost": {
                    "max_traveltime": 15,
                    "traveltime_step": 5,
                    "speed": 15,
                },
                "isochrone_type": "polygon",
                "polygon_difference": True,
                "result_table": "polygon_744e4fd1685c495c8b02efebce875359",
                "layer_id": "744e4fd1-685c-495c-8b02-efebce875359",
            },
        },
        # 3. Single isochrone for walking with scenario
        "single_point_walking_scenario": {
            "summary": "Single point isochrone walking",
            "value": {
                "starting_points": {"latitude": [13.4050], "longitude": [52.5200]},
                "routing_type": "walking",
                "travel_cost": {
                    "max_traveltime": 30,
                    "traveltime_step": 10,
                    "speed": 5,
                },
                "isochrone_type": "polygon",
                "polygon_difference": True,
                "scenario_id": "e7dcaae4-1750-49b7-89a5-9510bf2761ad",
                "result_table": "polygon_744e4fd1685c495c8b02efebce875359",
                "layer_id": "744e4fd1-685c-495c-8b02-efebce875359",
            },
        },
        # 4. Multi-isochrone walking with more than one starting point
        "multi_point_walking": {
            "summary": "Multi point isochrone walking",
            "value": {
                "starting_points": {
                    "latitude": [
                        13.4050,
                        13.4060,
                        13.4070,
                        13.4080,
                        13.4090,
                        13.4100,
                        13.4110,
                        13.4120,
                        13.4130,
                        13.4140,
                    ],
                    "longitude": [
                        52.5200,
                        52.5210,
                        52.5220,
                        52.5230,
                        52.5240,
                        52.5250,
                        52.5260,
                        52.5270,
                        52.5280,
                        52.5290,
                    ],
                },
                "routing_type": "walking",
                "travel_cost": {
                    "max_traveltime": 30,
                    "traveltime_step": 10,
                    "speed": 5,
                },
                "isochrone_type": "polygon",
                "polygon_difference": True,
                "result_table": "polygon_744e4fd1685c495c8b02efebce875359",
                "layer_id": "744e4fd1-685c-495c-8b02-efebce875359",
            },
        },
        # 5. Multi-isochrone cycling with more than one starting point
        "multi_point_cycling": {
            "summary": "Multi point isochrone cycling",
            "value": {
                "starting_points": {
                    "latitude": [
                        13.4050,
                        13.4060,
                        13.4070,
                        13.4080,
                        13.4090,
                        13.4100,
                        13.4110,
                        13.4120,
                        13.4130,
                        13.4140,
                    ],
                    "longitude": [
                        52.5200,
                        52.5210,
                        52.5220,
                        52.5230,
                        52.5240,
                        52.5250,
                        52.5260,
                        52.5270,
                        52.5280,
                        52.5290,
                    ],
                },
                "routing_type": "bicycle",
                "travel_cost": {
                    "max_traveltime": 15,
                    "traveltime_step": 5,
                    "speed": 15,
                },
                "isochrone_type": "polygon",
                "polygon_difference": True,
                "result_table": "polygon_744e4fd1685c495c8b02efebce875359",
                "layer_id": "744e4fd1-685c-495c-8b02-efebce875359",
            },
        },
    }
}
