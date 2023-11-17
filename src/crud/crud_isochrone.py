from typing import Any
import numpy as np
from geopandas import GeoDataFrame
from pandas.io.sql import read_sql
from shapely.geometry import Point
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from src.core.isochrone import compute_isochrone
from src.db import models
from src.db.session import legacy_engine
from src.schemas.isochrone import (
    IIsochroneActiveMobility,
    IsochroneMode,
    IsochroneStartingPointCoord,
    IsochroneTypeEnum,
)


class CRUDIsochrone:
    def init_network():
        #TODO: Add function here to read the data from database into polars.
        # We should save the geofence_active_mobility in the goat database and read the network for the respective area in chunks of h3 cells.
        # We could have the table dynamically defined in the config.py so we could ready other (smaller) extents for testing.
        # From what we know at the moment we should have one polars df for each h3-res3 cell.
        pass


    def read_network(
        self, db, obj_in: IIsochroneActiveMobility
    ) -> Any:
        # TODO: Once the data is inside polary we can start with writing the functions to read from polary 
        # The first thing is that we need to identify the h3-res3 that are needed for the isochrone calculation.
        # One idea that can work is that we get the h3-res5 grids for the starting points + a buffer distance that is defined
        # as flying-bird distance from the max travel time and speed.
        # There are functions in h3 that can get the neighbors of a given h3 cell and we can derive the size of the respective grid resolution with another function.
        # After we have the h3-res5 cells we can get the h3-res3 parent cells.
        # With both h3-res3 and h3-res5 cells we can query the polars df and get the data for the respective cells without doing any spatial intersections.

        # TODO: We need to read the scenario network dynamically from DB if a scenario is selected.
        sql_text = ""
        if isochrone_type == IsochroneTypeEnum.single.value:
            sql_text = """SELECT id, source, target, cost, reverse_cost, coordinates_3857 as geom, length_3857 AS length, starting_ids, starting_geoms
            FROM basic.fetch_network_routing(ARRAY[:x],ARRAY[:y], :max_cutoff, :speed, :modus, :scenario_id, :routing_profile)
            """
        elif isochrone_type == IsochroneTypeEnum.multi.value:
            sql_text = """SELECT id, source, target, cost, reverse_cost, coordinates_3857 as geom, length_3857 AS length, starting_ids, starting_geoms
            FROM basic.fetch_network_routing_multi(:x,:y, :max_cutoff, :speed, :modus, :scenario_id, :routing_profile)
            """
        elif isochrone_type == IsochroneTypeEnum.heatmap.value:
            sql_text = """
            SELECT id, source, target, cost, reverse_cost, coordinates_3857 as geom, length_3857 AS length, starting_ids, starting_geoms
            FROM basic.fetch_network_routing_heatmap(:x,:y, :max_cutoff, :speed, :modus, :scenario_id, :routing_profile, :table_prefix)
            """

        read_network_sql = text(sql_text)
        routing_profile = None
        if obj_in.mode.value == IsochroneMode.WALKING.value:
            routing_profile = obj_in.mode.value + "_" + obj_in.settings.walking_profile.value

        if obj_in.mode.value == IsochroneMode.CYCLING.value:
            routing_profile = obj_in.mode.value + "_" + obj_in.settings.cycling_profile.value

        x = y = None
        if (
            isochrone_type == IsochroneTypeEnum.multi.value
            or isochrone_type == IsochroneTypeEnum.heatmap.value
        ):
            if isinstance(obj_in.starting_point.input[0], IsochroneStartingPointCoord):
                x = [point.lon for point in obj_in.starting_point.input]
                y = [point.lat for point in obj_in.starting_point.input]
            else:
                starting_points = self.starting_points_opportunities(current_user, db, obj_in)
                x = starting_points[0][0]
                y = starting_points[0][1]
        else:
            x = obj_in.starting_point.input[0].lon
            y = obj_in.starting_point.input[0].lat

        edges_network = read_sql(
            read_network_sql,
            legacy_engine,
            params={
                "x": x,
                "y": y,
                "max_cutoff": obj_in.settings.travel_time * 60,  # in seconds
                "speed": obj_in.settings.speed / 3.6,
                "modus": obj_in.scenario.modus.value,
                "scenario_id": obj_in.scenario.id,
                "routing_profile": routing_profile,
                "table_prefix": table_prefix,
            },
        )
        starting_ids = edges_network.iloc[0].starting_ids
        if len(obj_in.starting_point.input) == 1 and isinstance(
            obj_in.starting_point.input[0], IsochroneStartingPointCoord
        ):
            starting_point_geom = str(
                GeoDataFrame(
                    {"geometry": Point(edges_network.iloc[-1:]["geom"].values[0][0])},
                    crs="EPSG:3857",
                    index=[0],
                )
                .to_crs("EPSG:4326")
                .to_wkt()["geometry"]
                .iloc[0]
            )
        else:
            starting_point_geom = str(edges_network["starting_geoms"].iloc[0])

        edges_network = edges_network.drop(["starting_ids", "starting_geoms"], axis=1)

        if (
            isochrone_type == IsochroneTypeEnum.single.value
            or isochrone_type == IsochroneTypeEnum.multi.value
        ):
            obj_starting_point = models.IsochroneCalculation(
                calculation_type=isochrone_type,
                user_id=current_user.id,
                scenario_id=None if obj_in.scenario.id == 0 else obj_in.scenario.id,
                starting_point=starting_point_geom,
                routing_profile=routing_profile,
                speed=obj_in.settings.speed,
                modus=obj_in.scenario.modus.value,
                parent_id=None,
            )

            db.add(obj_starting_point)
            db.commit()

        # return edges_network and obj_starting_point
        edges_network.astype(
            {
                "id": np.int64,
                "source": np.int64,
                "target": np.int64,
                "cost": np.double,
                "reverse_cost": np.double,
                "length": np.double,
            }
        )
        return edges_network, starting_ids, starting_point_geom


    def calculate(
        self, db: AsyncSession, obj_in: IsochroneDTO, current_user: models.User, study_area_bounds
    ) -> Any:
        """
        Calculate the isochrone for a given location and time
        """
        grid = None
        result = None
        network = None
        if obj_in.settings.travel_time is not None:
            pass

        if len(obj_in.starting_point.input) == 1 and isinstance(
            obj_in.starting_point.input[0], IsochroneStartingPointCoord
        ):
            isochrone_type = IsochroneTypeEnum.single.value
        else:
            isochrone_type = IsochroneTypeEnum.multi.value

        # Read network from in memory DB
        network, starting_ids, starting_point_geom = self.read_network(
            db, obj_in, current_user, isochrone_type
        )
        network = network.iloc[1:, :]
        grid, network = compute_isochrone(
            network,
            starting_ids,
            obj_in.settings.travel_time,
            obj_in.settings.speed / 3.6,
            obj_in.output.resolution,
        )

        # TODO: The idea is to generate here different return types
        #Network: return the reached network edges with the respective travel times
        #Polygon: return the Jsoline polygon (use function from jsoline)
        #Grid: return the traveltime grid

        return result

    def save_result():
        #TODO: The results should be saved to the user_data database. Depending on the result type we need a fast method to save the data.
        # It could be faster to avoid saving the data in a geospatial format and just save the data as a list of coordinates first into a temp table.
        # In that case we can make use of more performant connectors such as ADBC. We can then convert the data to valid geometries inside the database.
        pass


isochrone = CRUDIsochrone()
