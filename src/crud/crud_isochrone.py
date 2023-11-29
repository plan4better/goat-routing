import time
from typing import Any

import numpy as np
import polars as pl
from geopandas import GeoDataFrame
from pandas.io.sql import read_sql
from shapely.geometry import Point
from sqlalchemy.sql import text

import src.crud.crud_isochrone_helper as helper
from src.core.config import settings
from src.db.db import Database

# from src.db import models
# from src.db.session import legacy_engine
from src.schemas.isochrone import IIsochroneActiveMobility


class CRUDIsochrone:
    def __init__(self):
        self.segments_df = {}
        self.connectors_df = {}

    def init_network(self):
        """Initialize the network by loading it into memory using polars data frames."""

        start_time = time.time()
        print("Loading network into polars...")

        # Get network H3 cells
        h3_3_grid = []
        db = Database(settings.POSTGRES_DATABASE_URI)
        try:
            sql_get_h3_3_grid = f"""
                WITH region AS (
                    SELECT geom from {settings.NETWORK_REGION_TABLE}
                )
                SELECT g.h3_short FROM region r,
                LATERAL temporal.fill_polygon_h3_3(r.geom) g;
            """
            for h3_index in db.select(sql_get_h3_3_grid):
                h3_3_grid.append(h3_index[0])
                break
        except Exception as e:
            print(e)
        finally:
            db.conn.close()

        # Load segments & connectors into polars data frames
        try:
            for h3_index in h3_3_grid:
                print(f"Loading network for H3 index {h3_index}.")

                self.segments_df[h3_index] = helper.retrieve_segments(
                    settings.POSTGRES_DATABASE_URI, h3_index
                )
                self.connectors_df[h3_index] = helper.retrieve_connectors(
                    settings.POSTGRES_DATABASE_URI, h3_index
                )
        except Exception as e:
            print(e)

        print(
            f"Network loaded into polars. Time taken: {time.time() - start_time} sec."
        )

    def read_network(self, db, obj_in: IIsochroneActiveMobility) -> Any:
        # Compute buffer distance for identifying relevant H3_5 cells
        buffer_dist = obj_in.travel_cost.max_traveltime * (
            (obj_in.travel_cost.speed * 1000) / 60
        )

        start = time.time()

        # Identify H3_3 & H3_5 cells relevant to this isochrone calculation
        h3_3_cells = set()
        h3_5_cells = set()

        x_points = obj_in.starting_points.longitude
        y_points = obj_in.starting_points.latitude
        for i in range(len(x_points)):
            x, y = x_points[i], y_points[i]

            sql_get_relevant_cells = f"""
                WITH point AS (
                    SELECT geom FROM temporal.isochrone_input
                ),
                num_cells AS (
                    SELECT generate_series(0, CASE WHEN value < 1.0 THEN 0 ELSE ROUND(value)::int END) AS value
                    FROM (SELECT ({buffer_dist} / (h3_get_hexagon_edge_length_avg(5, 'm') * 2)) AS value) sub
                ),
                cells AS (
                    SELECT DISTINCT h3_grid_ring_unsafe(h3_lat_lng_to_cell(point.geom::point, 5), sub.value) AS h3_index
                    FROM point,
                    LATERAL (SELECT * FROM num_cells) sub
                )
                SELECT to_short_h3_3(h3_cell_to_parent(h3_index, 3)::bigint) AS h3_3, ARRAY_AGG(to_short_h3_5(h3_index::bigint)) AS h3_5
                FROM cells
                GROUP BY h3_3;
            """
            result = db.select(sql_get_relevant_cells)

            for h3_3_cell in result:
                h3_3_cells.add(h3_3_cell[0])
                for h3_5_cell in h3_3_cell[1]:
                    h3_5_cells.add(h3_5_cell)

        print(f"{h3_5_cells}")

        sub_network = pl.DataFrame()

        # Get relevant segments & connectors
        for h3_3 in h3_3_cells:
            sub_df = self.segments_df[h3_3].filter(pl.col("h3_5").is_in(h3_5_cells))
            if sub_network.width > 0:
                sub_network.extend(sub_df)
            else:
                sub_network = sub_df

        print(sub_network.shape)

        print(f"Time taken: {time.time() - start} sec.")

        return

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
            routing_profile = (
                obj_in.mode.value + "_" + obj_in.settings.walking_profile.value
            )

        if obj_in.mode.value == IsochroneMode.CYCLING.value:
            routing_profile = (
                obj_in.mode.value + "_" + obj_in.settings.cycling_profile.value
            )

        x = y = None
        if (
            isochrone_type == IsochroneTypeEnum.multi.value
            or isochrone_type == IsochroneTypeEnum.heatmap.value
        ):
            if isinstance(obj_in.starting_point.input[0], IsochroneStartingPointCoord):
                x = [point.lon for point in obj_in.starting_point.input]
                y = [point.lat for point in obj_in.starting_point.input]
            else:
                starting_points = self.starting_points_opportunities(
                    current_user, db, obj_in
                )
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

    """def calculate(
        self,
        db: AsyncSession,
        obj_in: IsochroneDTO,
        current_user: models.User,
        study_area_bounds,
    ) -> Any:
        \"""
        Calculate the isochrone for a given location and time
        \"""
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
        # Network: return the reached network edges with the respective travel times
        # Polygon: return the Jsoline polygon (use function from jsoline)
        # Grid: return the traveltime grid

        return result"""

    def save_result():
        # TODO: The results should be saved to the user_data database. Depending on the result type we need a fast method to save the data.
        # It could be faster to avoid saving the data in a geospatial format and just save the data as a list of coordinates first into a temp table.
        # In that case we can make use of more performant connectors such as ADBC. We can then convert the data to valid geometries inside the database.
        pass


isochrone = CRUDIsochrone()
isochrone.init_network()

db = Database(settings.POSTGRES_DATABASE_URI)
try:
    isochrone.read_network(
        db,
        IIsochroneActiveMobility(
            starting_points={"latitude": [10.4780595], "longitude": [52.7052410]},
            routing_type="walking",
            travel_cost={
                "max_traveltime": 45,
                "traveltime_step": 10,
                "speed": 25,
            },
            isochrone_type="polygon",
            polygon_difference=True,
            result_table="polygon_744e4fd1685c495c8b02efebce875359",
            layer_id="744e4fd1-685c-495c-8b02-efebce875359",
        ),
    )
except Exception as e:
    print(e)
    raise e
finally:
    db.close()
