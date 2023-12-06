import time
from typing import Any

import numpy as np
import polars as pl
from geopandas import GeoDataFrame
from pandas.io.sql import read_sql
from shapely.geometry import Point
from sqlalchemy.sql import text

from src.core.config import settings
from src.db.db import Database

# from src.db import models
# from src.db.session import legacy_engine
from src.schemas.isochrone import IIsochroneActiveMobility


class CRUDIsochrone:
    def __init__(self):
        self.segments_df = {}

        self.class_walking = """
            secondary,tertiary,residential,
            livingStreet,trunk,unclassified,
            parkingAisle,driveway,pedestrian,
            footway,steps,track,bridleway,
            unknown
        """

        self.class_bicycle = """
            secondary,tertiary,residential,
            livingStreet,trunk,unclassified,
            parkingAisle,driveway,pedestrian,
            track,cycleway,bridleway,unknown
        """

        self.segment_schema = {
            "id": pl.Int32,
            "length_m": pl.Float64,
            "length_3857": pl.Float64,
            "class_": pl.Utf8,
            "impedance_slope": pl.Float64,
            "impedance_slope_reverse": pl.Float64,
            "impedance_surface": pl.Float32,
            "coordinates_3857": pl.List(pl.List(pl.Float64)),
            "source": pl.Int32,
            "target": pl.Int32,
            "tags": pl.Utf8,
            "h3_3": pl.Int32,
            "h3_5": pl.Int32,
        }

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

                self.segments_df[h3_index] = pl.read_database_uri(
                    query=f"""
                        SELECT
                            id, length_m, length_3857,
                            class_, impedance_slope, impedance_slope_reverse,
                            impedance_surface, coordinates_3857, source,
                            target, CAST(tags AS TEXT) AS tags, h3_3, h3_5
                        FROM temporal.segment
                        WHERE h3_3 = {h3_index}
                    """,
                    uri=settings.POSTGRES_DATABASE_URI,
                    schema_overrides=self.segment_schema,
                )
        except Exception as e:
            print(e)

        print(
            f"Network loaded into polars. Time taken: {time.time() - start_time} sec."
        )

    def read_network(self, db, obj_in: IIsochroneActiveMobility) -> Any:
        """Read relevant sub-network for Isochrone calculation from polars dataframe."""

        start_time = time.time()

        # Get valid segment classes based on transport mode
        valid_segment_classes = (
            self.class_walking
            if obj_in.routing_type == "walking"
            else self.class_bicycle
        )

        # Get number of points for which we need to calculate the isochrone
        num_points = db.select("SELECT count(*) FROM temporal.isochrone_input;")[0][0]

        # Compute buffer distance for identifying relevant H3_5 cells
        buffer_dist = obj_in.travel_cost.max_traveltime * (
            (obj_in.travel_cost.speed * 1000) / 60
        )

        # Identify H3_3 & H3_5 cells relevant to this isochrone calculation
        h3_3_cells = set()
        h3_5_cells = set()

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

        sub_network = pl.DataFrame()

        # Get relevant segments & connectors
        for h3_3 in h3_3_cells:
            sub_df = self.segments_df[h3_3].filter(
                pl.col("h3_5").is_in(h3_5_cells)
                & pl.col("class_").is_in(valid_segment_classes.split(","))
            )
            if sub_network.width > 0:
                sub_network.extend(sub_df)
            else:
                sub_network = sub_df

        # Create necessary artifical segments and add them to our sub network
        sql_get_artificial_segments = f"""
            SELECT
                point_id,
                old_id,
                id, length_m, length_3857, class_, impedance_slope,
                impedance_slope_reverse, impedance_surface,
                coordinates_3857, source, target, tags, h3_3, h3_5
            FROM temporal.get_artificial_segments({num_points}, '{valid_segment_classes}');
        """
        artificial_segments = db.select(sql_get_artificial_segments)

        for a_seg in artificial_segments:
            if a_seg[0] is not None:
                sub_network = sub_network.filter(pl.col("id") != a_seg[1])

            new_df = pl.DataFrame(
                [
                    {
                        "id": a_seg[2],
                        "length_m": a_seg[3],
                        "length_3857": a_seg[4],
                        "class_": a_seg[5],
                        "impedance_slope": a_seg[6],
                        "impedance_slope_reverse": a_seg[7],
                        "impedance_surface": a_seg[8],
                        "coordinates_3857": a_seg[9],
                        "source": a_seg[10],
                        "target": a_seg[11],
                        "tags": a_seg[12],
                        "h3_3": a_seg[13],
                        "h3_5": a_seg[14],
                    }
                ],
                schema_overrides=self.segment_schema,
            )
            sub_network.extend(new_df)

        # Replace all NULL values in the impedance columns with 0
        sub_network = sub_network.with_columns(pl.col("impedance_slope").fill_null(0))
        sub_network = sub_network.with_columns(
            pl.col("impedance_slope_reverse").fill_null(0)
        )
        sub_network = sub_network.with_columns(pl.col("impedance_surface").fill_null(0))

        sub_network = self.compute_segment_cost(
            sub_network,
            obj_in.routing_type,
            obj_in.travel_cost.speed,
        )

        print(sub_network.shape)
        print(sub_network.head())

        sub_network = sub_network.select(
            "id",
            "source",
            "target",
            "cost",
            "reverse_cost",
            pl.col("length_3857").alias("length"),
        ).to_dict(as_series=False)

        sub_network["id"] = np.fromiter(sub_network["id"], dtype=np.int64)
        sub_network["source"] = np.fromiter(sub_network["source"], dtype=np.int64)
        sub_network["target"] = np.fromiter(sub_network["target"], dtype=np.int64)
        sub_network["cost"] = np.fromiter(sub_network["cost"], dtype=np.double)
        sub_network["reverse_cost"] = np.fromiter(
            sub_network["reverse_cost"], dtype=np.double
        )
        sub_network["length"] = np.fromiter(sub_network["length"], dtype=np.double)

        print(f"Processing artificial segments time: {time.time() - start_time} sec.")

        return sub_network

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

    def compute_segment_cost(self, sub_network, mode, speed):
        """Compute the cost of a segment based on the mode, speed, impedance, etc."""

        if mode == "walking":
            return sub_network.with_columns(
                (pl.col("length_m") / speed).alias("cost"),
                (pl.col("length_m") / speed).alias("reverse_cost"),
            )
        elif mode == "bicycle":
            return sub_network.with_columns(
                pl.when(pl.col("class_") != "pedestrian")
                .then(
                    (
                        pl.col("length_m")
                        * (1 + pl.col("impedance_slope") + pl.col("impedance_surface"))
                    )
                    / speed
                )
                .otherwise(pl.col("length_m") / speed)
                .alias("cost"),
                pl.when(pl.col("class_") != "pedestrian")
                .then(
                    (
                        pl.col("length_m")
                        * (
                            1
                            + pl.col("impedance_slope_reverse")
                            + pl.col("impedance_surface")
                        )
                    )
                    / speed
                )
                .otherwise(pl.col("length_m") / speed)
                .alias("reverse_cost"),
            )
        elif mode == "pedelec":
            return sub_network.with_columns(
                pl.when(pl.col("class_") != "pedestrian")
                .then((pl.col("length_m") * (1 + pl.col("impedance_surface"))) / speed)
                .otherwise(pl.col("length_m") / speed)
                .alias("cost"),
                pl.when(pl.col("class_") != "pedestrian")
                .then((pl.col("length_m") * (1 + pl.col("impedance_surface"))) / speed)
                .otherwise(pl.col("length_m") / speed)
                .alias("reverse_cost"),
            )
        else:
            return None

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
            routing_type="pedelec",
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
