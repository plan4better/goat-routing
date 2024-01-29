import math
import time

import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.isochrone import (
    compute_isochrone_h3_optimized,
    construct_adjacency_list_,
    dijkstra_h3,
    prepare_network_isochrone,
)
from src.crud.crud_isochrone import CRUDIsochrone, FetchRoutingNetwork
from src.schemas.heatmap import ROUTING_COST_CONFIG, IHeatmapActiveMobility
from src.schemas.isochrone import (
    IIsochroneActiveMobility,
    IsochroneStartingPoints,
    IsochroneType,
    TravelTimeCostActiveMobility,
)


class CRUDHeatmap:
    def __init__(self, db_connection: AsyncSession) -> None:
        self.db_connection = db_connection
        self.routing_network = None

    async def generate_isochrone_request(self, obj_in: IHeatmapActiveMobility):
        lat = []
        long = []

        sql_get_centroid = f"""
            WITH centroid AS (
                SELECT ST_SetSRID(h3_cell_to_lat_lng(h3_index)::geometry, 4326) AS geom
                FROM h3_cell_to_children('{obj_in.h3_6_cell}'::h3index, 10) AS h3_index
            )
            SELECT ST_X(geom), ST_Y(geom)
            FROM centroid;
        """
        result = (await self.db_connection.execute(sql_get_centroid)).fetchall()

        for centroid in result:
            lat.append(centroid[1])
            long.append(centroid[0])

        return IIsochroneActiveMobility(
            starting_points=IsochroneStartingPoints(
                latitude=lat,
                longitude=long,
            ),
            routing_type=obj_in.routing_type,
            travel_cost=ROUTING_COST_CONFIG[obj_in.routing_type.value],
            scenario_id=None,
            isochrone_type=IsochroneType.polygon,
            polygon_difference=True,
            result_table="",
            layer_id=None,
        )

    async def get_h3_10_grid(
        self,
        travel_cost: TravelTimeCostActiveMobility,
        point_coords: tuple,
    ):
        buffer_dist = travel_cost.max_traveltime * ((travel_cost.speed * 1000) / 60)

        sql_get_relevant_cells = f"""
            WITH point AS (
                SELECT ST_SetSRID(ST_MakePoint({point_coords[0]}, {point_coords[1]}), 4326) AS geom
            ),
            buffer AS (
                SELECT ST_Buffer(point.geom::geography, {buffer_dist})::geometry AS geom
                FROM point
            ),
            cells AS (
                SELECT h3_index, h3_short
                FROM buffer,
                LATERAL temporal.fill_polygon_h3_10(buffer.geom)
            )
            SELECT cells.*, ST_X(coords), ST_Y(coords)
            FROM cells,
            LATERAL h3_cell_to_lat_lng(h3_index) geom,
            LATERAL ST_Transform(ST_SetSRID(geom::geometry, 4326), 3857) coords;
        """
        result = (await self.db_connection.execute(sql_get_relevant_cells)).fetchall()

        h3_index = []
        h3_short = np.empty(len(result))
        x_centroids = np.empty(len(result))
        y_centroids = np.empty(len(result))
        for i in range(len(result)):
            h3_index.append(result[i][0])
            h3_short[i] = result[i][1]
            x_centroids[i] = result[i][2]
            y_centroids[i] = result[i][3]

        return h3_index, h3_short, x_centroids, y_centroids

    async def write_h3_index_cost_to_db(self, db_connection, h3_index, h3_cost):
        # await db_connection.execute("DROP TABLE IF EXISTS basic.heatmap_h3;")
        # sql_create_table = """
        #     CREATE TABLE basic.heatmap_h3 (
        #         id serial4 PRIMARY KEY,
        #         cost double precision,
        #         geom geometry(Polygon, 4326)
        #     );
        # """
        # await db_connection.execute(sql_create_table)
        # await db_connection.commit()

        insert_string = ""
        for i in range(len(h3_index)):
            if math.isnan(h3_cost[i]):
                continue

            insert_string += (
                f"({h3_cost[i]}, h3_cell_to_boundary('{h3_index[i]}')::geometry),"
            )
        await self.db_connection.execute(
            f"""
                INSERT INTO basic.heatmap_h3 (cost, geom)
                VALUES {insert_string.rstrip(",")};
            """
        )

        await db_connection.commit()

    async def write_to_db(self, db_connection, origin_h3_coords, h3_short, h3_cost):
        # await db_connection.execute("DROP TABLE IF EXISTS basic.heatmap_grid_walking;")
        # sql_create_table = """
        #     CREATE TABLE IF NOT EXISTS basic.heatmap_grid_walking (
        #         h3_orig bigint,
        #         h3_dest bigint[],
        #         cost int,
        #         h3_3 int
        #     );
        # """
        # await db_connection.execute(sql_create_table)

        cost_map = {}
        for i in range(len(h3_short)):
            if math.isnan(h3_cost[i]):
                continue
            if int(h3_cost[i]) not in cost_map:
                cost_map[int(h3_cost[i])] = []
            cost_map[int(h3_cost[i])].append(int(h3_short[i]))

        sql_get_h3_short = f"""
            WITH point AS (
                SELECT (ST_SetSRID(ST_MakePoint({origin_h3_coords[0]}, {origin_h3_coords[1]}), 4326))::point AS geom
            )
            SELECT to_short_h3_10(h3_lat_lng_to_cell(point.geom, 10)::bigint), to_short_h3_3(h3_lat_lng_to_cell(point.geom, 3)::bigint)
            FROM point;
        """
        h3_orig, h3_3 = (await db_connection.execute(sql_get_h3_short)).fetchall()[0]

        insert_string = ""
        for cost in cost_map:
            insert_string += f"({h3_orig}, ARRAY{cost_map[cost]}, {cost}, {h3_3}),"
        await db_connection.execute(
            f"""
                INSERT INTO basic.heatmap_grid_walking (h3_orig, h3_dest, cost, h3_3)
                VALUES {insert_string.rstrip(",")};
            """
        )

        await db_connection.commit()

    async def run(
        self,
        crud_isochrone: CRUDIsochrone,
        obj_in: IHeatmapActiveMobility,
    ):
        """Compute isochrones for the given request parameters."""

        # Fetch routing network (processed segments) and load into memory
        if self.routing_network is None:
            self.routing_network = await FetchRoutingNetwork(self.db_connection).fetch()
        routing_network = self.routing_network

        total_start = time.time()

        # Produce isochrone request to call isochrone CRUD functions
        isochrone_request = await self.generate_isochrone_request(obj_in)

        # Read & process routing network to extract relevant sub-network
        start_time = time.time()
        sub_routing_network = None
        origin_connector_ids = None
        origin_point_coords = None
        input_table = None
        num_points = None
        try:
            # Create input table for isochrone origin points
            input_table, num_points = await crud_isochrone.create_input_table(
                isochrone_request
            )

            (
                sub_routing_network,
                origin_connector_ids,
                origin_point_coords,
            ) = await crud_isochrone.read_network(
                routing_network,
                isochrone_request,
                input_table,
                num_points,
            )
        except Exception as e:
            await self.db_connection.rollback()
            print(e)
            return
        print(f"Network read time: {round(time.time() - start_time, 2)} sec")

        # Compute heatmap grid utilizing processed sub-network
        start_time = time.time()
        try:
            (
                edges_source,
                edges_target,
                edges_cost,
                edges_reverse_cost,
                edges_length,
                unordered_map,
                node_coords,
                extent,
                geom_address,
                geom_array,
            ) = prepare_network_isochrone(edge_network_input=sub_routing_network)

            adj_list = construct_adjacency_list_(
                len(unordered_map),
                edges_source,
                edges_target,
                edges_cost,
                edges_reverse_cost,
            )

            start_vertices_ids = np.array(
                [unordered_map[v] for v in origin_connector_ids]
            )
            distances_list = dijkstra_h3(
                start_vertices_ids,
                adj_list,
                isochrone_request.travel_cost.max_traveltime,
                False,
            )

            for i in range(len(origin_point_coords)):
                (
                    h3_index,
                    h3_short,
                    h3_centroid_x,
                    h3_centroid_y,
                ) = await self.get_h3_10_grid(
                    isochrone_request.travel_cost,
                    origin_point_coords[i],
                )

                h3_cost = compute_isochrone_h3_optimized(
                    edges_source=edges_source,
                    edges_target=edges_target,
                    edges_length=edges_length,
                    node_coords=node_coords,
                    geom_address=geom_address,
                    geom_array=geom_array,
                    distances=distances_list[i],
                    travel_time=isochrone_request.travel_cost.max_traveltime,
                    speed=isochrone_request.travel_cost.speed / 3.6,
                    centroid_x=h3_centroid_x,
                    centroid_y=h3_centroid_y,
                    extent=extent,
                )

                await self.write_to_db(
                    self.db_connection,
                    origin_h3_coords=origin_point_coords[i],
                    h3_short=h3_short,
                    h3_cost=h3_cost,
                )

                """await self.write_h3_index_cost_to_db(
                    self.db_connection, h3_index, h3_cost
                )"""
        except Exception as e:
            await self.db_connection.rollback()
            print(e)
            return
        print(f"Grid computation time: {round(time.time() - start_time, 2)} sec")

        print(f"Total time: {round(time.time() - total_start, 2)} sec")
