import math
import time

import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.isochrone import (
    construct_adjacency_list_,
    dijkstra_h3,
    network_to_grid_h3,
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
        h3_6_index: str,
    ):
        buffer_dist = travel_cost.max_traveltime * ((travel_cost.speed * 1000) / 60)

        sql_get_relevant_cells = f"""
            WITH cells AS (
                SELECT h3_grid_disk(origin_h3_index, radius.value) AS h3_index
                FROM h3_cell_to_center_child('{h3_6_index}', 10) AS origin_h3_index,
                LATERAL (SELECT (h3_get_hexagon_edge_length_avg(6, 'm') + {buffer_dist})::int AS dist) AS buffer,
                LATERAL (SELECT (buffer.dist / (h3_get_hexagon_edge_length_avg(10, 'm') * 2)::int) AS value) AS radius
            )
            SELECT h3_index, to_short_h3_10(h3_index::bigint), ST_X(centroid), ST_Y(centroid)
            FROM cells,
            LATERAL (
                SELECT ST_Transform(ST_SetSRID(point::geometry, 4326), 3857) AS centroid
                FROM h3_cell_to_lat_lng(h3_index) AS point
            ) sub;
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

    def add_to_insert_string(self, orig_h3_10, orig_h3_3, dest_h3_10, cost):
        cost_map = {}
        for i in range(len(dest_h3_10)):
            if math.isnan(cost[i]):
                continue
            if int(cost[i]) not in cost_map:
                cost_map[int(cost[i])] = []
            cost_map[int(cost[i])].append(int(dest_h3_10[i]))

        for cost in cost_map:
            self.insert_string += (
                f"({orig_h3_10}, ARRAY{cost_map[cost]}, {cost}, {orig_h3_3}),"
            )
            self.num_rows_queued += 1

    async def write_to_db(self, db_connection):
        # await db_connection.execute("DROP TABLE IF EXISTS basic.heatmap_grid_walking;")
        # sql_create_table = """
        #     CREATE UNLOGGED TABLE IF NOT EXISTS basic.heatmap_grid_walking (
        #         h3_orig bigint,
        #         h3_dest bigint[],
        #         cost int,
        #         h3_3 int
        #     );
        # """
        # await db_connection.execute(sql_create_table)
        # await db_connection.execute("SELECT create_distributed_table('basic.heatmap_grid_walking', 'h3_3');")
        # await db_connection.commit()

        await db_connection.execute(
            f"""
                INSERT INTO basic.heatmap_grid_walking (h3_orig, h3_dest, cost, h3_3)
                VALUES {self.insert_string.rstrip(",")};
            """
        )
        await db_connection.commit()

        # print(f"Insert time: {round(time.time() - insert_time, 3)} sec")

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
        origin_point_h3_10 = None
        origin_point_h3_3 = None
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
                origin_point_h3_10,
                origin_point_h3_3,
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

            (
                h3_index,
                h3_short,
                h3_centroid_x,
                h3_centroid_y,
            ) = await self.get_h3_10_grid(
                isochrone_request.travel_cost,
                obj_in.h3_6_cell,
            )

            self.insert_string = ""
            self.num_rows_queued = 0
            for i in range(len(origin_point_h3_10)):
                mapped_cost = network_to_grid_h3(
                    extent=extent,
                    zoom=10,
                    edges_source=edges_source,
                    edges_target=edges_target,
                    edges_length=edges_length,
                    geom_address=geom_address,
                    geom_array=geom_array,
                    distances=distances_list[i],
                    node_coords=node_coords,
                    speed=isochrone_request.travel_cost.speed / 3.6,
                    max_traveltime=isochrone_request.travel_cost.max_traveltime,
                    centroid_x=h3_centroid_x,
                    centroid_y=h3_centroid_y,
                )

                self.add_to_insert_string(
                    orig_h3_10=origin_point_h3_10[i],
                    orig_h3_3=origin_point_h3_3[i],
                    dest_h3_10=h3_short,
                    cost=mapped_cost,
                )

                if self.num_rows_queued >= 800 or i == len(origin_point_h3_10) - 1:
                    await self.write_to_db(self.db_connection)
                    self.insert_string = ""
                    self.num_rows_queued = 0

                """await self.write_h3_index_cost_to_db(
                    self.db_connection, h3_index, mapped_cost
                )"""
        except Exception as e:
            await self.db_connection.rollback()
            print(e)
            return
        print(f"Grid computation time: {round(time.time() - start_time, 2)} sec")

        print(f"Total time: {round(time.time() - total_start, 2)} sec")
