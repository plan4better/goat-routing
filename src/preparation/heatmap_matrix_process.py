import math

import numpy as np
import psycopg2
from tqdm import tqdm

from src.core.config import settings
from src.core.isochrone import (
    construct_adjacency_list_,
    dijkstra_h3,
    network_to_grid_h3,
    prepare_network_isochrone,
)
from src.crud.crud_catchment_area_sync import CRUDCatchmentArea, FetchRoutingNetwork
from src.schemas.catchment_area import (
    CatchmentAreaStartingPoints,
    CatchmentAreaType,
    ICatchmentAreaActiveMobility,
    RoutingActiveMobilityType,
)
from src.schemas.error import BufferExceedsNetworkError, DisconnectedOriginError
from src.schemas.heatmap import ROUTING_COST_CONFIG


class HeatmapMatrixProcess:
    def __init__(
        self,
        thread_id: int,
        chunk: list,
        routing_type: RoutingActiveMobilityType,
    ):
        self.thread_id = thread_id
        self.routing_network = None
        self.chunk = chunk
        self.routing_type = routing_type
        self.INSERT_BATCH_SIZE = 800

        self.buffer_distance = ROUTING_COST_CONFIG[
            routing_type.value
        ].max_traveltime * ((ROUTING_COST_CONFIG[routing_type.value].speed * 1000) / 60)

    def generate_multi_catchment_area_request(
        self, db_cursor, h3_6_index: str, routing_type: RoutingActiveMobilityType
    ):
        """Produce a multi-catchment area request for a given H3_6 index and routing type."""

        origin_lat = []
        origin_lng = []

        # Get the centroid coordinates for all child H3_10 cells of the supplied H3_6 parent cell
        sql_get_centroid = f"""
            WITH centroid AS (
                SELECT ST_SetSRID(h3_cell_to_lat_lng(h3_index)::geometry, 4326) AS geom
                FROM h3_cell_to_children('{h3_6_index}'::h3index, 10) AS h3_index
            )
            SELECT ST_X(geom), ST_Y(geom)
            FROM centroid;
        """
        db_cursor.execute(sql_get_centroid)
        result = db_cursor.fetchall()

        # Group centroid coordinates into latitude and longitude lists
        for centroid in result:
            origin_lat.append(centroid[1])
            origin_lng.append(centroid[0])

        # Produce final ICatchmentAreaActiveMobility object (request for CRUDCatchmentArea)
        return ICatchmentAreaActiveMobility(
            starting_points=CatchmentAreaStartingPoints(
                latitude=origin_lat,
                longitude=origin_lng,
            ),
            routing_type=routing_type,
            travel_cost=ROUTING_COST_CONFIG[routing_type.value],
            scenario_id=None,
            catchment_area_type=CatchmentAreaType.polygon,
            polygon_difference=True,
            result_table="",
            layer_id=None,
        )

    def get_h3_10_grid(self, db_cursor, h3_6_index: str):
        sql_get_relevant_cells = f"""
            WITH cells AS (
                SELECT h3_grid_disk(origin_h3_index, radius.value) AS h3_index
                FROM h3_cell_to_center_child('{h3_6_index}', 10) AS origin_h3_index,
                LATERAL (SELECT (h3_get_hexagon_edge_length_avg(6, 'm') + {self.buffer_distance})::int AS dist) AS buffer,
                LATERAL (SELECT (buffer.dist / (h3_get_hexagon_edge_length_avg(10, 'm') * 2)::int) AS value) AS radius
            )
            SELECT h3_index, ST_X(centroid), ST_Y(centroid)
            FROM cells,
            LATERAL (
                SELECT ST_Transform(ST_SetSRID(point::geometry, 4326), 3857) AS centroid
                FROM h3_cell_to_lat_lng(h3_index) AS point
            ) sub;
        """
        db_cursor.execute(sql_get_relevant_cells)
        result = db_cursor.fetchall()

        h3_index = []
        x_centroids = np.empty(len(result))
        y_centroids = np.empty(len(result))
        for i in range(len(result)):
            h3_index.append(result[i][0])
            x_centroids[i] = result[i][1]
            y_centroids[i] = result[i][2]

        return h3_index, x_centroids, y_centroids

    def add_to_insert_string(self, orig_h3_10, dest_h3_10, cost, orig_h3_3):
        cost_map = {}
        for i in range(len(dest_h3_10)):
            if math.isnan(cost[i]) or int(cost[i]) == 0:
                continue
            if int(cost[i]) not in cost_map:
                cost_map[int(cost[i])] = []
            cost_map[int(cost[i])].append(dest_h3_10[i])

        for traveltime in cost_map:
            self.insert_string += f"""(
                '{orig_h3_10}',
                ARRAY{cost_map[traveltime]},
                {traveltime},
                {orig_h3_3}
            ),"""
            self.num_rows_queued += 1

        """cost_map = json.dumps(cost_map)

        self.insert_string += (
            f"('{orig_h3_10}'::h3index, '{cost_map}'::jsonb, {orig_h3_3}),"
        )
        self.num_rows_queued += 1"""

    def write_to_db(self, db_cursor, db_connection):
        db_cursor.execute(
            f"""
                INSERT INTO basic.traveltime_matrix_walking_text (orig_id, dest_id, traveltime, h3_3)
                VALUES {self.insert_string.rstrip(",")};
            """
        )
        db_connection.commit()

    def run(self):
        db_connection = psycopg2.connect(settings.POSTGRES_DATABASE_URI)
        db_cursor = db_connection.cursor()

        crud_catchment_area = CRUDCatchmentArea(db_connection, db_cursor)

        # Fetch routing network (processed segments) and load into memory
        if self.routing_network is None:
            self.routing_network = FetchRoutingNetwork(db_cursor).fetch()

        for index in tqdm(
            range(len(self.chunk)), desc=f"Thread {self.thread_id}", unit=" cell"
        ):
            h3_6_index = self.chunk[index]

            catchment_area_request = self.generate_multi_catchment_area_request(
                db_cursor=db_cursor,
                h3_6_index=h3_6_index,
                routing_type=self.routing_type,
            )

            sub_routing_network = None
            origin_connector_ids = None
            origin_point_h3_10 = None
            origin_point_h3_3 = None
            try:
                # Create input table for catchment area origin points
                input_table, num_points = crud_catchment_area.create_input_table(
                    catchment_area_request
                )

                # Read & process routing network to extract relevant sub-network
                (
                    sub_routing_network,
                    origin_connector_ids,
                    origin_point_h3_10,
                    origin_point_h3_3,
                ) = crud_catchment_area.read_network(
                    self.routing_network,
                    catchment_area_request,
                    input_table,
                    num_points,
                )

                # Delete input table for catchment area origin points
                crud_catchment_area.delete_input_table(input_table)
            except Exception as e:
                db_connection.rollback()
                if isinstance(e, DisconnectedOriginError):
                    print(
                        f"Thread {self.thread_id}: Skipping {h3_6_index} due to disconnected origin. Starting points: [temporal.{input_table}]"
                    )
                    continue
                elif isinstance(e, BufferExceedsNetworkError):
                    print(
                        f"Thread {self.thread_id}: Skipping {h3_6_index} due to buffer exceeding network. Starting points: [temporal.{input_table}]"
                    )
                    continue
                else:
                    print(e)
                    print(
                        f"Thread {self.thread_id}: Error processing {h3_6_index}, exiting."
                    )
                    break

            # Compute heatmap grid utilizing processed sub-network
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
                    catchment_area_request.travel_cost.max_traveltime,
                    False,
                )

                (
                    h3_index,
                    h3_centroid_x,
                    h3_centroid_y,
                ) = self.get_h3_10_grid(
                    db_cursor,
                    h3_6_index,
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
                        speed=catchment_area_request.travel_cost.speed / 3.6,
                        max_traveltime=catchment_area_request.travel_cost.max_traveltime,
                        centroid_x=h3_centroid_x,
                        centroid_y=h3_centroid_y,
                    )

                    self.add_to_insert_string(
                        orig_h3_10=origin_point_h3_10[i],
                        dest_h3_10=h3_index,
                        cost=mapped_cost,
                        orig_h3_3=origin_point_h3_3[i],
                    )

                    if (
                        self.num_rows_queued >= self.INSERT_BATCH_SIZE
                        or i == len(origin_point_h3_10) - 1
                    ):
                        self.write_to_db(
                            db_cursor,
                            db_connection,
                        )
                        self.insert_string = ""
                        self.num_rows_queued = 0

            except Exception as e:
                db_connection.rollback()
                print(e)
                print(
                    f"Thread {self.thread_id}: Error processing {h3_6_index}, exiting."
                )
                break

        db_connection.close()

        print(f"Thread {self.thread_id} finished.")
