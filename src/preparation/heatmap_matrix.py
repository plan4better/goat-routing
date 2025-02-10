from concurrent.futures import ProcessPoolExecutor

import psycopg2

from src.core.config import settings
from src.preparation.heatmap_matrix_process import HeatmapMatrixProcess
from src.schemas.catchment_area import CatchmentAreaRoutingTypeActiveMobility
from src.schemas.heatmap import GEOFENCE_TABLE_CONFIG
from src.utils import print_info


class HeatmapMatrixPreparation:
    def __init__(self):
        # User configurable
        self.ROUTING_TYPE = CatchmentAreaRoutingTypeActiveMobility.walking
        self.NUM_THREADS = 20

    def get_cells_to_process(self, db_cursor):
        """Produce a grid of H3_6 cells to prepare the heatmap matrix in batches."""

        print_info(
            "Producing H3_6 grid of heatmap geofence region for matrix preparation."
        )

        # Produce and fetch a list of H3_6 cells representing the geofence region
        sql_fetch_region_h3_grid = f"""
            WITH region AS (
                SELECT ST_Union(geom) AS geom
                FROM {GEOFENCE_TABLE_CONFIG[self.ROUTING_TYPE]}
            )
            SELECT grid.*
            FROM region,
            LATERAL basic.fill_polygon_h3(geom, 6) grid;
        """
        db_cursor.execute(sql_fetch_region_h3_grid)
        result = db_cursor.fetchall()

        cells_to_process = []
        for h3_index in result:
            cells_to_process.append(h3_index[0])

        print_info(f"Number of H3_6 cells to process: {len(cells_to_process)}")

        return cells_to_process

    def split_cells_into_chunks(self, cells_to_process):
        """Split cells to process into NUM_THREADS chunks."""

        # Calculate chunk size and remainder
        chunk_size = len(cells_to_process) // self.NUM_THREADS
        remainder = len(cells_to_process) % self.NUM_THREADS

        # Split cells to process into thunks of size chunk_size + remainder
        chunks = []
        start = 0
        for i in range(self.NUM_THREADS):
            end = start + chunk_size + (i < remainder)
            chunks.append([i, cells_to_process[start:end]])
            start = end

        return chunks

    def initialize_traveltime_matrix_table(self, db_cursor, db_connection):
        """Create table to store traveltime matrix."""

        traveltime_matrix_table = f"basic.traveltime_matrix_{self.ROUTING_TYPE.value}_{settings.HEATMAP_MATRIX_DATE_SUFFIX}"

        # Drop table if it already exists
        sql_drop_table = f"""
            DROP TABLE IF EXISTS {traveltime_matrix_table};
        """
        db_cursor.execute(sql_drop_table)

        # Create table
        sql_create_table = f"""
            CREATE TABLE {traveltime_matrix_table} (
                orig_id h3index,
                dest_id h3index[],
                traveltime smallint,
                h3_3 int,
                PRIMARY KEY (orig_id, traveltime, h3_3)
            );
        """
        db_cursor.execute(sql_create_table)

        # Distribute table using CITUS
        sql_distribute_table = f"""
            SELECT create_distributed_table(
                '{traveltime_matrix_table}',
                'h3_3'
            );
        """
        db_cursor.execute(sql_distribute_table)

        # Commit changes
        db_connection.commit()

        print_info(f"Initialized traveltime matrix table: {traveltime_matrix_table}")

    def process_chunk(self, chunk):
        HeatmapMatrixProcess(
            thread_id=chunk[0],
            chunk=chunk[1],
            routing_type=self.ROUTING_TYPE,
        ).run()

    def run(self):
        # Connect to database
        db_connection = psycopg2.connect(settings.POSTGRES_DATABASE_URI)
        db_cursor = db_connection.cursor()

        # Get full list of parent H3_6 cells within our region of interest
        cells_to_process = self.get_cells_to_process(db_cursor)

        # Split cells to process into NUM_THREADS chunks to be processed in parallel
        chunks = self.split_cells_into_chunks(cells_to_process)

        # Initialize traveltime matrix table
        self.initialize_traveltime_matrix_table(db_cursor, db_connection)

        # Close database connection
        db_connection.close()

        try:
            # Spawn NUM_THREADS processes to compute matrix in parallel
            with ProcessPoolExecutor(max_workers=self.NUM_THREADS) as process_pool:
                process_pool.map(self.process_chunk, chunks)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    HeatmapMatrixPreparation().run()
