from concurrent.futures import ProcessPoolExecutor

import psycopg2

from src.core.config import settings
from src.preparation.heatmap_matrix_process import HeatmapMatrixProcess
from src.schemas.isochrone import RoutingActiveMobilityType


class HeatmapMatrixPreparation:
    def __init__(self):
        # User configurable
        self.ROUTING_TYPE = RoutingActiveMobilityType.walking
        self.NUM_THREADS = 16

    def get_cells_to_process(self, db_cursor):
        """Get list of parent H3_6 cells to process."""

        # Get cells from pre-defined table
        sql_get_cells_to_process = """
            SELECT h3_index
            FROM basic.geofence_heatmap_grid;
        """
        db_cursor.execute(sql_get_cells_to_process)
        result = db_cursor.fetchall()

        cells_to_process = []
        for h3_index in result:
            cells_to_process.append(h3_index[0])

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

        db_connection.close()

        try:
            # Spawn NUM_THREADS processes to compute matrix in parallel
            with ProcessPoolExecutor(max_workers=self.NUM_THREADS) as process_pool:
                process_pool.map(self.process_chunk, chunks)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    HeatmapMatrixPreparation().run()
