import polars as pl


def retrieve_segments(db_uri, h3_index):
    """Load segments from database into a polars data frame."""

    data_frame = pl.read_database_uri(
        query=f"""
            SELECT
                id, length_m, length_3857,
                class_, impedance_slope, impedance_slope_reverse,
                impedance_surface, coordinates_3857, source,
                target, CAST(tags AS TEXT) AS tags, h3_3, h3_5
            FROM temporal.segment
            WHERE h3_3 = {h3_index}
        """,
        uri=db_uri,
        schema_overrides={
            "id": pl.Utf8,
            "length_m": pl.Float64,
            "length_3857": pl.Float64,
            "class_": pl.Utf8,
            "impedance_slope": pl.Float64,
            "impedance_slope_reverse": pl.Float64,
            "impedance_surface": pl.Float32,
            "coordinates_3857": pl.List(pl.List(pl.Float64)),
            "source": pl.Utf8,
            "target": pl.Utf8,
            "tags": pl.Utf8,
            "h3_3": pl.Int32,
            "h3_5": pl.Int32,
        },
    )
    return data_frame


def retrieve_connectors(db_uri, h3_index):
    """Load connectors from database into a polars data frame."""

    data_frame = pl.read_database_uri(
        query=f"""
            SELECT id, h3_3, h3_5
            FROM temporal.connector
            WHERE h3_3 = {h3_index}
        """,
        uri=db_uri,
        schema_overrides={
            "id": pl.Utf8,
            "h3_3": pl.Int32,
            "h3_5": pl.Int32,
        },
    )
    return data_frame
