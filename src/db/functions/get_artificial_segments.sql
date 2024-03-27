DROP TYPE IF EXISTS basic.origin_segment;
CREATE TYPE basic.origin_segment AS (
    id int, class_ text, impedance_slope float8, impedance_slope_reverse float8,
    impedance_surface float8, maxspeed_forward int, maxspeed_backward int, source int,
    target int, geom geometry, h3_3 int2, h3_6 int4, fraction float[], fraction_geom geometry[],
    point_id int2[], point_geom geometry[]
);


DROP TYPE IF EXISTS basic.artificial_segment CASCADE;
CREATE TYPE basic.artificial_segment AS (
    point_id int2, point_geom geometry, point_cell_index h3index, point_h3_3 int, old_id int,
    id int, length_m float, length_3857 float, class_ text, impedance_slope float8,
    impedance_slope_reverse float8, impedance_surface float8, coordinates_3857 jsonb,
    maxspeed_forward int, maxspeed_backward int, source int, target int, geom geometry,
    h3_3 int2, h3_6 int4
);


DROP FUNCTION IF EXISTS basic.get_artificial_segments;
CREATE OR REPLACE FUNCTION basic.get_artificial_segments(
    input_table text,
    num_points integer,
    classes text,
    point_cell_resolution int
)
RETURNS SETOF basic.artificial_segment
LANGUAGE plpgsql
AS $function$
DECLARE
    custom_cursor REFCURSOR;
	origin_segment basic.origin_segment;
	artificial_segment basic.artificial_segment;

    -- Increment everytime a new artificial segment is created
    artificial_seg_index int = 1000000000; -- Defaults to 1 billion

    -- Increment everytime a new artificial connector/node is created
    artificial_con_index int = 1000000000; -- Defaults to 1 billion

    -- Increment everytime a new artificial origin node is created (for isochrone starting points)
    artifical_origin_index int = 2000000000; -- Defaults to 2 billion

    fraction float;
    new_geom geometry;
BEGIN
    
	OPEN custom_cursor FOR EXECUTE
		FORMAT (
            'WITH origin AS  (
                SELECT
                    id, geom,
                    ST_SETSRID(ST_Buffer(geom::geography, 100)::geometry, 4326) AS buffer_geom,
                    to_short_h3_3(h3_lat_lng_to_cell(ST_Centroid(geom)::point, 3)::bigint) AS h3_3
                FROM temporal."%s"
                LIMIT %s::int
            ),
            best_segment AS (
                SELECT DISTINCT ON (o.id)
                    o.id AS point_id, o.geom AS point_geom, o.buffer_geom AS point_buffer,
                    s.id, s.class_, s.impedance_slope, s.impedance_slope_reverse,
                    s.impedance_surface, s.maxspeed_forward, s.maxspeed_backward,
                    s."source", s.target, s.geom, s.h3_3, s.h3_6,
                    ST_LineLocatePoint(s.geom, o.geom) AS fraction,
                    ST_ClosestPoint(s.geom, o.geom) AS fraction_geom
                FROM basic.segment s, origin o
                WHERE
                s.h3_3 = o.h3_3
                AND s.class_ = ANY(string_to_array(''%s'', '',''))
                AND ST_Intersects(s.geom, o.buffer_geom)
                ORDER BY o.id, ST_ClosestPoint(s.geom, o.geom) <-> o.geom
            )
            SELECT
                bs.id, bs.class_, bs.impedance_slope,
                bs.impedance_slope_reverse, bs.impedance_surface,
                bs.maxspeed_forward, bs.maxspeed_backward, bs."source",
                bs.target, bs.geom, bs.h3_3, bs.h3_6,
                ARRAY_AGG(bs.fraction) AS fraction,
                ARRAY_AGG(bs.fraction_geom) AS fraction_geom,
                ARRAY_AGG(bs.point_id) AS point_id,
                ARRAY_AGG(bs.point_geom) AS point_geom
            FROM (SELECT * FROM best_segment ORDER BY fraction) bs
            GROUP BY
                bs.id, bs.class_, bs.impedance_slope, bs.impedance_slope_reverse,
                bs.impedance_surface, bs.maxspeed_forward, bs.maxspeed_backward,
                bs."source", bs.target, bs.geom, bs.h3_3, bs.h3_6;'
        , input_table, num_points, classes);
	
	LOOP
		FETCH custom_cursor INTO origin_segment;
		EXIT WHEN NOT FOUND;

        -- Assign values carried over from origin segment
        artificial_segment.old_id = origin_segment.id;
        artificial_segment.class_ = origin_segment.class_;
        artificial_segment.impedance_slope = origin_segment.impedance_slope;
        artificial_segment.impedance_slope_reverse = origin_segment.impedance_slope_reverse;
        artificial_segment.impedance_surface = origin_segment.impedance_surface;
        artificial_segment.maxspeed_forward = origin_segment.maxspeed_forward;
        artificial_segment.maxspeed_backward = origin_segment.maxspeed_backward;
        artificial_segment.h3_3 = origin_segment.h3_3;
        artificial_segment.h3_6 = origin_segment.h3_6;

        IF origin_segment.fraction[1] != 0 THEN
            -- Generate the first artifical segment for this origin segment
            artificial_segment.point_id = NULL;
            artificial_segment.point_geom = NULL;
            artificial_segment.point_cell_index = NULL;
            artificial_segment.point_h3_3 = NULL;
            artificial_segment.id = artificial_seg_index;
            new_geom = ST_LineSubstring(origin_segment.geom, 0, origin_segment.fraction[1]);
            artificial_segment.length_m = ST_Length(new_geom::geography);
            artificial_segment.length_3857 = ST_Length(ST_Transform(new_geom, 3857));
            artificial_segment.coordinates_3857 = (ST_AsGeoJSON(ST_Transform(new_geom, 3857))::jsonb)->'coordinates';
            artificial_segment.source = origin_segment.source;
            IF origin_segment.fraction[1] = 1 THEN
                artificial_segment.target = origin_segment.target;
            ELSE
                artificial_segment.target = artificial_con_index;
                artificial_con_index = artificial_con_index + 1;
            END IF;
            artificial_segment.geom = new_geom;
            RETURN NEXT artificial_segment;
            artificial_seg_index = artificial_seg_index + 1;
        END IF;

        -- Iterate over fractions if the origin segment is the origin for multiple isochrone starting points
        IF array_length(origin_segment.fraction, 1) > 1 THEN
            FOR i IN 2..array_length(origin_segment.fraction, 1) LOOP
                -- Generate an artificial segment connecting the origin point to the new artificial segment
                artificial_segment.point_id = origin_segment.point_id[i - 1];
                artificial_segment.point_geom = origin_segment.point_geom[i - 1];
                artificial_segment.point_cell_index = h3_lat_lng_to_cell(artificial_segment.point_geom::point, point_cell_resolution);
                artificial_segment.point_h3_3 = to_short_h3_3(h3_lat_lng_to_cell(artificial_segment.point_geom::point, 3)::bigint);
                artificial_segment.id = artificial_seg_index;
                new_geom = ST_SetSRID(ST_MakeLine(
                    origin_segment.point_geom[i - 1],
                    origin_segment.fraction_geom[i - 1]
                ), 4326);
                artificial_segment.length_m = ST_Length(new_geom::geography);
                artificial_segment.length_3857 = ST_Length(ST_Transform(new_geom, 3857));
                artificial_segment.coordinates_3857 = (ST_AsGeoJSON(ST_Transform(new_geom, 3857))::jsonb)->'coordinates';
                artificial_segment.maxspeed_forward = 30;
                artificial_segment.maxspeed_backward = 30;
                artificial_segment.source = artifical_origin_index;
                artifical_origin_index = artifical_origin_index + 1;
                IF origin_segment.fraction[i - 1] = 0 THEN
                    artificial_segment.target = origin_segment.source;
                ELSIF origin_segment.fraction[i] = 1 THEN
                    artificial_segment.target = origin_segment.target;
                ELSE
                    artificial_segment.target = artificial_con_index - 1;
                END IF;
                artificial_segment.geom = new_geom;
                RETURN NEXT artificial_segment;
                artificial_seg_index = artificial_seg_index + 1;

                IF origin_segment.fraction[i] != origin_segment.fraction[i - 1] THEN
                    artificial_segment.point_id = NULL;
                    artificial_segment.point_geom = NULL;
                    artificial_segment.point_cell_index = NULL;
                    artificial_segment.point_h3_3 = NULL;
                    artificial_segment.id = artificial_seg_index;
                    new_geom = ST_LineSubstring(origin_segment.geom, origin_segment.fraction[i - 1], origin_segment.fraction[i]);
                    artificial_segment.length_m = ST_Length(new_geom::geography);
                    artificial_segment.length_3857 = ST_Length(ST_Transform(new_geom, 3857));
                    artificial_segment.coordinates_3857 = (ST_AsGeoJSON(ST_Transform(new_geom, 3857))::jsonb)->'coordinates';
                    IF origin_segment.fraction[i - 1] = 0 THEN
                        artificial_segment.source = origin_segment.source;
                    ELSE
                        artificial_segment.source = artificial_con_index - 1;
                    END IF;
                    IF origin_segment.fraction[i] = 1 THEN
                        artificial_segment.target = origin_segment.target;
                    ELSE
                        artificial_segment.target = artificial_con_index;
                        artificial_con_index = artificial_con_index + 1;
                    END IF;
                    artificial_segment.geom = new_geom;
                    RETURN NEXT artificial_segment;
                    artificial_seg_index = artificial_seg_index + 1;
                END IF;
            END LOOP;
        END IF;

        -- Generate an artificial segment connecting the origin point to the new artificial segment
        artificial_segment.point_id = origin_segment.point_id[array_length(origin_segment.point_id, 1)];
        artificial_segment.point_geom = origin_segment.point_geom[array_length(origin_segment.point_geom, 1)];
        artificial_segment.point_cell_index = h3_lat_lng_to_cell(artificial_segment.point_geom::point, point_cell_resolution);
        artificial_segment.point_h3_3 = to_short_h3_3(h3_lat_lng_to_cell(artificial_segment.point_geom::point, 3)::bigint);
        artificial_segment.id = artificial_seg_index;
        new_geom = ST_SetSRID(ST_MakeLine(
            origin_segment.point_geom[array_length(origin_segment.point_geom, 1)],
            origin_segment.fraction_geom[array_length(origin_segment.fraction_geom, 1)]
        ), 4326);
        artificial_segment.length_m = ST_Length(new_geom::geography);
        artificial_segment.length_3857 = ST_Length(ST_Transform(new_geom, 3857));
        artificial_segment.coordinates_3857 = (ST_AsGeoJSON(ST_Transform(new_geom, 3857))::jsonb)->'coordinates';
        artificial_segment.maxspeed_forward = 30;
        artificial_segment.maxspeed_backward = 30;
        artificial_segment.source = artifical_origin_index;
        artifical_origin_index = artifical_origin_index + 1;
        IF origin_segment.fraction[array_length(origin_segment.fraction, 1)] = 0 THEN
            artificial_segment.target = origin_segment.source;
        ELSIF origin_segment.fraction[array_length(origin_segment.fraction, 1)] = 1 THEN
            artificial_segment.target = origin_segment.target;
        ELSE
            artificial_segment.target = artificial_con_index - 1;
        END IF;
        artificial_segment.geom = new_geom;
        RETURN NEXT artificial_segment;
        artificial_seg_index = artificial_seg_index + 1;

        IF origin_segment.fraction[array_length(origin_segment.fraction, 1)] != 1 THEN
            -- Generate the last artificial segment for this origin segment
            artificial_segment.point_id = NULL;
            artificial_segment.point_geom = NULL;
            artificial_segment.point_cell_index = NULL;
            artificial_segment.point_h3_3 = NULL;
            artificial_segment.id = artificial_seg_index;
            new_geom = ST_LineSubstring(origin_segment.geom, origin_segment.fraction[array_length(origin_segment.fraction, 1)], 1);
            artificial_segment.length_m = ST_Length(new_geom::geography);
            artificial_segment.length_3857 = ST_Length(ST_Transform(new_geom, 3857));
            artificial_segment.coordinates_3857 = (ST_AsGeoJSON(ST_Transform(new_geom, 3857))::jsonb)->'coordinates';
            IF origin_segment.fraction[array_length(origin_segment.fraction, 1)] = 0 THEN
                artificial_segment.source = origin_segment.source;
            ELSE
                artificial_segment.source = artificial_con_index - 1;
            END IF;
            artificial_segment.target = origin_segment.target;
            artificial_segment.geom = new_geom;
            RETURN NEXT artificial_segment;
            artificial_seg_index = artificial_seg_index + 1;
        END IF;
	
	END LOOP;
	
	CLOSE custom_cursor;
	
END;
$function$
