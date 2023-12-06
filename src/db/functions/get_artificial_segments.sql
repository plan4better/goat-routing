DROP TYPE IF EXISTS temporal.origin_segment;
CREATE TYPE temporal.origin_segment AS (
    id int, class_ text, impedance_slope float8, impedance_slope_reverse float8,
    impedance_surface float8, source int, target int, tags text,
    geom geometry, h3_3 int2, h3_5 int4, fraction float[], fraction_geom geometry[],
    point_id int2[], point_geom geometry[]
);


DROP TYPE IF EXISTS temporal.artificial_segment CASCADE;
CREATE TYPE temporal.artificial_segment AS (
    point_id int2, point_geom geometry, old_id int, id int, length_m float,
    length_3857 float, class_ text, impedance_slope float8, impedance_slope_reverse float8,
    impedance_surface float8, coordinates_3857 jsonb,
    source int, target int, geom geometry, tags text, h3_3 int2, h3_5 int4
);


DROP FUNCTION IF EXISTS temporal.get_artificial_segments;
CREATE OR REPLACE FUNCTION temporal.get_artificial_segments(num_points integer, classes text)
RETURNS SETOF temporal.artificial_segment
LANGUAGE plpgsql
AS $function$
DECLARE
    custom_cursor REFCURSOR;
	origin_segment temporal.origin_segment;
	artificial_segment temporal.artificial_segment;

    artificial_seg_index int = 1000000000; -- Defaults to 1 billion
    artificial_con_index int = 1000000000; -- Defaults to 1 billion

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
                FROM temporal.isochrone_input
                LIMIT %s::int
            ),
            best_segment AS (
                SELECT DISTINCT ON (o.id)
                    o.id AS point_id, o.geom AS point_geom, o.buffer_geom AS point_buffer,
                    s.id, s.class_, s.impedance_slope, s.impedance_slope_reverse,
                    s.impedance_surface, s."source", s.target, s.tags, s.geom,
                    s.h3_3, s.h3_5, ST_LineLocatePoint(s.geom, o.geom) AS fraction,
                    ST_ClosestPoint(s.geom, o.geom) AS fraction_geom
                FROM temporal.segment s, origin o
                WHERE
                s.h3_3 = o.h3_3
                AND s.class_ = ANY(string_to_array(''%s'', '',''))
                AND ST_Intersects(s.geom, o.buffer_geom)
                ORDER BY o.id, ST_ClosestPoint(s.geom, o.geom) <-> o.geom
            )
            SELECT
                bs.id, bs.class_, bs.impedance_slope,
                bs.impedance_slope_reverse, bs.impedance_surface,
                bs."source", bs.target, bs.tags,
                bs.geom, bs.h3_3, bs.h3_5,
                ARRAY_AGG(bs.fraction) AS fraction,
                ARRAY_AGG(bs.fraction_geom) AS fraction_geom,
                ARRAY_AGG(bs.point_id) AS point_id,
                ARRAY_AGG(bs.point_geom) AS point_geom
            FROM (SELECT * FROM best_segment ORDER BY fraction) bs
            GROUP BY
                bs.id, bs.class_, bs.impedance_slope, bs.impedance_slope_reverse,
                bs.impedance_surface, bs."source", bs.target, bs.tags,
                bs.geom, bs.h3_3, bs.h3_5;'
        , num_points, classes);
	
	LOOP
		FETCH custom_cursor INTO origin_segment;
		EXIT WHEN NOT FOUND;

        -- Assign values carried over from origin segment
        artificial_segment.old_id = origin_segment.id;
        artificial_segment.class_ = origin_segment.class_;
        artificial_segment.impedance_slope = origin_segment.impedance_slope;
        artificial_segment.impedance_slope_reverse = origin_segment.impedance_slope_reverse;
        artificial_segment.impedance_surface = origin_segment.impedance_surface;
        artificial_segment.tags = origin_segment.tags;
        artificial_segment.h3_3 = origin_segment.h3_3;
        artificial_segment.h3_5 = origin_segment.h3_5;

        -- Generate the first artifical segment for this origin segment
        artificial_segment.point_id = NULL;
        artificial_segment.point_geom = NULL;
        artificial_segment.id = artificial_seg_index;
        new_geom = ST_LineSubstring(origin_segment.geom, 0, origin_segment.fraction[1]);
        artificial_segment.length_m = ST_Length(new_geom::geography);
        artificial_segment.length_3857 = ST_Length(ST_Transform(new_geom, 3857));
        artificial_segment.coordinates_3857 = (ST_AsGeoJSON(ST_Transform(new_geom, 3857))::jsonb)->'coordinates';
        artificial_segment.source = origin_segment.source;
        artificial_segment.target = artificial_con_index;
        artificial_segment.geom = new_geom;
        RETURN NEXT artificial_segment;
        artificial_seg_index = artificial_seg_index + 1;
        artificial_con_index = artificial_con_index + 1;

        -- Iterate over fractions if the origin segment is the origin for multiple isochrone starting points
        IF array_length(origin_segment.fraction, 1) > 1 THEN
            FOR i IN 2..array_length(origin_segment.fraction, 1) LOOP
                artificial_segment.point_id = NULL;
                artificial_segment.point_geom = NULL;
                artificial_segment.id = artificial_seg_index;
                new_geom = ST_LineSubstring(origin_segment.geom, origin_segment.fraction[i - 1], origin_segment.fraction[i]);
                artificial_segment.length_m = ST_Length(new_geom::geography);
                artificial_segment.length_3857 = ST_Length(ST_Transform(new_geom, 3857));
                artificial_segment.coordinates_3857 = (ST_AsGeoJSON(ST_Transform(new_geom, 3857))::jsonb)->'coordinates';
                artificial_segment.source = artificial_con_index - 1;
                artificial_segment.target = artificial_con_index;
                artificial_segment.geom = new_geom;
                RETURN NEXT artificial_segment;
                artificial_seg_index = artificial_seg_index + 1;
                artificial_con_index = artificial_con_index + 1;

                -- Generate an artificial segment connecting the origin point to the new artificial segment
                artificial_segment.point_id = origin_segment.point_id[i - 1];
                artificial_segment.point_geom = origin_segment.point_geom[i - 1];
                artificial_segment.id = artificial_seg_index;
                new_geom = ST_SetSRID(ST_MakeLine(
                    origin_segment.point_geom[i - 1],
                    origin_segment.fraction_geom[i - 1]
                ), 4326);
                artificial_segment.length_m = ST_Length(new_geom::geography);
                artificial_segment.length_3857 = ST_Length(ST_Transform(new_geom, 3857));
                artificial_segment.coordinates_3857 = (ST_AsGeoJSON(ST_Transform(new_geom, 3857))::jsonb)->'coordinates';
                artificial_segment.source = artificial_con_index;
                artificial_segment.target = artificial_con_index - 2;
                artificial_segment.geom = new_geom;
                RETURN NEXT artificial_segment;
                artificial_seg_index = artificial_seg_index + 1;
                artificial_con_index = artificial_con_index + 1;
            END LOOP;
        END IF;

        -- Generate the last artificial segment for this origin segment
        artificial_segment.point_id = NULL;
        artificial_segment.point_geom = NULL;
        artificial_segment.id = artificial_seg_index;
        new_geom = ST_LineSubstring(origin_segment.geom, origin_segment.fraction[array_length(origin_segment.fraction, 1)], 1);
        artificial_segment.length_m = ST_Length(new_geom::geography);
        artificial_segment.length_3857 = ST_Length(ST_Transform(new_geom, 3857));
        artificial_segment.coordinates_3857 = (ST_AsGeoJSON(ST_Transform(new_geom, 3857))::jsonb)->'coordinates';
        IF array_length(origin_segment.fraction, 1) > 1 THEN
            artificial_segment.source = artificial_con_index - 2;
        ELSE
            artificial_segment.source = artificial_con_index - 1;
        END IF;
        artificial_segment.target = origin_segment.target;
        artificial_segment.geom = new_geom;
        RETURN NEXT artificial_segment;
        artificial_seg_index = artificial_seg_index + 1;

        -- Generate an artificial segment connecting the origin point to the new artificial segment
        artificial_segment.point_id = origin_segment.point_id[array_length(origin_segment.point_id, 1)];
        artificial_segment.point_geom = origin_segment.point_geom[array_length(origin_segment.point_geom, 1)];
        artificial_segment.id = artificial_seg_index;
        new_geom = ST_SetSRID(ST_MakeLine(
            origin_segment.point_geom[array_length(origin_segment.point_geom, 1)],
            origin_segment.fraction_geom[array_length(origin_segment.fraction_geom, 1)]
        ), 4326);
        artificial_segment.length_m = ST_Length(new_geom::geography);
        artificial_segment.length_3857 = ST_Length(ST_Transform(new_geom, 3857));
        artificial_segment.coordinates_3857 = (ST_AsGeoJSON(ST_Transform(new_geom, 3857))::jsonb)->'coordinates';
        artificial_segment.source = artificial_con_index;
        IF array_length(origin_segment.fraction, 1) > 1 THEN
            artificial_segment.target = artificial_con_index - 2;
        ELSE
            artificial_segment.target = artificial_con_index - 1;
        END IF;
        artificial_segment.geom = new_geom;
        RETURN NEXT artificial_segment;
        artificial_seg_index = artificial_seg_index + 1;
        artificial_con_index = artificial_con_index + 1;
	
	END LOOP;
	
	CLOSE custom_cursor;
	
END;
$function$
