WITH mart_route_stats AS (
    SELECT *
    FROM {{ ref('prep_flights') }}
),

route_creation AS (
    SELECT
        *,
        origin || ' - ' || dest AS route
    FROM mart_route_stats
),

route_stats AS (
    SELECT
        route,
        origin,
        dest,

        COUNT(*) AS total_flights,
        COUNT(DISTINCT tail_number) AS unique_airplanes,
        COUNT(DISTINCT airline) AS unique_airlines,

        ROUND(AVG(actual_elapsed_time), 2) AS avg_elapsed_time,
        ROUND(AVG(arr_delay), 2) AS avg_arr_delay,

        ROUND(AVG(actual_elapsed_time) * 60) * INTERVAL '1 second'
            AS avg_elapsed_time_int,

        ROUND(AVG(arr_delay) * 60) * INTERVAL '1 second'
            AS avg_arr_delay_int,

        MAX(arr_delay) AS max_delay,
        MIN(arr_delay) AS min_delay,
        (MAX(arr_delay) * 60) * INTERVAL '1 second' AS max_delay_int,

        (MIN(arr_delay) * 60) * INTERVAL '1 second' AS min_delay_int,

        SUM(cancelled) AS total_cancellations,
        SUM(diverted) AS total_diverted

    FROM route_creation
    GROUP BY route, origin, dest
),

route_stats_enriched AS (
    SELECT
        rs.*,

        orig.city    AS origin_city,
        orig.country AS origin_country,
        orig.name    AS origin_airport_name,

        dest.city    AS dest_city,
        dest.country AS dest_country,
        dest.name    AS dest_airport_name

    FROM route_stats rs
    LEFT JOIN {{ ref('prep_airport') }} orig
        ON rs.origin = orig.faa
    LEFT JOIN {{ ref('prep_airport') }} dest
        ON rs.dest = dest.faa
)

SELECT *
FROM route_stats_enriched
ORDER BY route, origin, dest

