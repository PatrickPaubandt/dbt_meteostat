WITH weather_base AS (
    SELECT *
    FROM {{ ref('prep_weather_daily') }}
    WHERE airport_code IN ('LAX','JFK','MIA')
      AND date >= '2025-01-01'
      AND date <  '2025-02-01'
),

dep_stats AS (
    SELECT
        flight_date,
        origin AS airport_code,
        COUNT(DISTINCT dest) AS uni_dep_connections,
        COUNT(*) AS planned_dep_flights,
        SUM(cancelled) AS cancelled_dep_flights,
        SUM(diverted) AS diverted_dep_flights,
        SUM(CASE WHEN cancelled = 0 THEN 1 ELSE 0 END) AS occurred_dep_flights,
        COUNT(DISTINCT tail_number) AS uni_dep_airplanes,
        COUNT(DISTINCT airline) AS uni_dep_airlines
    FROM {{ ref('prep_flights') }}
    WHERE origin IN ('LAX','JFK','MIA')
    GROUP BY origin, flight_date
),

arr_stats AS (
    SELECT
        flight_date,
        dest AS airport_code,
        COUNT(DISTINCT origin) AS uni_arr_connections,
        COUNT(*) AS planned_arr_flights,
        SUM(cancelled) AS cancelled_arr_flights,
        SUM(diverted) AS diverted_arr_flights,
        SUM(CASE WHEN cancelled = 0 THEN 1 ELSE 0 END) AS occurred_arr_flights,
        COUNT(DISTINCT tail_number) AS uni_arr_airplanes,
        COUNT(DISTINCT airline) AS uni_arr_airlines
    FROM {{ ref('prep_flights') }}
    WHERE dest IN ('LAX','JFK','MIA')
    GROUP BY dest, flight_date
),

combined_stats AS (
    SELECT
        w.date AS flight_date,
        w.airport_code,

        d.uni_dep_connections,
        a.uni_arr_connections,

        d.planned_dep_flights + a.planned_arr_flights AS planned_flights,
        d.cancelled_dep_flights + a.cancelled_arr_flights AS total_cancellations,
        d.diverted_dep_flights + a.diverted_arr_flights AS total_diverted,
        d.occurred_dep_flights + a.occurred_arr_flights AS occurred_flights,

        ROUND((d.uni_dep_airplanes + a.uni_arr_airplanes)::NUMERIC / 2, 1) AS avg_unique_airplanes,
        ROUND((d.uni_dep_airlines + a.uni_arr_airlines)::NUMERIC / 2, 1) AS avg_unique_airlines,

        w.min_temp_c,
        w.max_temp_c,
        w.precipitation_mm,
        w.max_snow_mm,
        w.avg_wind_direction,
        w.avg_wind_speed_kmh,
        w.wind_peakgust_kmh

    FROM weather_base w
    LEFT JOIN dep_stats d
      ON w.airport_code = d.airport_code
     AND w.date = d.flight_date
    LEFT JOIN arr_stats a
      ON w.airport_code = a.airport_code
     AND w.date = a.flight_date
),

stats_add_airport AS (
    SELECT
        c.*,
        ai.city,
        ai.country,
        ai.name
    FROM combined_stats c
    LEFT JOIN {{ ref('prep_airport') }} ai
      ON c.airport_code = ai.faa
)

SELECT *
FROM stats_add_airport
ORDER BY airport_code, flight_date
