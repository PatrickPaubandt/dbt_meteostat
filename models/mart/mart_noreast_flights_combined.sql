WITH weather_base AS (
    SELECT *
    FROM {{ ref('prep_noreast_weather_daily') }}
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
    FROM {{ ref('prep_noreast_flights') }}
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
    FROM {{ ref('prep_noreast_flights') }}
    GROUP BY dest, flight_date
),

combined_stats AS (
    SELECT
        w.date AS flight_date,
        w.cw,
        w.date_month,
        w.airport_code,

        CASE
            WHEN w.cw BETWEEN 1 AND 6 THEN 'previous'
            WHEN w.cw = 7 THEN 'weather_event'
            WHEN w.cw BETWEEN 8 AND 14 THEN 'subsequent'
            ELSE NULL
        END AS weather_event,

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
        w.wind_peakgust_kmh,
        w.avg_pressure_hpa,

        /* 🌩 Weather severity score (0–1) */
        ROUND(
              0.35 * (w.precipitation_mm / 66.0)
            + 0.40 * (w.max_snow_mm / 46.0)
            + 0.25 * (w.avg_wind_speed_kmh / 45.4)
        , 3) AS weather_score

    FROM weather_base w
    LEFT JOIN dep_stats d
      ON w.airport_code = d.airport_code
     AND w.date = d.flight_date
    LEFT JOIN arr_stats a
      ON w.airport_code = a.airport_code
     AND w.date = a.flight_date
),

combined_with_severity AS (
    SELECT
        *,
        CASE
            WHEN weather_score < 0.20 THEN 'low'
            WHEN weather_score < 0.40 THEN 'moderate'
            WHEN weather_score < 0.60 THEN 'severe'
            ELSE 'extreme'
        END AS weather_severity
    FROM combined_stats
),

stats_add_airport AS (
    SELECT
        ai.city,
        ai.name,
        c.*
    FROM combined_with_severity c
    LEFT JOIN {{ ref('prep_noreast_airports') }} ai
      ON c.airport_code = ai.faa
)

SELECT *
FROM stats_add_airport
ORDER BY airport_code, flight_date
