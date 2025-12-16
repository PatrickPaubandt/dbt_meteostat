WITH mart_faa_stats AS (
    SELECT * 
    FROM {{ref('prep_flights')}}
    ),
    dep_stats AS (
    				SELECT origin AS airport_code, 
    				       Count(DISTINCT dest) AS uni_dep_connections,
    				       Count(flight_date) AS planned_dep_flights,
    				       Sum(cancelled) AS cancelled_dep_flights,
    				       Sum (diverted) AS diverted_dep_flights,
    				       Count(DISTINCT tail_number) AS uni_dep_airplanes,
    				       Count(DISTINCT airline) AS uni_dep_airlines
    				FROM mart_faa_stats
    				GROUP BY origin),
   arr_stats AS (
   					SELECT dest AS airport_code,
   					COUNT(DISTINCT origin) AS uni_arr_connections,
   					Count(flight_date) AS planned_arr_flights,
   					SUM (cancelled) AS cancelled_arr_flights,
   					SUM(diverted) AS diverted_arr_flights,
   					Count(DISTINCT tail_number) AS uni_arr_airplanes,
   					COUNT(DISTINCT airline) AS uni_arr_airlines
   					FROM mart_faa_stats
   					GROUP BY dest),
 combined_stats AS (
 					SELECT d.airport_code,
 						   d.uni_dep_connections + a.uni_arr_connections AS unique_connections,
 						   d.planned_dep_flights + a.planned_arr_flights AS planned_flights,
 						   d.cancelled_dep_flights + a.cancelled_arr_flights AS total_cancellations,
 						   d.diverted_dep_flights + a.diverted_arr_flights AS total_diverted,
 						   ROUND((d.uni_dep_airplanes + a.uni_arr_airplanes)::NUMERIC/2,1) AS avg_unique_airplanes,
 						   ROUND((d.uni_dep_airlines + a.uni_arr_airlines)::NUMERIC/2,1) AS avg_unique_airlines
 					FROM dep_stats d
 					JOIN arr_stats a
 					ON d.airport_code = a.airport_code),
total_stats AS (
					SELECT air.city,
					       air.country,
					       air.name,
					       c.*
					FROM combined_stats c
					LEFT JOIN {{ref('prep_airport')}} air
					ON c.airport_code = air.faa)
SELECT * 
FROM total_stats
ORDER BY unique_connections DESC;