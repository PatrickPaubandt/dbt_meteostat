WITH daily_data AS (
        SELECT * 
        FROM {{source('air_force', 'staging_noreast_weather_daily')}}
    ),
    add_features AS (
        SELECT *
    		, DATE_PART('day', date) AS date_day 		-- number of the day of month
    		, DATE_PART('month', date) AS date_month 	-- number of the month of year
    		, DATE_PART('year', date) AS date_year 		-- number of year
    		, DATE_PART('week', date) AS cw 			-- number of the week of year
    		, TO_CHAR(date, 'FMmonth') AS month_name 	-- name of the month
    		, TO_CHAR(date, 'FMday') AS weekday 		-- name of the weekday
        FROM daily_data 
    )
    SELECT *
    FROM add_features
    ORDER BY date