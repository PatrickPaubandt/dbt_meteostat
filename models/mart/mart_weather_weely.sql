SELECT
    airport_code,
    cw,
    AVG(avg_temp_c) AS avg_week_temp,
    MIN(min_temp_c) AS min_week_temp,
    MAX(max_temp_c) AS max_week_temp,
    SUM(precipitation_mm) AS sum_week_precipitation,
    SUM(max_snow_mm) AS sum_week_snowmm,
    AVG(avg_wind_direction) AS avg_week_winddirection,
    AVG(avg_wind_speed_kmh) AS avg_week_windspeed,
    AVG(wind_peakgust_kmh) AS avg_week_peakgust,
    MAX(wind_peakgust_kmh) AS max_week_peakgust,
    AVG(avg_pressure_hpa) AS avg_week_pressurehpa,
    AVG(sun_minutes) AS avg_week_sunminutes,
    SUM(sun_minutes) AS sum_week_sunminutes
FROM {{ ref('prep_weather_daily') }}
GROUP BY airport_code, cw
ORDER BY airport_code, cw