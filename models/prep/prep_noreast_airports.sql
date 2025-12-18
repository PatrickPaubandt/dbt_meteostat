WITH airports_reorder AS (
    SELECT faa
            ,name
            ,city
            ,country
            ,lat
            ,lon
            ,alt
            ,tz
            ,dst
    FROM {{source ('air_force','airports_filtered')}}
    )
    SELECT * FROM airports_reorder