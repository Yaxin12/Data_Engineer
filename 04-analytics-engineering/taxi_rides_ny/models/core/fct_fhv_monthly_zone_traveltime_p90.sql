WITH trip_durations AS (
    SELECT 
        pickup_location_id,
        dropoff_location_id,
        year,
        month,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref('dim_fhv_trips') }}
),
p90_travel_times AS (
    SELECT 
        pickup_location_id,
        dropoff_location_id,
        year,
        month,
        PERCENTILE_CONT(trip_duration, 0.90) 
            OVER (PARTITION BY year, month, pickup_location_id, dropoff_location_id) AS p90_trip_duration
    FROM trip_durations
)
SELECT DISTINCT * FROM p90_travel_times
