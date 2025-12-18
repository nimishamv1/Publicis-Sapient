-- Temperature and Humidity Patterns Analysis by Location and Time

SELECT 
    device_id,
    received_at as timestamp,
    sensorlocation as location,
    ROUND(AVG(airtemperature)::numeric, 2) AS avg_temperature,
    ROUND(MIN(airtemperature)::numeric, 2) AS min_temperature,
    ROUND(MAX(airtemperature)::numeric, 2) AS max_temperature,
    ROUND(AVG(relativehumidity)::numeric, 2) AS avg_humidity,
    ROUND(MIN(relativehumidity)::numeric, 2) AS min_humidity,
    ROUND(MAX(relativehumidity)::numeric, 2) AS max_humidity,
    COUNT(*) AS measurement_count
FROM weather_data
GROUP BY device_id, received_at,sensorlocation
ORDER BY device_id, received_at,sensorlocation DESC;
