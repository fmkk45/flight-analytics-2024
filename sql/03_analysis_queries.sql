-- ============================================================
--  Flight Data Analytics 2024 - Analysis Queries
-- ============================================================

-- 1. Flüge pro Airline
SELECT 
    op_unique_carrier AS airline,
    COUNT(*) AS total_flights
FROM flight_data_2024
GROUP BY op_unique_carrier
ORDER BY total_flights DESC;

-- 2. Durchschnittliche Verspätung pro Airline
SELECT 
    op_unique_carrier AS airline,
    ROUND(AVG(arr_delay), 2) AS avg_arrival_delay
FROM flight_data_2024
GROUP BY op_unique_carrier
ORDER BY avg_arrival_delay DESC;

-- 3. Monatliche Stornierungen
SELECT 
    month,
    COUNT(*) AS total_flights,
    SUM(cancelled) AS cancelled_flights,
    ROUND(100.0 * SUM(cancelled) / COUNT(*), 2) AS cancel_rate_percent
FROM flight_data_2024
GROUP BY month
ORDER BY month;

-- 4. Beliebteste Routen
SELECT 
    origin_city_name + ' → ' + dest_city_name AS route,
    COUNT(*) AS total_flights
FROM flight_data_2024
GROUP BY origin_city_name, dest_city_name
ORDER BY total_flights DESC;

-- 5. Durchschnittliche Flugzeit nach Distanz
SELECT 
    ROUND(distance, -2) AS distance_group,
    ROUND(AVG(air_time), 2) AS avg_air_time
FROM flight_data_2024
WHERE distance IS NOT NULL
GROUP BY ROUND(distance, -2)
ORDER BY distance_group;
