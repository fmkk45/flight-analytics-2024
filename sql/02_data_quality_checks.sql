-- ============================================================
-- Data Quality Checks
-- ============================================================

-- 1. Zeilenanzahl prüfen
SELECT COUNT(*) AS total_rows FROM flight_data_2024;

-- 2. Fehlende Werte in kritischen Spalten
SELECT 
    SUM(CASE WHEN fl_date IS NULL THEN 1 ELSE 0 END) AS missing_dates,
    SUM(CASE WHEN origin IS NULL THEN 1 ELSE 0 END) AS missing_origin,
    SUM(CASE WHEN dest IS NULL THEN 1 ELSE 0 END) AS missing_dest
FROM flight_data_2024;

-- 3. Doppelte Zeilen prüfen
SELECT year, month, day_of_month, op_unique_carrier, op_carrier_fl_num, COUNT(*) AS duplicates
FROM flight_data_2024
GROUP BY year, month, day_of_month, op_unique_carrier, op_carrier_fl_num
HAVING COUNT(*) > 1;

-- 4. Durchschnittliche Werte prüfen
SELECT 
    AVG(dep_delay) AS avg_dep_delay,
    AVG(arr_delay) AS avg_arr_delay,
    AVG(distance) AS avg_distance
FROM flight_data_2024;
