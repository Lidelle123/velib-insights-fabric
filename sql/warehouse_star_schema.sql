-- =====================================================================
-- Vélib' Insights — Warehouse wh_velib (T-SQL)
-- Modèle en étoile construit à partir des lakehouses lh_silver et lh_gold
-- (requêtes inter-bases : nom en trois parties lakehouse.schéma.table)
-- Exécuter chaque bloc séparément : CREATE SCHEMA et CREATE VIEW doivent être seuls dans leur lot.
-- =====================================================================

-- 1. Schéma
CREATE SCHEMA velib;

-- 2. Dimension Station
CREATE TABLE velib.dim_station AS
SELECT station_id, station_code, station_name, capacity, lat, lon
FROM lh_silver.dbo.silver_station;

-- 3. Dimension Temps (heure)
CREATE TABLE velib.dim_hour AS
SELECT DISTINCT hour_ts,
       CAST(hour_ts AS DATE) AS date_utc,
       hour_of_day, day_of_week, is_weekend
FROM lh_gold.dbo.gold_station_hourly;

-- 4. Table de faits (une ligne par station et par heure)
CREATE TABLE velib.fact_station_hour AS
SELECT station_id, hour_ts,
       avg_bikes, min_bikes, avg_ebike, avg_docks,
       pct_time_empty, pct_time_full,
       temperature_c, precipitation_mm, wind_kmh
FROM lh_gold.dbo.gold_station_hourly;

-- 5. Vue KPI par station — stations inactives exclues
--    (capacité 0, vides 100 % du temps ou sans aucune borne libre : anomalies de la source)
CREATE OR ALTER VIEW velib.v_station_kpi AS
SELECT s.station_name,
       s.capacity,
       ROUND(AVG(f.pct_time_empty) * 100, 1) AS pct_temps_vide,
       ROUND(AVG(f.pct_time_full)  * 100, 1) AS pct_temps_plein,
       ROUND(AVG(f.avg_bikes), 1)            AS velos_moyens
FROM velib.fact_station_hour f
JOIN velib.dim_station s ON f.station_id = s.station_id
WHERE s.capacity > 0
GROUP BY s.station_name, s.capacity
HAVING AVG(f.pct_time_empty) < 1
   AND AVG(f.pct_time_full)  < 1;

-- 6. Question métier : les 10 stations actives les plus souvent vides
SELECT TOP 10 *
FROM velib.v_station_kpi
ORDER BY pct_temps_vide DESC;

-- 7. Profil horaire : semaine vs week-end
SELECT h.hour_of_day,
       CASE WHEN h.is_weekend = 1 THEN 'Week-end' ELSE 'Semaine' END AS type_jour,
       ROUND(AVG(f.pct_time_empty) * 100, 1) AS pct_temps_vide
FROM velib.fact_station_hour f
JOIN velib.dim_hour h ON f.hour_ts = h.hour_ts
JOIN velib.dim_station s ON f.station_id = s.station_id
WHERE s.capacity > 0
GROUP BY h.hour_of_day, h.is_weekend
ORDER BY type_jour, h.hour_of_day;

-- 8. Effet de la pluie sur la disponibilité
SELECT CASE WHEN f.precipitation_mm > 0 THEN 'Pluie' ELSE 'Sec' END AS meteo,
       COUNT(DISTINCT f.hour_ts)              AS heures,
       ROUND(AVG(f.avg_bikes), 2)             AS velos_moyens_par_station,
       ROUND(AVG(f.pct_time_empty) * 100, 1)  AS pct_temps_vide
FROM velib.fact_station_hour f
WHERE f.precipitation_mm IS NOT NULL
GROUP BY CASE WHEN f.precipitation_mm > 0 THEN 'Pluie' ELSE 'Sec' END;
