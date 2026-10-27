# Fabric notebook — nb_03_silver_gold
# Langage : PySpark · Lakehouse par défaut : lh_silver
# Historique des stations + météo (Silver) -> agrégats horaires par station (Gold)

# %% Cellule 1 — Historique des stations : Bronze -> silver_status_history
from pyspark.sql import functions as F, Window

spark.conf.set("spark.sql.session.timeZone", "UTC")

ONELAKE = "abfss://ws-velib-dev@onelake.dfs.fabric.microsoft.com"
BRONZE_STATUS  = f"{ONELAKE}/lh_bronze.Lakehouse/Files/raw/velib/station_status"
BRONZE_WEATHER = f"{ONELAKE}/lh_bronze.Lakehouse/Files/raw/weather"
GOLD_TABLES    = f"{ONELAKE}/lh_gold.Lakehouse/Tables"

raw_status = spark.read.option("multiLine", True).json(BRONZE_STATUS)

silver_status = (
    raw_status
    .select(F.input_file_name().alias("file"), "date", F.explode("data.stations").alias("s"))
    # l'heure du relevé est encodée dans le nom du fichier : station_status_HHMMSS.json
    .withColumn("hhmmss", F.regexp_extract("file", r"station_status_(\d{6})\.json", 1))
    .withColumn("snapshot_ts", F.to_timestamp(F.concat_ws(" ", F.col("date").cast("string"), "hhmmss"),
                                              "yyyy-MM-dd HHmmss"))
    .select(
        F.col("s.station_id").cast("long").alias("station_id"),
        "snapshot_ts",
        F.col("s.num_bikes_available").cast("int").alias("bikes"),
        F.expr("aggregate(s.num_bikes_available_types, 0L, (acc, x) -> acc + coalesce(x.ebike, 0))").cast("int").alias("ebike"),
        F.expr("aggregate(s.num_bikes_available_types, 0L, (acc, x) -> acc + coalesce(x.mechanical, 0))").cast("int").alias("mechanical"),
        F.col("s.num_docks_available").cast("int").alias("docks"),
        F.col("s.is_renting").cast("int").alias("is_renting"),
        F.col("s.is_returning").cast("int").alias("is_returning"),
        F.col("date").cast("date").alias("date"),
    )
    .dropDuplicates(["station_id", "snapshot_ts"])
)

silver_status.write.mode("overwrite").format("delta").partitionBy("date").saveAsTable("silver_status_history")
print("silver_status_history :", spark.table("silver_status_history").count(), "lignes")


# %% Cellule 2 — Météo : Bronze -> silver_weather_hourly
raw_w = spark.read.option("multiLine", True).json(BRONZE_WEATHER)

weather = (
    raw_w.select(
        F.input_file_name().alias("file"),
        F.col("hourly.time").alias("t"),
        F.col("hourly.temperature_2m").alias("temp"),
        F.col("hourly.precipitation").alias("precip"),
        F.col("hourly.wind_speed_10m").alias("wind"),
        F.col("hourly.weather_code").alias("wcode"),
    )
    .select("file", F.explode(F.arrays_zip("t", "temp", "precip", "wind", "wcode")).alias("z"))
    .select(
        "file",
        F.to_timestamp("z.t", "yyyy-MM-dd'T'HH:mm").alias("weather_hour"),
        F.col("z.temp").cast("double").alias("temperature_c"),
        F.col("z.precip").cast("double").alias("precipitation_mm"),
        F.col("z.wind").cast("double").alias("wind_kmh"),
        F.col("z.wcode").cast("int").alias("weather_code"),
    )
)

# Une même heure figure dans plusieurs fichiers (veille observée + prévisions) : on garde la plus récente
w = Window.partitionBy("weather_hour").orderBy(F.col("file").desc())
silver_weather = weather.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn", "file")

silver_weather.write.mode("overwrite").format("delta").saveAsTable("silver_weather_hourly")
print("silver_weather_hourly :", silver_weather.count(), "heures")


# %% Cellule 3 — Gold : une ligne par station et par heure, enrichie (stations + météo)
status = spark.table("silver_status_history")
stations = spark.table("silver_station").select("station_id", "station_name", "capacity", "lat", "lon")

gold = (
    status.withColumn("hour_ts", F.date_trunc("hour", "snapshot_ts"))
    .groupBy("station_id", "hour_ts")
    .agg(
        F.round(F.avg("bikes"), 2).alias("avg_bikes"),
        F.min("bikes").alias("min_bikes"),
        F.round(F.avg("ebike"), 2).alias("avg_ebike"),
        F.round(F.avg("docks"), 2).alias("avg_docks"),
        F.round(F.avg(F.when(F.col("bikes") == 0, 1).otherwise(0)), 3).alias("pct_time_empty"),
        F.round(F.avg(F.when(F.col("docks") == 0, 1).otherwise(0)), 3).alias("pct_time_full"),
    )
    .join(stations, "station_id", "left")
    .join(silver_weather.withColumnRenamed("weather_hour", "hour_ts"), "hour_ts", "left")
    # stockage en UTC, variables calendaires en heure de Paris
    .withColumn("hour_of_day", F.hour(F.from_utc_timestamp("hour_ts", "Europe/Paris")))
    .withColumn("day_of_week", F.dayofweek(F.from_utc_timestamp("hour_ts", "Europe/Paris")))
    .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
)

gold.write.mode("overwrite").format("delta").save(f"{GOLD_TABLES}/gold_station_hourly")
print("gold_station_hourly :", gold.count(), "lignes")
display(gold.orderBy(F.desc("hour_ts")).limit(10))
