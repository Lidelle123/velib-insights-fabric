# Fabric notebook — nb_02_silver_station
# Langage : PySpark · Lakehouse par défaut : lh_silver
# Référentiel des stations : JSON brut (Bronze) -> table Delta silver_station (Silver)

# %% Cellule 1 — Lecture du Bronze (autre lakehouse, via son chemin OneLake)
from pyspark.sql import functions as F

BRONZE = "abfss://ws-velib-dev@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/raw/velib/station_information"

raw = spark.read.option("multiLine", True).json(BRONZE)
raw.select("date").distinct().orderBy("date").show()   # colonne "date" issue du partitionnement date=YYYY-MM-DD


# %% Cellule 2 — Transformation : une ligne par station, colonnes typées
latest = raw.agg(F.max("date")).first()[0]
print("Référentiel utilisé :", latest)

silver_station = (
    raw.filter(F.col("date") == latest)
       .select(F.explode("data.stations").alias("s"))
       .select(
           F.col("s.station_id").cast("long").alias("station_id"),
           F.col("s.stationCode").alias("station_code"),
           F.col("s.name").alias("station_name"),
           F.col("s.lat").cast("double").alias("lat"),
           F.col("s.lon").cast("double").alias("lon"),
           F.col("s.capacity").cast("int").alias("capacity"),
       )
       .dropDuplicates(["station_id"])
       .withColumn("snapshot_date", F.lit(latest).cast("date"))
       .withColumn("processed_at", F.current_timestamp())
)

print("Nombre de stations :", silver_station.count())
display(silver_station.limit(10))


# %% Cellule 3 — Écriture Delta (photo la plus récente du référentiel)
silver_station.write.mode("overwrite").format("delta").saveAsTable("silver_station")
print("Table silver_station écrite dans lh_silver")
