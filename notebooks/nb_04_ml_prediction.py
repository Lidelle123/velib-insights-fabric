# Fabric notebook — nb_04_ml_prediction
# Langage : PySpark (+ pandas / scikit-learn) · Lakehouse par défaut : lh_gold
# Prédire le nombre de vélos disponibles dans 1 heure, par station.
# Suivi des essais et registre de modèles : MLflow (expérience exp_velib_bikes_1h, modèle velib_bikes_1h)
#
# Résultats (données du 16 au 26/10/2026, test = 2 derniers jours) :
#   Persistance (référence)               MAE 1,11 vélo
#   v1 HistGradientBoosting (niveau)      MAE 1,17  (-4,9 %)
#   v2 HGB (variation + profil horaire)   MAE 1,23  (-10,4 %)
# -> À 1 h et avec 10 jours d'historique, la persistance n'est pas battue (voir README).

# %% Cellule 1 — Préparation, entraînement (v2), comparaison à la référence
import pandas as pd, mlflow
from pyspark.sql import functions as F, Window
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

GOLD = "abfss://ws-velib-dev@onelake.dfs.fabric.microsoft.com/lh_gold.Lakehouse/Tables"
df = spark.read.format("delta").load(f"{GOLD}/gold_station_hourly")

# lag = passé (variable), lead = futur (cible), calculés station par station
w = Window.partitionBy("station_id").orderBy("hour_ts")
df_feat = (df.withColumn("bikes_lag1", F.lag("avg_bikes", 1).over(w))
             .withColumn("target_next_hour", F.lead("avg_bikes", 1).over(w))
             .withColumn("next_ts", F.lead("hour_ts", 1).over(w)))

BASE = ["avg_bikes", "bikes_lag1", "pct_time_empty", "hour_of_day", "day_of_week",
        "is_weekend", "capacity", "temperature_c", "precipitation_mm", "wind_kmh"]
FEATURES = BASE + ["delta_1h", "profile_next", "gap_to_profile"]

# on ne garde que les lignes dont l'heure suivante existe réellement, stations actives uniquement
train_df = df_feat.filter((F.col("next_ts") == F.col("hour_ts") + F.expr("INTERVAL 1 HOUR"))
                          & (F.col("capacity") > 0))
pdf = train_df.select("station_id", "hour_ts", *BASE, "target_next_hour").toPandas()
pdf["delta_1h"] = pdf["avg_bikes"] - pdf["bikes_lag1"]

# Découpage TEMPOREL (pas aléatoire) pour éviter toute fuite de données
cutoff = pdf["hour_ts"].max() - pd.Timedelta(days=2)
train, test = pdf[pdf.hour_ts <= cutoff].copy(), pdf[pdf.hour_ts > cutoff].copy()

# Profil habituel de la station à l'heure suivante, appris sur l'entraînement uniquement
profile = (train.groupby(["station_id", "hour_of_day"])["avg_bikes"].mean()
                .rename("profile_next").reset_index())
profile["hour_of_day"] = (profile["hour_of_day"] - 1) % 24

def add_profile(d):
    d = d.merge(profile, on=["station_id", "hour_of_day"], how="left")
    d["gap_to_profile"] = d["profile_next"] - d["avg_bikes"]
    return d

train, test = add_profile(train), add_profile(test)

mlflow.set_experiment("exp_velib_bikes_1h")
with mlflow.start_run(run_name="hgb_v2_delta_profile"):
    model = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, random_state=42)
    model.fit(train[FEATURES], train["target_next_hour"] - train["avg_bikes"])   # cible = variation

    pred = (test["avg_bikes"] + model.predict(test[FEATURES])).clip(lower=0)
    mae_model    = mean_absolute_error(test["target_next_hour"], pred)
    mae_baseline = mean_absolute_error(test["target_next_hour"], test["avg_bikes"])   # persistance

    mlflow.log_params({"model": "HGB", "target": "delta_1h", "features": len(FEATURES)})
    mlflow.log_metrics({"mae_model": mae_model, "mae_baseline": mae_baseline})
    mlflow.sklearn.log_model(model, "model", registered_model_name="velib_bikes_1h",
                             input_example=test[FEATURES].head(5))

print(f"Référence (persistance) : {mae_baseline:.3f} vélos")
print(f"Modèle v2               : {mae_model:.3f} vélos")
print(f"Amélioration            : {100 * (1 - mae_model / mae_baseline):.1f} %")


# %% Cellule 2 — Scoring : prédiction pour l'heure suivante -> gold_predictions
latest = (df_feat.withColumn("rk", F.row_number().over(
              Window.partitionBy("station_id").orderBy(F.desc("hour_ts")))).filter("rk = 1"))

lp = latest.select("station_id", "station_name", "hour_ts", *BASE).toPandas()
lp = lp[lp["capacity"] > 0]
lp["delta_1h"] = lp["avg_bikes"] - lp["bikes_lag1"]
lp = add_profile(lp)

lp["pred_bikes_next_hour"] = (lp["avg_bikes"] + model.predict(lp[FEATURES])).clip(lower=0).round(1)
lp["predicted_for"] = lp["hour_ts"] + pd.Timedelta(hours=1)
# alerte anticipée : station encore approvisionnée maintenant, presque vide dans 1 h
lp["risque_vide"] = ((lp["avg_bikes"] >= 2) & (lp["pred_bikes_next_hour"] < 2)).astype(int)

out = spark.createDataFrame(lp[["station_id", "station_name", "predicted_for",
                                "avg_bikes", "pred_bikes_next_hour", "risque_vide"]])
out.write.mode("overwrite").format("delta").save(f"{GOLD}/gold_predictions")
print("Stations à risque de se vider dans l'heure :", int(lp["risque_vide"].sum()))
