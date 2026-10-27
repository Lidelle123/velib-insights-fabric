# Pipelines Data Factory

## `pl_ingest_weather` — météo quotidienne (Bronze)

| Élément | Valeur |
|---|---|
| Activité | **Copier des données** `Copy_OpenMeteo` |
| Paramètres du pipeline | `latitude` = `48.8566`, `longitude` = `2.3522` (String) |
| Source | Connexion HTTP `conn_http_openmeteo` (`https://api.open-meteo.com`, anonyme), méthode GET, format **Binaire** |
| URL relative (contenu dynamique) | `@concat('v1/forecast?latitude=', pipeline().parameters.latitude, '&longitude=', pipeline().parameters.longitude, '&hourly=temperature_2m,precipitation,wind_speed_10m,weather_code&past_days=1&forecast_days=2&timezone=UTC')` |
| Destination | `lh_bronze` → Files, format **Binaire** |
| Dossier | `@concat('raw/weather/date=', formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd'))` |
| Fichier | `@concat('openmeteo_', formatDateTime(pipeline().TriggerTime, 'HHmmss'), '.json')` |
| Planification | Tous les jours à 06:00 (Europe/Paris) |

Choix :
- **Binaire** : copie octet par octet, sans interprétation — règle du Bronze.
- **`pipeline().TriggerTime`** plutôt que `utcNow()` : une seule heure de référence pour toute l'exécution (dossier et fichier cohérents, même autour de minuit).
- **Paramètres** : le même pipeline peut servir à une autre ville sans modification.

## `pl_master_daily` — orchestration des traitements

```
nb_02_silver_station  ──(réussite)──►  nb_03_silver_gold  ──(réussite)──►  nb_04_ml_prediction
```

| Élément | Valeur |
|---|---|
| Activités | 3 activités **Notebook**, enchaînées sur la condition **En cas de réussite** |
| Planification | Tous les jours à 06:30 (Europe/Paris), après la collecte météo de 06:00 |
| Échecs | Notification par e-mail (paramètres de planification) |

La collecte Vélib' (`nb_01_ingest_velib`) tourne séparément, toutes les 15 minutes, car elle alimente aussi le temps réel.
