# Fabric notebook — nb_01_ingest_velib
# Langage : Python (sans Spark) · Lakehouse par défaut : lh_bronze
# Planification : toutes les 15 minutes
#
# 1) Collecte l'état des stations Vélib' (API GBFS) et l'enregistre BRUT dans le Bronze
# 2) Envoie un événement par station vers l'Eventstream (point de terminaison personnalisé)

# %% Cellule 1 — Collecte vers le Bronze
import os, json, requests
from datetime import datetime, timezone

BASE_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
ROOT = "/lakehouse/default/Files/raw/velib"      # zone Files de lh_bronze

now = datetime.now(timezone.utc)                    # toujours en UTC
date_part = now.strftime("%Y-%m-%d")
time_part = now.strftime("%H%M%S")

def fetch(endpoint):
    """Appelle l'API et échoue explicitement en cas d'erreur HTTP."""
    r = requests.get(f"{BASE_URL}/{endpoint}.json", timeout=30)
    r.raise_for_status()
    return r.json()

def save(payload, folder, filename):
    """Enregistre le JSON tel quel (règle du Bronze : aucune transformation)."""
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/{filename}"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return path

# Statut des stations : à chaque exécution
status = fetch("station_status")
p1 = save(status, f"{ROOT}/station_status/date={date_part}", f"station_status_{time_part}.json")
print("Statut enregistré :", p1, "-", len(status["data"]["stations"]), "stations")

# Référentiel des stations : une fois par jour
info_folder = f"{ROOT}/station_information/date={date_part}"
if not os.path.exists(info_folder):
    info = fetch("station_information")
    p2 = save(info, info_folder, "station_information.json")
    print("Référentiel enregistré :", p2, "-", len(info["data"]["stations"]), "stations")
else:
    print("Référentiel déjà collecté aujourd'hui")


# %% Cellule 2 — Envoi vers l'Eventstream (Event Hub REST + jeton SAS)
import time, hmac, hashlib, base64, urllib.parse

# Le secret est stocké HORS du code, dans le lakehouse (jamais versionné)
with open("/lakehouse/default/Files/_config/eventstream_conn.txt", encoding="utf-8-sig") as f:
    conn_str = f.read().strip().strip('"')

parts = {}
for p in conn_str.split(";"):
    if "=" in p:
        k, v = p.split("=", 1)
        parts[k.strip()] = v.strip()

namespace_host = parts["Endpoint"].replace("sb://", "").strip("/")
key_name, key, entity = parts["SharedAccessKeyName"], parts["SharedAccessKey"], parts["EntityPath"]

def sas_token(uri, key_name, key, ttl=3600):
    """Signature temporaire HMAC-SHA256 : la clé ne transite jamais sur le réseau."""
    encoded_uri = urllib.parse.quote_plus(uri)
    expiry = str(int(time.time()) + ttl)
    to_sign = f"{encoded_uri}\n{expiry}".encode("utf-8")
    sig = base64.b64encode(hmac.new(key.encode("utf-8"), to_sign, hashlib.sha256).digest())
    return f"SharedAccessSignature sr={encoded_uri}&sig={urllib.parse.quote_plus(sig)}&se={expiry}&skn={key_name}"

resource_uri = f"https://{namespace_host}/{entity}"
headers = {
    "Authorization": sas_token(resource_uri, key_name, key),
    "Content-Type": "application/vnd.microsoft.servicebus.json",
}

def to_event(s):
    """Aplatit une station GBFS en un événement simple."""
    types = {k: v for d in s.get("num_bikes_available_types", []) for k, v in d.items()}
    return {
        "station_id": s["station_id"],
        "station_code": s.get("stationCode"),
        "bikes_available": s.get("num_bikes_available"),
        "mechanical": types.get("mechanical"),
        "ebike": types.get("ebike"),
        "docks_available": s.get("num_docks_available"),
        "is_installed": s.get("is_installed"),
        "is_renting": s.get("is_renting"),
        "is_returning": s.get("is_returning"),
        "last_reported": datetime.fromtimestamp(s["last_reported"], timezone.utc).isoformat(),
        "ingestion_time": now.isoformat(),
    }

events = [to_event(s) for s in status["data"]["stations"]]

BATCH = 400   # limite Event Hub : 1 Mo par envoi
for i in range(0, len(events), BATCH):
    body = json.dumps([{"Body": json.dumps(e)} for e in events[i:i + BATCH]])
    r = requests.post(f"{resource_uri}/messages?timeout=60&api-version=2014-01",
                      headers=headers, data=body, timeout=60)
    r.raise_for_status()

print(f"{len(events)} événements envoyés vers l'Eventstream")
