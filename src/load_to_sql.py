"""
 Flight Analytics 2024 – CSV → SQL Server Loader
--------------------------------------------------
Lädt die bereinigte CSV-Datei (flight_data_2024_clean.csv) performant in
eine SQL-Server-Tabelle (flight_data_2024) – in Chunks, mit fast_executemany.

Voraussetzungen:
- Tabelle per sql/01_create_tables.sql anlegen (einmalig ausführen)
- ODBC Driver 18 for SQL Server installiert
- Python-Pakete: pyodbc, pandas, tqdm

Autor: Fabrice Martial
"""

import os
import math
import pyodbc
import pandas as pd
from tqdm import tqdm


# -----------------------------
#  Konfiguration
# -----------------------------
CSV_PATH = os.path.join("data", "flight_data_2024_clean.csv")
TABLE_NAME = "flight_data_2024"
CHUNK_SIZE = 100_000           
TRUNCATE_BEFORE_LOAD = True    

#  Verbindung 
SQL_CONFIG = {
    
    "server": r"DESKTOP-CFP338R",      
    "database": "FlightAnalytics2024",
    # Windows-Authentifizierung:
    "trusted_connection": "yes",
    
}

# ODBC-Driver und Verbindungsstring
def make_connection_string(cfg: dict) -> str:
    base = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={cfg['server']};"
        f"DATABASE={cfg['database']};"
        "TrustServerCertificate=yes;"
    )
    if cfg.get("trusted_connection") == "yes":
        return base + "Trusted_Connection=yes;"
    else:
        return base + f"UID={cfg['uid']};PWD={cfg['pwd']};"

# -----------------------------
#  Spalten in der richtigen Reihenfolge
#   (muss zur SQL-Tabelle passen)
# -----------------------------
COLUMNS = [
    "year", "month", "day_of_month", "day_of_week", "fl_date",
    "op_unique_carrier", "op_carrier_fl_num", "origin", "origin_city_name",
    "origin_state_nm", "dest", "dest_city_name", "dest_state_nm",
    "crs_dep_time", "dep_time", "dep_delay", "taxi_out", "wheels_off",
    "wheels_on", "taxi_in", "crs_arr_time", "arr_time", "arr_delay",
    "cancelled", "cancellation_code", "diverted", "crs_elapsed_time",
    "actual_elapsed_time", "air_time", "distance", "carrier_delay",
    "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"
]

# Platzhalter für INSERT (35 Spalten => 35 Fragezeichen)
PLACEHOLDERS = ", ".join(["?"] * len(COLUMNS))

INSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    {", ".join(COLUMNS)}
) VALUES ({PLACEHOLDERS})
"""

# -----------------------------
#  DB-Verbindung
# -----------------------------
def get_connection():
    conn_str = make_connection_string(SQL_CONFIG)
    return pyodbc.connect(conn_str, autocommit=False)

# -----------------------------
# Tabelle leeren 
# -----------------------------
def truncate_table_if_requested(cursor):
    if TRUNCATE_BEFORE_LOAD:
        print(f"🧹 TRUNCATE TABLE {TABLE_NAME} …")
        cursor.execute(f"IF OBJECT_ID('{TABLE_NAME}', 'U') IS NOT NULL TRUNCATE TABLE {TABLE_NAME};")
        cursor.commit()

# -----------------------------
#  Typ- und Wertetuning pro Chunk
# -----------------------------
def prepare_chunk(df: pd.DataFrame) -> pd.DataFrame:
    # Nur die erwarteten Spalten (und in derselben Reihenfolge)
    df = df[COLUMNS].copy()

    # --- Spalten nach Typ ---
    numeric_float_cols = [
        "dep_time", "dep_delay", "taxi_out", "wheels_off", "wheels_on",
        "taxi_in", "arr_time", "arr_delay",
        "crs_elapsed_time", "actual_elapsed_time", "air_time", "distance"
    ]
    numeric_int_cols = [
        "year", "month", "day_of_month", "day_of_week",
        "crs_dep_time", "crs_arr_time",
        "carrier_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"
    ]
    bit_cols = ["cancelled", "diverted"]
    text_cols = [
        "op_unique_carrier", "origin", "origin_city_name", "origin_state_nm",
        "dest", "dest_city_name", "dest_state_nm", "cancellation_code"
    ]

    # --- Datum coerzen ---
    df["fl_date"] = pd.to_datetime(df["fl_date"], errors="coerce")

    # --- Flugnummer robust numerisch + nullable Int ---
    df["op_carrier_fl_num"] = pd.to_numeric(df["op_carrier_fl_num"], errors="coerce")

    # --- Floats coercen (nicht-numerisch -> NaN) ---
    for col in numeric_float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- Ints coercen (nicht-numerisch -> NaN) ---
    for col in numeric_int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- Bits auf 0/1 bringen ---
    for col in bit_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        df[col] = df[col].apply(lambda x: 1 if x == 1 else 0)

    # --- Texte säubern ---
    for col in text_cols:
        df[col] = df[col].astype(object).where(pd.notnull(df[col]), None)
        df[col] = df[col].apply(lambda v: v.strip() if isinstance(v, str) else v)

    #  WICHTIG: NaN → None pro Spalte (sonst bleibt NaN in float-Spalten!)
    #    1) erst auf object casten, 2) dann NaN durch None ersetzen
    for col in (numeric_float_cols + numeric_int_cols + ["op_carrier_fl_num"]):
        df[col] = df[col].astype(object)
        df.loc[df[col].isna(), col] = None

    # Flugnummer abschließend wieder sauber als Int64 (nullable) – pyodbc mag auch object/None,
    # aber das hier ist “schöner”. (Wenn es Probleme macht, kommentieren.)
    try:
        df["op_carrier_fl_num"] = pd.Series(df["op_carrier_fl_num"], dtype="Int64")
    except Exception:
        pass  # notfalls als object lassen (None bleibt erhalten)

    return df


# -----------------------------
# CSV in SQL laden (Chunking)
# -----------------------------
def load_csv_in_chunks(csv_path: str):
    total_rows = sum(1 for _ in open(csv_path, "r", encoding="utf-8")) - 1  
    total_batches = math.ceil(total_rows / CHUNK_SIZE)

    print(f" Quelle: {csv_path}")
    print(f" Zeilen gesamt (ohne Header): {total_rows:,}")
    print(f" Chunk-Größe: {CHUNK_SIZE:,} → {total_batches} Batches\n")

    conn = get_connection()
    cursor = conn.cursor()
    cursor.fast_executemany = True  # Turbo für executemany

    try:
        truncate_table_if_requested(cursor)

        batch_idx = 0
        for chunk in tqdm(
            pd.read_csv(csv_path, chunksize=CHUNK_SIZE, parse_dates=["fl_date"]),
            total=total_batches,
            desc="Lade nach SQL"
        ):
            batch_idx += 1

            # vorbereiten (Reihenfolge, Typen, NULL)
            chunk = prepare_chunk(chunk)

            # In Liste von Tupeln umwandeln
            rows = [tuple(record) for record in chunk.to_numpy()]

            # Insert
            cursor.executemany(INSERT_SQL, rows)
            conn.commit()

        print("\n Import abgeschlossen!")
    except Exception as e:
        conn.rollback()
        print("\n❌ Fehler – Transaktion zurückgerollt.")
        raise e
    finally:
        cursor.close()
        conn.close()

# -----------------------------
#  Main
# -----------------------------
if __name__ == "__main__":
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV nicht gefunden: {CSV_PATH}")
    load_csv_in_chunks(CSV_PATH)
    print("\n Fertig! Daten sind in SQL Server verfügbar.")


