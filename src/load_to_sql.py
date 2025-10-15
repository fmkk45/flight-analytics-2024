"""
Flight Analytics 2024 – CSV → SQL Server Loader
------------------------------------------------
Zweck:
- Lädt die bereinigte CSV (flight_data_2024_clean.csv) in die SQL-Tabelle flight_data_2024.
- In Chunks und mit fast_executemany für Tempo.
- Robust gegenüber schiefen Typen (NaN/Strings in Zahlen-Spalten → sauber nach NULL).

Voraussetzungen:
- Ziel-Tabelle existiert (siehe eigenes CREATE-SQL).
- ODBC Driver für SQL Server installiert (hier: 17).
- Pakete: pyodbc, pandas, tqdm.

Autor: Fabrice Martial
"""

import os
import math
import pyodbc
import pandas as pd
from tqdm import tqdm

# -------------------------------------------------
# Basiskonfiguration
# -------------------------------------------------
CSV_PATH = os.path.join("data", "flight_data_2024_clean.csv")
TABLE_NAME = "flight_data_2024"

# Große Dateien nicht auf einmal laden → weniger RAM, stabiler Fehlerfall
CHUNK_SIZE = 100_000

# Vor dem Laden die Tabelle leeren (TRUNCATE). Auf False setzen, wenn anhängen gewünscht.
TRUNCATE_BEFORE_LOAD = True

# Verbindungsparameter (Windows-Login = Trusted_Connection)
SQL_CONFIG = {
    "server": r"DESKTOP-CFP338R",     # ggf. DESKTOP-... \ SQLEXPRESS
    "database": "FlightAnalytics2024",
    "trusted_connection": "yes",
    # Für SQL-Login: "trusted_connection": "no", zusätzlich "uid"/"pwd"
}

def make_connection_string(cfg: dict) -> str:
    """
    Baut den ODBC-Connection-String.
    Hinweis: 'TrustServerCertificate=yes' und 'Encrypt=no' hält das Setup simpel
    für lokale DEV-Umgebungen. In PROD anders konfigurieren.
    """
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

# -------------------------------------------------
# Spaltenreihenfolge muss exakt zur Zieltabelle passen
# -------------------------------------------------
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

PLACEHOLDERS = ", ".join(["?"] * len(COLUMNS))

INSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    {", ".join(COLUMNS)}
) VALUES ({PLACEHOLDERS})
"""

# -------------------------------------------------
# Verbindung öffnen
# -------------------------------------------------
def get_connection():
    conn_str = make_connection_string(SQL_CONFIG)
    return pyodbc.connect(conn_str, autocommit=False)

# -------------------------------------------------
# Tabelle leeren (optional)
# -------------------------------------------------
def truncate_table_if_requested(cursor):
    if TRUNCATE_BEFORE_LOAD:
        print(f" TRUNCATE TABLE {TABLE_NAME} …")
        cursor.execute(f"IF OBJECT_ID('{TABLE_NAME}', 'U') IS NOT NULL TRUNCATE TABLE {TABLE_NAME};")
        cursor.commit()

# -------------------------------------------------
# Chunk vor dem Insert typ- und werteseitig geradeziehen
# -------------------------------------------------
def prepare_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Erwartet: DataFrame mit allen COLUMNS (Reihenfolge egal).
    Liefert:  DataFrame mit exakt COLUMNS-Reihenfolge, korrekten Typen
              und None statt NaN (wichtig für pyodbc/SQL NULL).
    """
    # 1) Nur erwartete Spalten & in definierter Reihenfolge
    df = df[COLUMNS].copy()

    # 2) Typ-Gruppen definieren
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

    # 3) Datum sicher konvertieren
    df["fl_date"] = pd.to_datetime(df["fl_date"], errors="coerce")

    # 4) Flugnummer robust numerisch
    df["op_carrier_fl_num"] = pd.to_numeric(df["op_carrier_fl_num"], errors="coerce")

    # 5) Floats erzwingen (Strings → NaN)
    for col in numeric_float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 6) Ints erzwingen (Strings → NaN)
    for col in numeric_int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 7) Bits auf 0/1 normalisieren
    for col in bit_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        df[col] = df[col].apply(lambda x: 1 if x == 1 else 0)

    # 8) Texte trimmen + leere auf None
    for col in text_cols:
        df[col] = df[col].astype(object).where(pd.notnull(df[col]), None)
        df[col] = df[col].apply(lambda v: v.strip() if isinstance(v, str) else v)

    # 9) NaN → None, damit SQL NULL bekommt (vor allem in Float/Int-Spalten)
    for col in (numeric_float_cols + numeric_int_cols + ["op_carrier_fl_num"]):
        df[col] = df[col].astype(object)
        df.loc[df[col].isna(), col] = None

    # 10) Flugnummer optional „schön“ als pandas-Int64 (nullable)
    try:
        df["op_carrier_fl_num"] = pd.Series(df["op_carrier_fl_num"], dtype="Int64")
    except Exception:
        # Wenn’s knallt, reicht auch object/None für den Insert
        pass

    return df

# -------------------------------------------------
# CSV → SQL in Chunks
# -------------------------------------------------
def load_csv_in_chunks(csv_path: str):
    # Zeilen zählen (Header abziehen)
    total_rows = sum(1 for _ in open(csv_path, "r", encoding="utf-8")) - 1
    total_batches = math.ceil(total_rows / CHUNK_SIZE)

    print(f" Quelle: {csv_path}")
    print(f" Zeilen gesamt (ohne Header): {total_rows:,}")
    print(f" Chunk-Größe: {CHUNK_SIZE:,} → {total_batches} Batches\n")

    conn = get_connection()
    cursor = conn.cursor()
    cursor.fast_executemany = True  # deutlicher Geschwindigkeitsgewinn bei executemany

    try:
        truncate_table_if_requested(cursor)

        # pandas liefert hier Iterator von DataFrames (Chunk für Chunk)
        for chunk in tqdm(
            pd.read_csv(csv_path, chunksize=CHUNK_SIZE, parse_dates=["fl_date"]),
            total=total_batches,
            desc="Lade nach SQL"
        ):
            # pro Chunk Typen/NULLs ordnen
            chunk = prepare_chunk(chunk)

            # DataFrame → Tupel-Liste (passt zu ?-Platzhaltern)
            rows = [tuple(record) for record in chunk.to_numpy()]

            # Bulk Insert
            cursor.executemany(INSERT_SQL, rows)
            conn.commit()

        print("\n Import abgeschlossen!")
    except Exception as e:
        # Bei Fehlern alles zurückrollen (keine halben Inserts)
        conn.rollback()
        print("\n❌ Fehler – Transaktion zurückgerollt.")
        raise e
    finally:
        cursor.close()
        conn.close()

# -------------------------------------------------
# Einstiegspunkt
# -------------------------------------------------
if __name__ == "__main__":
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV nicht gefunden: {CSV_PATH}")
    load_csv_in_chunks(CSV_PATH)
    print("\n Fertig! Daten sind in SQL Server verfügbar.")
