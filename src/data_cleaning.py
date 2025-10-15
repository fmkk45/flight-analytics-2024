"""
 Flight Data 2024 – Data Cleaning Script
==========================================

Dieses Skript bereinigt Flugdaten aus dem Kaggle-Datensatz „Flight Data 2024“.
Ziel ist es, eine saubere CSV-Datei zu erzeugen, die für SQL-Analysen
und Power BI Dashboards verwendet werden kann.

Autor: Fabrice Martial
Datum: 2025-10-13
"""

# ------------------------------------------------------------
#  1. Bibliotheken importieren
# ------------------------------------------------------------
import pandas as pd

# ------------------------------------------------------------
#  2. Daten einlesen (Testweise 100.000 Zeilen)
# ------------------------------------------------------------
print(" Lade Teilmenge der großen Flugdatendatei (100.000 Zeilen)...\n")
df = pd.read_csv("data/flight_data_2024.csv", nrows=100000)

print(" Datei erfolgreich geladen.")
print("Anzahl Zeilen und Spalten:", df.shape)
print("\nSpaltenübersicht:")
print(df.columns.tolist())

# ------------------------------------------------------------
#  3. Überblick über fehlende Werte
# ------------------------------------------------------------
print("\n Fehlende Werte (Top 10 Spalten):")
print(df.isnull().sum().sort_values(ascending=False).head(10))

# ------------------------------------------------------------
#  4. Datentypen prüfen
# ------------------------------------------------------------
print("\n Datentypen:")
print(df.dtypes)

# ------------------------------------------------------------
#  5. Beispielzeilen anzeigen
# ------------------------------------------------------------
print("\n Beispiel-Daten:")
print(df.head(5))


# ------------------------------------------------------------
# 🔹 6. Zusätzliche Dateien laden (Datenwörterbuch + Sample)
# ------------------------------------------------------------
print("\n Lade Data Dictionary und Sample-Datei...")

data_dict = pd.read_csv("data/flight_data_2024_data_dictionary.csv")
print("\n Data Dictionary Vorschau:")
print(data_dict.head())

df_sample = pd.read_csv("data/flight_data_2024_sample.csv")
print("\n Sample-Datei Vorschau:")
print(df_sample.head())


# ------------------------------------------------------------
#  7. Bereinigungsschritte anwenden
# ------------------------------------------------------------
print("\n Beginne mit der Datenbereinigung...")

# Kopie verwenden, um das Original zu schützen
df = df_sample.copy()

# 1️ NaN in 'cancellation_code' belassen, wenn Flug storniert wurde;
# andernfalls durch "Not Cancelled" ersetzen.
df['cancellation_code'] = df.apply(
    lambda x: x['cancellation_code'] if x['cancelled'] == 1 else 'Not Cancelled',
    axis=1
)

# 2️ Fehlende Werte in numerischen Spalten mit 0 ersetzen
num_cols = [
    'dep_delay', 'arr_delay', 'air_time', 'taxi_out', 'taxi_in',
    'carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay'
]
df[num_cols] = df[num_cols].fillna(0)

# 3️ Datentypen konvertieren
#    - fl_date → Datum
#    - op_carrier_fl_num → Ganzzahl (Flugnummer)
df['fl_date'] = pd.to_datetime(df['fl_date'], errors='coerce')
df['op_carrier_fl_num'] = df['op_carrier_fl_num'].astype('Int64')

# ------------------------------------------------------------
#  8. Kontrolle nach der Bereinigung
# ------------------------------------------------------------
print("\n Nach Bereinigung:")
print(df.info())

print("\n Beispiel nach Cleaning:")
print(df.head(3))

# ------------------------------------------------------------
#  9. Ergebnis speichern
# ------------------------------------------------------------
output_path = "data/flight_data_2024_clean.csv"
df.to_csv(output_path, index=False)
print(f"\n Bereinigte Datei gespeichert unter: {output_path}")

# ------------------------------------------------------------
#  10. Abschließende Qualitätsprüfung
# ------------------------------------------------------------
total_missing = df.isnull().sum().sum()
print(f"\n Gesamtzahl fehlender Werte nach Cleaning: {total_missing}")

if total_missing == 0:
    print(" Alle fehlenden Werte erfolgreich bereinigt!")
else:
    print(" Es sind noch einige fehlende Werte vorhanden (siehe Details oben).")
