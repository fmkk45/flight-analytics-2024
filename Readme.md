# ✈️ Flight Analytics 2024 – End-to-End Data Project

## Überblick
Dieses Projekt analysiert US-Flugdaten aus 2024 und demonstriert einen vollständigen Analytics-Workflow von der Datenbereinigung über das Laden in eine relationale Datenbank bis hin zu Auswertungen und Visualisierungen. Ziel ist es, realistische Fragestellungen zu Luftverkehr, Verspätungen und Stornierungen zu beantworten und dabei belastbare, wiederholbare Prozesse aufzubauen.

## Ziele
- Daten verlässlich bereinigen, anreichern und verständlich dokumentieren.
- Performantes Laden großer CSV-Daten in SQL Server (robust gegen fehlerhafte Werte).
- Kennzahlen und Muster sichtbar machen (Verspätungen, Stornoraten, saisonale Effekte, Routen).
- Ergebnisse als Diagramme sichern und für Reporting/BI aufbereiten.

## Datenquellen
- Kaggle-Datensatz „Flight Data 2024“ (Rohdatei, Sample und Data Dictionary).
- Relevante Attribute u. a.: Datum, Airline-Code, Flugnummer, Start/Ziel (Airport & Stadt), Plan- und Ist-Zeiten, Verzögerungen (Carrier, Wetter, NAS, Security, Late Aircraft), Distanz, Stornierungen und Umleitungen.

## Verwendete Tools & Technologien
- Programmiersprache: Python
- Bibliotheken: pandas (Datenverarbeitung), NumPy (Numerik), Matplotlib (Visualisierung)
- Analyseumgebung: Jupyter Notebook
- Datenbank: Microsoft SQL Server
- Anbindung: ODBC/pyodbc (transaktionales, chunk-basiertes Laden)


## Vorgehen & Workflow
1) **Datenverständnis**  
   Sichtung der Struktur anhand von Sample und Data Dictionary. Identifikation kritischer Spalten (Zeitstempel, Delays, Storno-Flag, Distanz) und potenzieller Datenprobleme (fehlende Werte, „NA“/leere Strings, inkonsistente Typen).

2) **Datenbereinigung (Cleaning)**  
   Konvertierung von Datumsangaben in echte Datumsformate. Typbereinigung: numerische Spalten werden auf numerisch geprüft; unlesbare Werte werden kontrolliert in fehlende Werte überführt. Klare Regeln für Storni (nicht stornierte Flüge → neutraler Code „Not Cancelled“). Einheitliche Behandlung fehlender Werte, sodass beim späteren Laden echte Datenbank-NULLs entstehen.

3) **Laden in SQL Server (ETL-Teil)**  
   Erstellen einer passenden Zieltabelle, die NULL-Werte zulässt und für große Datenmengen ausgelegt ist. Chunk-basiertes, transaktionales Laden (sicher gegen fehlerhafte Zeilen und mit klaren Fehlermeldungen). Nach dem Import wurden sinnvolle Indizes angelegt (z. B. auf Datum, Airline, Route), um Abfragen und Reporting zu beschleunigen.

4) **Analyse & Visualisierung**  
   Kennzahlen (KPIs): Gesamtzahl Flüge, Ø Ankunfts- und Abflugverspätung (Minuten), Anzahl Stornos, Stornorate. Mustererkennung und Vergleiche:
   - Flüge pro Monat (Saisonalität)
   - Ø Ankunftsverspätung je Airline (Carrier-Vergleich)
   - Stornorate pro Monat (Trends und Peaks)
   - Distanz vs. Flugzeit (Plausibilitätscheck)
   - Top-Routen nach Anzahl Flügen (Netzwerk-Charakteristik)
   Export der wichtigsten Diagramme als PNG in den Ordner `visuals/` zur Dokumentation.



## Wichtige Ergebnisse & Learnings (Beispiele)
- Saisonalität und Volumenschwankungen werden über Flüge-pro-Monat deutlich sichtbar.
- Airlines unterscheiden sich spürbar in der durchschnittlichen Ankunftsverspätung (Hinweis auf operative Unterschiede).
- Stornoraten zeigen Peaks in bestimmten Monaten Ansatzpunkt für Ursachenanalyse (z. B. Wetter/Operative Kapazität).
- Distanz und Flugzeit korrelieren erwartungsgemäß; Ausreißer helfen, potenzielle Daten- oder Prozessbesonderheiten zu entdecken.
- Durch robuste Typ-Konvertierung und konsequente NULL-Behandlung werden Ladefehler vermieden und Analysen belastbarer.



## Mögliche Erweiterungen
- Zeitreihen-Forecasts (z. B. erwartete Verspätung/Stornorate je Monat oder Airline).
- Ursachenanalyse von Delays (Carrier vs. Wetter vs. NAS etc.) mit statistischen Tests.
- Geovisualisierung von Routen (Airports/Koordinaten anreichern).
- Automatisierte Pipeline (z. B. geplanter Import, Qualitätstests, Report-Export).

## Autor
**Fabrice Martial Kamwameugne Kuokam** Student in Informatik  (Python | SQL | Power BI)
