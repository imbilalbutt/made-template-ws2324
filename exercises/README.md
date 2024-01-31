# Exercises

During the course there were also five exercises given to work on. All of them required to build a datapipeline based on a given task and a given dataset. The exercises should be solved alternately [Python](https://www.python.org/) and [Jayvee](https://github.com/jvalue/jayvee).  
Automated Feedback to the exercises was provided using a GitHub action, which is defined in `.github/workflows/exercise-feedback.yml`

## Exercise 1

-   Build an automated data pipeline for the following source:
    

-   [https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv](https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv)
    
Goal
    
-   Write data into a SQLite database called “airports.sqlite”, in the table “airports”
    
-   Assign fitting built-in SQLite types (e.g., BIGINT, TEXT or FLOAT) to all columns
    
-   Do not rename column names!
    
-   No further data validation is required, do not drop any rows or change any data points
    

## Exercise 2

-   Build an automated data pipeline for the following source:
    

-   direct link to CSV: [https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV](https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV)
    
Goal
    
-   Write data into a SQLite database called “trainstops.sqlite”, in the table “trainstops”
    
-   First, drop the "Status" column
    
-   Then, drop all rows with invalid values:
   
-   Valid "Verkehr" values are "FV", "RV", "nur DPN"
    
-   Valid "Laenge", "Breite" values are geographic coordinate system values between and including -90 and 90
    
-   Valid "IFOPT" values follow this pattern.
-   exactly two characters : any amount of numbers : any amount of numbers. Optionally another colon followed by any amount of numbers.
    
-   This is not the official IFOPT standard, please follow our guidelines and not the official standard
    
-   Empty cells are considered invalid
   
-   Use fitting SQLite types (e.g., BIGINT, TEXT or FLOAT) for all columns

    

## Exercise 3

-   Build an automated data pipeline for the following source:
    

-   Direct download link: [https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv](https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv)
    

Goal
    
-   Write data into a SQLite database called “cars.sqlite”, in the table “cars”
    
-   Pick suitable encoding:
   
-   Make sure to preserve the German special letters like “ü” or “ä”
   
-   Reshape data structure
    
-   Ignore the first 6 lines and last 4 lines as metadata
    
-   Keep only the following columns, rename them to the new name given here (columns M-BU contain summary data)
    

-   Column A: date
    
-   Column B: CIN
    
-   Column C: name
    
-   Column M: petrol
    
-   Column W: diesel
    
-   Column AG: gas
    
-   Column AQ: electro
    
-   Column BA: hybrid
    
-   Column BK: plugInHybrid
    
-   Column BU: others
    
-   Drop all other columns
    
-   Validate data
    
-   date/name are strings; no need to validate the date
    
-   CINs are [Community Identification Numbers](https://en.wikipedia.org/wiki/Community_Identification_Number), must be strings with 5 characters, and can have a leading 0
    
-   all other columns should be positive integers > 0
    
-   drop all rows that contain invalid values
    

-   Use fitting SQLite types (e.g., BIGINT, TEXT, or FLOAT) for all columns

## Exercise 4

-   Build an automated data pipeline for the following source

-   Direct download link: [https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip](https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip)
    

 Goal
    
-   Download and unzip data
    

-   Use the “data.csv” in the zip file
    
-   for Python, consider using ‘urllib.request.urlretrieve’ instead of the request library to download the ZIP file
    
-   for Jayvee, if you use the FilePicker, do not use a leading dot in file paths, see this bug: [https://github.com/jvalue/jayvee/issues/381](https://github.com/jvalue/jayvee/issues/381)
    

-   Reshape data
    
-   Only use the columns "Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)", "Batterietemperatur in °C", "Geraet aktiv"
    
-   Rename "Temperatur in °C (DWD)" to "Temperatur"
    
-   Rename "Batterietemperatur in °C" to "Batterietemperatur"
    
-   There can be multiple temperature measurements per row
    
-   discard all columns to the right of “​​Geraet aktiv”
    
-   Transform data
    

-   Transform temperatures in Celsius to Fahrenheit (formula is (TemperatureInCelsius * 9/5) + 32) in place (keep the same column names)
    
-   Columns Temperatur and Batterietemperatur
    
-   Validate data
    
-   Use validations as you see fit, e.g., for “Geraet” to be an id over 0
    
-   Use fitting SQLite types (e.g., BIGINT, TEXT or FLOAT) for all columns
    
-   Write data into a SQLite database called “temperatures.sqlite”, in the table “temperatures”
    

## Exercise 5
    
-   Build an automated data pipeline for the following source:
    
-   Direct download link: https://gtfs.rhoenenergie-bus.de/GTFS.zip
    
 Goal
    
-   Work with GTFS data
    
for Python, consider using ‘urllib.request.urlretrieve’ instead of the request library to download the ZIP file
for Jayvee, if you use the FilePicker, do not use a leading dot in file paths, see this bug: https://github.com/jvalue/jayvee/issues/381
    
-   Pick out only stops (from stops.txt)
    
-   Only the columns stop_id, stop_name, stop_lat, stop_lon, zone_id with fitting data types
    
-   Filter data
    
-   Only keep stops from zone 2001
    
-   Validate data
    
-   stop_name can be any text but must maintain german umlauts
    
-   stop_lat/stop_lon must be a geographic coordinates between -90 and 90, including upper/lower bounds
    
-   Drop rows containing invalid data
    
-   Use fitting SQLite types (e.g., BIGINT, TEXT or FLOAT) for all columns
    
-   Write data into a SQLite database called “gtfs.sqlite”, in the table “stops”
    