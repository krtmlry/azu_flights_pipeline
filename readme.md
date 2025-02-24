# AZUL Airlines Flights Data Pipeline (2017-2023)

This data pipeline will extract flight records from csv files provided by ANAC through this link.
The data to be process will only be limited to **regular flights** done by `Azul Linhas Aéreas Brasileiras`.

**Regular flights** are flights done based on schedule provided by the airline or the aviation authority. The flights included under regular flights are usually passenger flights and cargo flights.

This pipeline will only run locally but this will also be improved to run on the cloud.

# Scope
This data pipeline will only process data that was conducted by `Azul Linhas Aéreas Brasileiras` from January 2017 to December 2023.


# Dataset limitations and problems

The dataset contains records where `actual_departure` and `actual_arrival` is null or missing and while this is not a problem for cancelled flights, this was a problem for accomplished flights. There are records where `actual_departure` and `actual_arrival` is missing but the flight was accomplished and the the departure and arrival situation columns are also null or empty.

To accomodate records where `actual_departure` and `actual_arrival` is missing but the flight was accomplished; the scheduled_departure and scheduled_arrival will be used respsectively and an additional column named `mod_flag` where `1 = departure and arrival date is assumed` and `0 = departure and arrival date is not assumed`.

Justificativa column is a column reserved for 


# Pre-requisites

### 1. Data source download
The data provided by ANAC is in csv format and it is distributed by month from 2017 - 2023. To make this process efficient, the files will be downloaded first and then combined into one csv file or convert it into parquet format.

The csv files can be donwloaded through this [link](https://siros.anac.gov.br/siros/registros/diversos/vra/).

### 2. Column name translation (optional)

The default language of the dataset is in portuguese and it will be much easier to process if the columns were translated to english before processing.

### 3. Creating data dictionary

To create csv files for data dictionary run the jupyter notebooks under n`otebooks/data_dictionary`.
Before running the ELT pipeline this step is crucial for creating the dimension tables using dbt.


# Records to remove

### 1. Duplicate records
To ensure uniqueness, all columns will be used to identify unique records.

### 2. Flight number is null or scheduled departure is null
If flight number and scheduled departure is missing the record will be removed.


# Columns to remove

1. **Referencia / Reference**

    This column containts the YYYY-MM-DD of the flight schedule. This is redundant because `scheduled_departure` column exists which contains the flight schedule in `dd-mm-yyyy HH:MM` format.

2. **orig_airport** and **dest_airport**
    
    These column contains the name of the origin and destination airports. This column will be removed and will be processed later on using `orig_airport_icao` and `dest_airport_icao`.


# Dimension tables

Data dictionary will be created before running the pipeline.
The following data should be created and extracted from flight records and will only be limited to flights performed by `AZU`


### 1. Aircrafts (dim_aircrafts)

| Field             | Description               |
| ---               | ---                       |
| `aircraft_icao`   | ICAO code of aircraft     |
| `aircraft_iata`   | IATA code of aircraft     |
| `model_name`      | Model name of aircraft    |
| `manufacturer`    | Manufacturer of aircraft  |

`NOTE`: There is one aircraft with missing information.
Aircraft with icao code **C082** had scheduled flights designated to carry 100 + passengers but I could not find any aircraft that fits the description using its icao code.

### 2. Airports (dim_airports)

| Field             | Description               |
| ---               | ---                       |
| `airport_icao`    | ICAO code of airport      |
| `airport_iata`    | IATA code of airport      |
| `airport_name`    | Name of airport           |
| `latitude`        | Latitude coordinates      |
| `longitude`       | Longitude coordinates     |
| `country_iso`     | ISO country code          |
| `country`         | Country                   |

`NOTE:` There are airports that doesn't have a designated iata code. These airports are typically private and small sized airports and does not have that much air traffic or performs a lot of regular flights.


### 3. Line type code (dim_line_type)

| Line type code    | Description                   |
| ---               | ---                           |
| `N`               | National                      |
| `I`               | International                 |
| `R`               | Regional                      |
| `E`               | Special                       |
| `L`               | Postal Network                |
| `H`               | Sub-regional                  |
| `C`               | Domestic Freighter            |
| `G`               | International Freighter       |


### 4. Arrival and Departure Situation (dim_arr_dep_situation)

| Situation ID | Situation short description | Description                   |
| ---          | ---                         | ---                           | 
| `A`          | Advanced                    | When actual departure or arrival is ahead of schedule                     |
| `P`          | Punctual                    | When actual departure or arrival is exactly on schedule or within +30 minutes from schedule|
| `D1`         | Delay 30 - 60               | When delay is between 31 to 60 minutes |
| `D2`         | Delay 60 - 120              | When delay is between 61 to 120 minutes |
| `D3`         | Delay 120 - 240             | When delay is between 121 to 240 minutes|
| `D4`         | Delay > 240                 | When delay is more than 240 minutes |

    Based on the flight records, a flight is considered delayed if it arrived or departed more than 30 mins from the scheduled time.


### 5. Flight status (dim_flight_status)

| status_id   | description    |
| ---         | -              |
| `0`         | Cancelled      |
| `1`         | Accomplished   |


### 6. Justificativa or justification (dim_justification_recs)

`Justificativa` column contains description of delay or flight cancellation. While it would be a great addition to future analysis, ANAC only provided justificativa until April 2020 and the default language is in portuguese.

The justificativa records will still be kept on a dedicated table for future processing.

Sample table: `azu_justification_records`

|Column | Description|
|-|-|
|`sk_id` | Generated surrogate key for each flight record|
|`justification`| Justification in portuguese|


### 7. Aircraft records (dim_aircraft_recs)

This will store records of aircraft_icao from `stg_03_convert_types_keys`.
dim_aircrafts contains duplicate records of aircraft_icao because some aircrafts have different model names but share the same aircraft icao code under the same manufacturer.

Joining tables where there are duplicate aircraft_icao codes on dim_aircrafts will cause duplicates.

The raw dataset only provided the icao code for the aircraft used for each flight and it is not enough basis for providing the necessary identification to be used in identifying the right aircraft.

The table will have three columns: `sk_id`, `aircraft_icao` and `passenger_count`

|Column | Description|
|-|-|
|`sk_id` | Generated surrogate key for each flight record |
|`aircraft_icao`| aircraft_icao code of aircraft |
|`passenger_count`| aircraft_icao code of aircraft |