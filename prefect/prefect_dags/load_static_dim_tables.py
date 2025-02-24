import os
from dotenv import load_dotenv
import psycopg2
from prefect import task, flow

"""
This script will only run once.
Create and load data to dimension tables:
dim_airports, dim_aircrafts, dim_line_types, dim_flight_status, dim_flight_situation
The following dimension tables are assumed to have fixed values and will not change for this project.

Dimension tables dim_datetime, dim_justification_recs will not have fixed values and will be created using DBT.
"""

@task(name='Initialize Database Credentials')
def task_1():
    load_dotenv()
    db_creds = {
        'dbname':os.getenv('pg_database'),
        'user':os.getenv('pg_user'),
        'password':os.getenv('pg_pass'),
        'host':os.getenv('pg_host'),
        'port':os.getenv('pg_port')
    }
    return db_creds

@task(name='Dimension Tables Create Query')
def task_2():
    dim_aircrafts = """
    CREATE TABLE IF NOT EXISTS dim_facts.dim_aircrafts (
        aircraft_icao VARCHAR,
        aircraft_iata VARCHAR,
        model_name VARCHAR,
        manufacturer VARCHAR
    )
    """
    
    dim_airports = """
    CREATE TABLE IF NOT EXISTS dim_facts.dim_airports (
        airport_icao VARCHAR,
        airport_iata VARCHAR,
        airport_name VARCHAR, 
        latitude FLOAT,
        longitude FLOAT,    
        country_iso VARCHAR,
        country VARCHAR            
    )
    """
    
    dim_line_types = """
    CREATE TABLE IF NOT EXISTS dim_facts.dim_line_types (
        line_type_code VARCHAR,
        description VARCHAR
    )
    """
    
    dim_flight_status = """
    CREATE TABLE IF NOT EXISTS dim_facts.dim_flight_status (
        status_id INT,
        description VARCHAR
    )
    """
    
    dim_flight_situation  = """
    CREATE TABLE IF NOT EXISTS dim_facts.dim_flight_situation (
        situation_id VARCHAR,
        situation_short_desc VARCHAR,
        situation_desc VARCHAR
    )
    """
    
    sql_query = {
        'dim_aircrafts': dim_aircrafts,
        'dim_airports': dim_airports,
        'dim_line_types': dim_line_types,
        'dim_flight_status': dim_flight_status,
        'dim_flight_situation': dim_flight_situation
    }
    return sql_query


@task(name='Create dimension tables')
def task_3(db_creds, sql_query):
    conn = psycopg2.connect(**db_creds)
    cur = conn.cursor()
    
    try:
        for table_name, query in sql_query.items():
            cur.execute(query)
            print(f"Table {table_name} created or already exists.")
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


@task(name='Copy CSV data dictionary to tables')
def task_4(db_creds):

    conn = psycopg2.connect(**db_creds)
    cur = conn.cursor()

    file_paths = {
        'dim_aircrafts':r"C:\Users\USER\Desktop\anac_reg_flights\data_dictionary\vra_azu_aircrafts.csv",
        'dim_airports':r"C:\Users\USER\Desktop\anac_reg_flights\data_dictionary\vra_azu_airports_final.csv",
        'dim_line_types':r"C:\Users\USER\Desktop\anac_reg_flights\data_dictionary\line_type_codes.csv",
        'dim_flight_status':r"C:\Users\USER\Desktop\anac_reg_flights\data_dictionary\flight_status.csv",
        'dim_flight_situation':r"C:\Users\USER\Desktop\anac_reg_flights\data_dictionary\flight_situation_codes.csv"
    }
    try:
        for table_name,file_path in file_paths.items():
            file_name = file_path.split('\\')[-1]
            with open(file_path, "r", encoding="utf-8") as f:
                cur.copy_expert(f"COPY dim_facts.{table_name} FROM STDIN WITH CSV HEADER", f)
                print(f"{file_name} loaded into {table_name}")
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

@flow(name='Load Dimension Tables')
def main():
    try:
        db_creds = task_1()
        sql_query = task_2()
        task_3(db_creds,sql_query)
        task_4(db_creds)
    except Exception as e:
        print(f"Error:{e}")

if __name__ == '__main__':
    main()
