from prefect import task, flow
import os
from dotenv import load_dotenv
import psycopg2
import csv
import shutil
from dbt.cli.main import dbtRunner, dbtRunnerResult



"""
Pre-requisites:
Run notebooks/pre_processing/download_files.ipynb and combine_convert_files.ipynb in order to download files before running this pipeline.
There are multiple files to be downloaded and might take some time to download depending on the internet speed.
"""

@task(name='Load credentials')
def load_creds():
    load_dotenv()
    db_creds = {
        "dbname":os.getenv('pg_database'),
        "user":os.getenv('pg_user'),
        "password":os.getenv('pg_pass'),
        "host":os.getenv('pg_host'),
        "port":os.getenv('pg_port')
    }
    return db_creds

@task(name='Get CSV file from local bucket or directory')
def get_csv_filepath():
    file_links = os.getenv('vra_2017_2023_paths')
    vra_2017_2023_unprocessed = os.getenv('vra_2017_2023_unprocessed')
    vra_2017_2023_processed = os.getenv('vra_2017_2023_processed')
    try:
        with open(file_links, "r") as file:
            for line in file:
                csv_file_name = line.strip().split('\\')[-1]
                csv_processed_path = os.path.join(vra_2017_2023_processed,csv_file_name)
                csv_unprocessed_path = os.path.join(vra_2017_2023_unprocessed,csv_file_name)
                if not os.path.exists(csv_processed_path):
                    print(csv_unprocessed_path)
                    return csv_unprocessed_path
        return None
    
    except Exception as e:
        print(f"Error: {e}")
        return None


@task(name='Check CSV column names and order')
def test_csv_cols(csv_unprocessed_path):
    if csv_unprocessed_path is None:
        raise ValueError("All CSV files processed.")

    expected_csv_cols = ['Sigla ICAO Empresa Aérea', 'Empresa Aérea', 'Número Voo', 'Código DI', 'Código Tipo Linha', 'Modelo Equipamento', 'Número de Assentos', 'Sigla ICAO Aeroporto Origem', 'Descrição Aeroporto Origem', 'Partida Prevista', 'Partida Real', 'Sigla ICAO Aeroporto Destino', 'Descrição Aeroporto Destino', 'Chegada Prevista', 'Chegada Real', 'Situação Voo', 'Justificativa', 'Referência', 'Situação Partida', 'Situação Chegada']

    with open(csv_unprocessed_path, "r", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter=',')
        actual_csv_cols = next(reader)

        if expected_csv_cols == actual_csv_cols:
            print("CSV Columns passed")
            return csv_unprocessed_path
        else:
            print("CSV Column mismatch! File will be skipped.")
            return None



@task(name='Load data to staging_vra.vra_raw_landing')
def load_data(db_creds,csv_unprocessed_path):
    if csv_unprocessed_path is None:
        print("No valid CSV file to load.")
        return
    conn = psycopg2.connect(**db_creds)
    cur = conn.cursor()
    schema = os.getenv('pg_schema_staging')
    table = os.getenv('pg_table_raw')
    schema_table = f"{schema}.{table}"
    
    with open(csv_unprocessed_path, "r", encoding="utf-8") as file:
        cur.copy_expert(f"COPY {schema_table} FROM STDIN WITH CSV HEADER DELIMITER ','", file)
    
    try:
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        return e

@task(name='Run DBT models')
def run_dbt():
    dbt_profiles = r"C:\Users\USER\.dbt"
    dbt_proj_dir = r"C:\Users\USER\Desktop\anac_reg_flights\dbt_vra_flights"

    dbt = dbtRunner()
    staging_vra = ["run", "-m", "tag:staging_vra", "--project-dir", dbt_proj_dir, "--profiles-dir", dbt_profiles]
    clean_records = ["run", "-m", "tag:clean_records", "--project-dir", dbt_proj_dir, "--profiles-dir", dbt_profiles]

    try:
        res1: dbtRunnerResult = dbt.invoke(staging_vra)
        res2: dbtRunnerResult = dbt.invoke(clean_records)
    except Exception as e:
        print(f"Error running DBT models: {e}")
        raise

    for res in [res1, res2]:
        if res is not None and hasattr(res, "result"):
            for r in res.result:
                print(f"{r.node.name}: {r.status}")
        else:
            print(f"DBT command did not return results: {res}")


@task(name='Transfer data file to processed directory')
def transfer_file(csv_unprocessed_path):
    vra_2017_2023_processed = os.getenv('vra_2017_2023_processed')
    if csv_unprocessed_path is None:
        pass
    else:
        shutil.move(csv_unprocessed_path, vra_2017_2023_processed)

@flow(name='AZU Flights ELT Pipeline')
def main():
    db_creds = load_creds()
    csv_unprocessed_path = get_csv_filepath()
    csv_unprocessed_path = test_csv_cols(csv_unprocessed_path)
    load_data(db_creds,csv_unprocessed_path)
    run_dbt()
    transfer_file(csv_unprocessed_path)

if __name__=='__main__':
    main()