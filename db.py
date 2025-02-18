import pandas as pd
import psycopg2
import json

with open('config.json', 'r') as f:
    config = json.load(f)

host = config['db_host']
dbname = config['db_name']
user = config['db_user']
password = config['db_password']
port = config['db_port']
table = config ['db_table']
csv_path = config ['file']

def loading():
    try:    
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port
        )
        print("Success.")
    except Exception as e:
        print("Failed. \nError:", str(e))
    return conn

conn = loading()
cursor = conn.cursor()

df = pd.read_csv(csv_path)
with open(csv_path, 'r') as f:
    next(f) 
    cursor.copy_expert(f"COPY {table} FROM STDIN WITH CSV DELIMITER ','", f)

    conn.commit()
    print(f"Dados inseridos com sucesso na '{table}'.")
    
    cursor.close()
    conn.close()