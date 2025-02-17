import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import json

def extracting(): #extracting data
    df = pd.read_csv('raw_data.csv', delimiter=';', quotechar='"')
    df.columns = ['nome', 'score', 'peso', 'data_nascimento', 'nome_da_mãe', 'data_nascimento_mãe', 'telefone', 'cpf', 'endereco', 'email', 'ocupacao', 'renda_mensal', 'status_cadastro', 'data_cadastro']
    return df

def cleansing(data, pattern=r'\D'): #cleansing document type
    return data.str.replace(pattern, '', regex=True).fillna('')

df = extracting()
df['telefone'] = cleansing(df['telefone'])
df['cpf'] = cleansing(df['cpf'], r'[^\d]')

def converting(df, cols, types, errors='coerce'): #converting types
    for col, datatype in types.items():
        if col in cols:
            if datatype == 'numeric':
                df[col] = pd.to_numeric(df[col], errors=errors)
            elif datatype == 'datetime':
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
            elif datatype == 'bool':
                df[col] = df[col].astype(bool)
        return df
    
df = extracting()
converted = df.columns
columns = {
    'score': 'numeric',
    'peso': 'bool',
    'data_nascimento': 'datetime',
    'data_nascimento_mae': 'datetime',
    'renda_mensal': 'bool',
    'data_cadastro': 'datetime'
    }
df = converting(df, converted, columns)

def formatting(df, column_name='endereco'): #adjusting address
    adjust1 = df[column_name].str.split(r',|-', expand=True)
    adjust1.columns = ['rua', 'numero', 'bairro', 'estado']
    df = pd.concat([df, adjust1], axis=1)
    df = df.drop('endereco', axis=1)
    return df

df = formatting(df)

#df.to_csv('csv_data.csv') #testing a csv file w/ transformed data

def loading(df, table_name='clientes', file='config.json'): #connecting and loading w/ database (review this!!!!!!!)
    with open(file, 'r') as f:
        db = json.load(f)
        
        user = db['db_user']
        password = db['db_password']
        host = db['db_host']
        port = db['db_port']
        database = db['db_database']
        engine_str = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        engine = create_engine(engine_str)

        df.to_sql(table_name, engine, if_exists='append', index=False)