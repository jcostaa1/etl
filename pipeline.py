import pandas as pd
from datetime import datetime

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

df.to_csv('csv_data.csv') #testing a csv file w/ transformed data

today = datetime.now()
message = f"Arquivo pronto. \nDia e horário: {today.strftime('%d/%m/%Y %H:%M:%S')}."
print (message)