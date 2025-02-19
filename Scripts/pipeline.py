import pandas as pd
from datetime import datetime

#extracting data
def extracting(): 
    df = pd.read_csv('raw_data.csv', delimiter=';', quotechar='"')
    df.columns = ['nome', 'score', 'peso', 'data_nascimento', 'nome_da_mãe', 'data_nascimento_mãe', 'telefone', 'cpf', 'endereco', 'email', 'ocupacao', 'renda_mensal', 'status_cadastro', 'data_cadastro']
    return df

#cleansing document type
def cleansing(data, pattern=r'\D'): 
    return data.str.replace(pattern, '', regex=True).fillna('')
df = extracting()
df['telefone'] = cleansing(df['telefone'])
df['cpf'] = cleansing(df['cpf'], r'[^\d]')

#converting types
def converting(df, cols, types, errors='coerce'): 
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

#adjusting address
def formatting(df, column_name='endereco'): 
    adjust1 = df[column_name].str.split(r',|-', expand=True)
    adjust1.columns = ['rua', 'numero', 'bairro', 'estado']
    df = pd.concat([df, adjust1], axis=1)
    df = df.drop('endereco', axis=1)
    return df
df = formatting(df)

#testing a csv file w/ transformed data
df.to_csv('csv_data.csv') 

today = datetime.now() #unnecessary message
message = f"Arquivo pronto. \nDia e horário: {today.strftime('%d/%m/%Y %H:%M:%S')}."
print (message)