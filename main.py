### Junção dos arquivos e transformação de dados
# Imports
import pandas as pd
import numpy as np
import os
import re
import pyodbc

# Variáveis com caminhos da pasta dos dados
kabum_data = "dados/kabum/"
pichau_data = "dados/pichau/"
terabyte_data = "dados/terabyte/"
historico_data = "dados/historico/dados_filtrados.json"

# Conexão com SQLServer
def conectar_sqlserver(server, database, username, password): 
    try: 
        driver_name = ''
        driver_names = [x for x in pyodbc.drivers() if x.endswith(' for SQL Server')]
        if driver_names:
            driver_name = driver_names[0]

        # Definindo a string de conexão 
        connection_string = ( 
            f'DRIVER={driver_name};' 
            f'SERVER={server};' 
            f'DATABASE={database};' 
            f'UID={username};' 
            f'PWD={password};' 
            f'Encrypt=no'
        ) 
        # Estabelecendo a conexão 
        conn = pyodbc.connect(connection_string) 
        # Retornando a conexão 
        return conn 
    except pyodbc.Error as e: 
        print(f"Erro ao conectar ao SQL Server: {e}") 
        return None

# Funções
# Para cada arquivo dentro da pasta, realiza a leitura e concatena no dataframe 
def extract_json(path, store):
    df = pd.DataFrame()

    for file_name in os.listdir(path):
        if file_name != f'{store}_products.json':
            file_path = os.path.join(path, file_name)
            
            temp_df = pd.read_json(file_path)
            date = file_name[-15:-5]
            
            temp_df['date'] = date
            
            df = pd.concat([df, temp_df])
    
    return df

def calcular_preco_parcelado(parcela): 
    match = re.match(r'(\d+)x de R\$ ([\d,]+)', parcela) 

    if match: 
        n_parcelas = int(match.group(1)) 
        valor = float(match.group(2).replace(',', '.')) 
        return n_parcelas * valor
    
    return None

def converter_valor(valor):
    if isinstance(valor, str):
        # Substitui o formato R$ 1.200,30 para 1200.30
        valor_formatado = re.sub(r'R\$\s?([\d\.]+),([\d]{2})', lambda m: m.group(1).replace('.', '') + '.' + m.group(2), valor)
        return float(valor_formatado)
    return None

# Cria a conexão com o SQLServer
server = 'thyagoquintas.com.br' 
database = 'PCGamer' 
username = 'PCGamer' 
password = 'PCGamer'

conn = conectar_sqlserver(server, database, username, password) 

if conn: 
    print("Conexão estabelecida com sucesso!") 
else: 
    print("Falha na conexão.")

# KaBum é diferente pois o tipo do arquivo é csv
# Criação de dataframe "inicial"
kabum = pd.DataFrame()

for file_name in os.listdir(kabum_data):
    if file_name.endswith('.csv'):
        file_path = os.path.join(kabum_data, file_name)
        
        temp_df = pd.read_csv(file_path, header=0)
        date = file_name[-14:-4]
        
        temp_df['date'] = date
        
        kabum = pd.concat([kabum, temp_df])

# Retira as linhas que vem da concatenação com o nome da coluna
kabum = kabum[kabum['code'] != 'code']
kabum['max_installment'] = kabum['max_installment'].astype(str)
kabum['preco_parcelado'] = kabum['max_installment'].apply(calcular_preco_parcelado)
kabum['origin'] = "kabum"

# Pichau
# Realiza a extração
pichau = extract_json(pichau_data, 'pichau')

# Seleciona apenas as colunas importantes
pichau = pichau[['sku', 'name', 'pichau_prices', 'date']]

# Normalização das colunas json
pichau_prices = pd.json_normalize(pichau['pichau_prices'])
pichau_prices = pichau_prices[['avista', 'base_price', 'final_price']]

# Junção com o dataframe original
pichau = pichau.drop(columns=['pichau_prices']).join(pichau_prices)
pichau['origin'] = "pichau"

# Terabyte
# Realiza a extração
terabyte = extract_json(terabyte_data, "terabyte")

# Retira a coluna de URL
terabyte = terabyte.drop(columns=['url', 'componentes'])
terabyte = terabyte[terabyte['preco_total'] != 0]
terabyte['preco_total'] = terabyte['preco_total'].apply(converter_valor)
terabyte['preco_avista'] = terabyte['preco_avista'].apply(converter_valor)
terabyte['preco_parcelado'] = terabyte['preco_parcelado'].apply(converter_valor)
terabyte['origin'] = "terabyte"

# Histórico
historico = pd.read_json(historico_data)
historico = pd.json_normalize(historico['existing_data'])
historico = historico.rename(columns={'site': 'origin'})

conn.close()