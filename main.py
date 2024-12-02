### Junção dos arquivos e transformação de dados
# Imports
import pandas as pd
import numpy as np
import os
import re
import pyodbc
import time

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

def is_table_empty(conn, table_name): 
    try: 
        cursor = conn.cursor() 
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}") 
        row_count = cursor.fetchone()[0] 
        return row_count == 0 
    except pyodbc.Error as e: 
        print(f"Erro ao verificar a tabela: {e}") 
        return False

# Cria a conexão com o SQLServer
server = 'thyagoquintas.com.br' 
database = 'PCGamer' 
username = 'PCGamer' 
password = 'PCGamer'

connMSSQL = conectar_sqlserver(server, database, username, password) 

if connMSSQL: 
    print("Conexão estabelecida com sucesso!") 
else: 
    print("Falha na conexão.")

# Cursor para inserir dados na tabela
cursor = connMSSQL.cursor()

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
kabum['price_installment'] = kabum['max_installment'].apply(calcular_preco_parcelado)
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
pichau = pichau.rename(columns={'sku': 'code', 'avista': 'price', 'base_price': 'old_price', 'final_price': 'price_installment'})
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
terabyte = terabyte.rename(columns={'nome': 'name', 'preco_avista': 'price', 'preco_total': 'old_price', 'preco_parcelado': 'price_installment'})
terabyte['origin'] = "terabyte"

# Histórico
historico = pd.read_json(historico_data)
historico = pd.json_normalize(historico['existing_data'])
historico = historico.rename(columns={'site': 'origin'})
historico = historico[historico['date'] <= '2024-10-30']

# Seleção para enviar para as tabelas computer e hardware
kabum_unique = kabum[['code', 'name', 'origin']].set_index('code')
kabum_unique = kabum_unique.drop_duplicates()

pichau_unique = pichau[['code', 'name', 'origin']].set_index('code')
pichau_unique = pichau_unique.drop_duplicates()

terabyte_unique = terabyte[['name', 'origin']]
terabyte_unique = terabyte_unique.drop_duplicates()
terabyte_unique['code'] = range(1, len(terabyte_unique) + 1)
terabyte_unique = terabyte_unique.set_index('code')

historico_unique = historico[['code', 'name', 'origin']].set_index('code')
historico_unique = historico_unique.drop_duplicates()

# Separação de PC e Hardware
kabum_pc = kabum_unique[kabum_unique['name'].str.contains('PC', case=False, na=False)]
kabum_hardware = kabum_unique[~kabum_unique['name'].str.contains('PC', case=False, na=False)]

pichau_pc = pichau_unique[pichau_unique['name'].str.contains('PC', case=False, na=False) | pichau_unique['name'].str.contains('Computador', case=False, na=False)]
pichau_hardware = pichau_unique[~pichau_unique['name'].str.contains('PC', case=False, na=False) & ~pichau_unique['name'].str.contains('Computador', case=False, na=False)]

terabyte_pc = terabyte_unique[terabyte_unique['name'].str.contains('PC', case=False, na=False)]
terabyte_hardware = terabyte_unique[~terabyte_unique['name'].str.contains('PC', case=False, na=False)]

computer = pd.concat([kabum_pc, pichau_pc, terabyte_pc], axis=0)
hardware = pd.concat([kabum_hardware, pichau_hardware, terabyte_hardware, historico_unique], axis=0)

# Categoria dos Hardwares
hardware['category'] = hardware['name'].apply(lambda x: 'Processador' if 'PROCESSADOR' in x.upper() else 'Placa de Vídeo' if 'PLACA DE VÍDEO' in x.upper() or 'PLACA DE VIDEO' in x.upper() else 'SSD' if 'SSD' in x else 'Fonte' if 'FONTE' in x.upper() else 'Memória RAM')
hardware = hardware.reset_index().groupby(['code', 'origin']).first().reset_index()
hardware = hardware.drop_duplicates()

pc_table = "computer"
hardware_table = "hardware"
price_table = "price"
raw_price_table = "raw_price"
pc_x_hardware = "hardware_pc"

kabum_filtered = kabum[['code', 'name', 'origin', 'price', 'old_price', 'price_installment', 'date']]
pichau_filtered = pichau[['code', 'name', 'origin', 'price', 'old_price', 'price_installment', 'date']]
terabyte_filtered = terabyte[['name', 'origin', 'price', 'old_price', 'price_installment', 'date']]
historico_filtered = historico[['code', 'name', 'origin', 'price', 'price_installment', 'date']]

terabyte_reset = terabyte_unique.reset_index()
terabyte_filtered = terabyte_filtered.merge(
    terabyte_reset[['name', 'origin', 'code']],  
    on=['name', 'origin'],  
    how='left'  
)

raw_price = pd.concat([kabum_filtered, pichau_filtered, terabyte_filtered, historico_filtered], axis=0)
raw_price.fillna(0, inplace=True)
raw_price = raw_price.drop_duplicates()

cursor.execute(f"TRUNCATE TABLE dbo.{pc_table}")
connMSSQL.commit()

cursor.execute(f"TRUNCATE TABLE dbo.{hardware_table}")
connMSSQL.commit()

cursor.execute(f"TRUNCATE TABLE dbo.{price_table}")
connMSSQL.commit()

cursor.execute(f"TRUNCATE TABLE dbo.{raw_price_table}")
connMSSQL.commit()

cursor.execute(f"TRUNCATE TABLE dbo.{pc_x_hardware}")
connMSSQL.commit()

time.sleep(5)

print("Iniciando primeira inserção")

computer = computer.reset_index()
for index, row in computer.iterrows():
    cursor.execute(
        f"INSERT INTO {pc_table} (original_code, origin, name) VALUES (?, ?, ?)", 
        row['code'], row['origin'], row['name']
    )
connMSSQL.commit()  # Confirmar as alterações

hardware = hardware.reset_index()
for index, row in hardware.iterrows():
    cursor.execute(
        f"INSERT INTO {hardware_table} (original_code, origin, name, category) VALUES (?, ?, ?, ?)", 
        row['code'], row['origin'], row['name'], row['category']
    )
connMSSQL.commit()  # Confirmar as alterações

print("Inserções das tabelas únicas realizadas com sucesso!")

for index, row in raw_price.iterrows():
    cursor.execute(
        f"INSERT INTO {raw_price_table} (original_code, name, origin, price, old_price, price_installment, date_price) VALUES (?, ?, ?, ?, ?, ?, ?)", 
        row['code'], row['name'], row['origin'], row['price'], row['old_price'], row['price_installment'], row['date']
    )
connMSSQL.commit() 

cursor.execute("EXEC pr_insertprices")
connMSSQL.commit() 

connMSSQL.close()