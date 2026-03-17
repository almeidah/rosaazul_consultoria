import pandas as pd
from ast import literal_eval
import os
from datetime import datetime

# -----------------------------------------------------------
# CONFIGURAÇÃO DE CAMINHOS E DATAS
# -----------------------------------------------------------

# Caminho completo do seu arquivo de origem
file_path_input = '/Users/henriquealmeida/Library/CloudStorage/GoogleDrive-henriquesilveiradealmeida@gmail.com/Meu Drive/Consultoria/Pink Dream/pinkdream-code/data/processed/products.csv'

# Define o nome do arquivo de saída (salva na mesma pasta do arquivo original)
output_dir = os.path.dirname(file_path_input)
file_path_output = os.path.join(output_dir, 'products_derivacoes_flat.csv')

# Delimitador usado no arquivo
DELIMITER = ';'

# Captura a data e hora atual no formato desejado
data_hora_relatorio = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# -----------------------------------------------------------
# 1. CARREGAR E PREPARAR DADOS
# -----------------------------------------------------------

try:
    df = pd.read_csv(file_path_input, sep=DELIMITER)
    print(f"Arquivo de origem carregado: {file_path_input}")
    print(f"Total de linhas originais: {len(df)}")
except FileNotFoundError:
    print(f"Erro: O arquivo não foi encontrado no caminho: {file_path_input}")
    exit()

# Trata valores nulos/vazios preenchendo com uma lista vazia
df['derivacoes'] = df['derivacoes'].fillna('[]')

# Converte a string para Listas de Dicionários (objetos Python)
df['derivacoes'] = df['derivacoes'].apply(literal_eval)

# Opcional: Remove linhas sem derivações e reseta o índice
df = df[df['derivacoes'].apply(len) > 0].reset_index(drop=True) 

# -----------------------------------------------------------
# 2. EXPLODIR E NORMALIZAR OS DADOS
# -----------------------------------------------------------

# A. Explode: Cria uma nova linha para cada objeto na lista 'derivacoes'
df_exploded = df.explode('derivacoes').reset_index(drop=True)

# B. Normaliza: Converte os dicionários da coluna 'derivacoes' em novas colunas
derivacoes_df = pd.json_normalize(df_exploded['derivacoes'])

# C. Renomeia as novas colunas
derivacoes_df = derivacoes_df.rename(columns={
    'id': 'id_derivacao',
    'codigo': 'codigo_derivacao',
    'nome': 'nome_derivacao',
    'ativo': 'ativo_derivacao'
})

# D. Combina os DataFrames
df_final = pd.concat([
    df_exploded.drop(columns=['derivacoes']),
    derivacoes_df
], axis=1)


# -----------------------------------------------------------
# 3. ADICIONAR COLUNA DE DATA DE GERAÇÃO
# -----------------------------------------------------------

# Adiciona a nova coluna com a data/hora de geração do relatório em todas as linhas
df_final['dataGeracaoRelatorio'] = data_hora_relatorio


# -----------------------------------------------------------
# 4. SALVAR O CSV FINAL
# -----------------------------------------------------------

# Salva o DataFrame final no novo arquivo CSV
df_final.to_csv(file_path_output, sep=DELIMITER, index=False)

# -----------------------------------------------------------
# RESULTADO E CONFIRMAÇÃO
# -----------------------------------------------------------

print("\n--- Processamento e Salvamento Concluídos ---")
print(f"Data de Geração Inserida: {data_hora_relatorio}")
print(f"DataFrame Final (Explodido): **{len(df_final)} linhas**")
print(f"Arquivo salvo com sucesso em: **{file_path_output}**")

print("\nPrimeiras 5 linhas do resultado (incluindo a nova coluna):")
# Mostra as colunas principais e a nova coluna de data ao final
print(df_final[['id', 'nome', 'id_derivacao', 'nome_derivacao', 'dataGeracaoRelatorio']].head())