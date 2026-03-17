import os
import asyncio
import aiohttp
import pandas as pd
import logging
from dotenv import load_dotenv
from datetime import datetime
from google.cloud import storage 
from ast import literal_eval 

# -------------------------------
# 0️⃣ Logging
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------------------
# 1️⃣ Carregar variáveis de ambiente
# -------------------------------
load_dotenv()
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

# -------------------------------
# 2️⃣ Configurações
# -------------------------------
LIMIT = 100
REQUEST_TIMEOUT = 30
MAX_CONCURRENT_REQUESTS = 5
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# Configurações do Google Cloud Storage
GCS_BUCKET_NAME = "pinkdata"
GCS_FOLDER_NAME = "magazord"

# Delimitador usado no arquivo final
DELIMITER = ";"

# -------------------------------
# 3️⃣ Funções Auxiliares
# -------------------------------

def safe_literal_eval(value):
    """
    Aplica literal_eval de forma segura apenas em strings, 
    deixando listas ou outros objetos intactos, tratando valores nulos.
    """
    if isinstance(value, str):
        try:
            value = value.strip()
            # Trata strings vazias ou NaN
            if not value or value.lower() in ('nan', 'none', 'null'):
                return []
            return literal_eval(value)
        except (ValueError, SyntaxError, TypeError):
            # Retorna lista vazia em caso de string malformada
            logger.warning(f"Erro ao avaliar string malformada: {value[:50]}...")
            return []
    # Se já for um objeto Python (como lista ou dict vindo direto do JSON da API)
    return value if value is not None else []


# -------------------------------
# 4️⃣ Função para buscar página de produtos
# -------------------------------
async def fetch_produto_page(session, page: int):
    url = f"{BASE_URL}/v2/site/produto"
    params = {
        "limit": LIMIT,
        "page": page,
        "order": "id",
        "orderDirection": "asc"
    }

    try:
        async with semaphore:
            async with session.get(url,
                                   auth=aiohttp.BasicAuth(USER, PASS),
                                   params=params,
                                   timeout=REQUEST_TIMEOUT) as resp:
                text = await resp.text()
                if resp.status == 200:
                    result = await resp.json()
                    data = result.get("data", {})
                    items = data.get("items", [])
                    has_more = data.get("has_more", False)

                    logger.info(f"✔ Página {page} | {len(items)} itens")
                    return items, has_more
                else:
                    logger.error(f"🚫 Erro {resp.status} na página {page}: {text}")
                    return [], False
    except Exception as e:
        logger.error(f"❌ Erro ao buscar página {page}: {e}")
        return [], False

# -------------------------------
# 5️⃣ Buscar todos os produtos
# -------------------------------
async def fetch_all_produtos():
    async with aiohttp.ClientSession() as session:
        page = 1
        all_items = []

        while True:
            items, has_more = await fetch_produto_page(session, page)
            if not items:
                break
            all_items.extend(items)
            if not has_more:
                break
            page += 1
            await asyncio.sleep(0.1)

        return all_items

# -------------------------------
# 6️⃣ Função para processar e achatar as derivações (FLAT)
# -------------------------------
def process_and_explode_derivacoes(df: pd.DataFrame, data_hora_relatorio: str) -> pd.DataFrame:
    """
    Aplica a lógica de explode: converte a coluna 'derivacoes' para lista de dicts,
    explode em novas linhas, normaliza os campos e adiciona a data de geração.
    """
    logger.info("🔄 Iniciando processamento das derivações (Explode)...")
    
    # 1. TRATAMENTO E CONVERSÃO
    # Aplica a função de conversão segura para lidar com strings e objetos.
    df['derivacoes'] = df['derivacoes'].apply(safe_literal_eval)
    
    # 2. FILTRO
    # Filtra apenas produtos que possuem derivações válidas (listas com itens)
    df_with_derivacoes = df[df['derivacoes'].apply(lambda x: isinstance(x, list) and len(x) > 0)].copy()
    
    if df_with_derivacoes.empty:
        logger.warning("⚠ Nenhum produto com derivações válidas para processar. Retornando DataFrame vazio.")
        return pd.DataFrame()

    # 3. EXPLODE E NORMALIZAÇÃO
    df_exploded = df_with_derivacoes.explode('derivacoes').reset_index(drop=True)
    derivacoes_df = pd.json_normalize(df_exploded['derivacoes'])

    # 4. RENOMEIA COLUNAS
    derivacoes_df = derivacoes_df.rename(columns={
        'id': 'id_derivacao',
        'codigo': 'codigo_derivacao',
        'nome': 'nome_derivacao',
        'ativo': 'ativo_derivacao'
    })

    # 5. COMBINA E ADICIONA DATA DE GERAÇÃO
    df_final = pd.concat([
        df_exploded.drop(columns=['derivacoes']),
        derivacoes_df
    ], axis=1)

    # Adiciona a nova coluna com a data de geração
    df_final['dataGeracaoRelatorio'] = data_hora_relatorio

    logger.info(f"✅ Processamento de 'Explode' concluído. Total de linhas: {len(df_final)}")
    return df_final


# -------------------------------
# 7️⃣ Função para upload no GCS
# -------------------------------
def upload_file_to_gcs(file_path: str, bucket_name: str, folder_name: str):
    """
    Faz o upload de um arquivo processado para um bucket do Google Cloud Storage.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    file_name = os.path.basename(file_path)
    blob_name = f"{folder_name}/{file_name}"
    blob = bucket.blob(blob_name)

    try:
        logger.info(f"🔄 Iniciando upload do arquivo processado para gs://{bucket_name}/{blob_name}")
        blob.upload_from_filename(file_path)
        logger.info(f"✅ Upload bem-sucedido para gs://{bucket_name}/{blob_name}")
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao fazer upload para GCS: {e}")
        return False

# -------------------------------
# 8️⃣ Main (Fluxo Principal)
# -------------------------------
async def main():
    logger.info("🔄 Buscando produtos...")
    items = await fetch_all_produtos()
    data_hora_relatorio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if items:
        df_raw = pd.DataFrame(items)
        df_raw["dataExtracao"] = data_hora_relatorio 
        logger.info(f"Dados brutos da API em DataFrame. Linhas: {len(df_raw)}")

        # 1. PROCESSA E EXPLODE AS DERIVAÇÕES
        df_final = process_and_explode_derivacoes(df_raw, data_hora_relatorio)

        if df_final.empty:
             logger.warning("O DataFrame final está vazio. Encerrando o salvamento.")
             return

        # 2. CONFIGURA CAMINHOS DE SAÍDA
        filename_local = "products_derivacoes_flat.csv" 
        output_dir = r"/Users/henriquealmeida/Library/CloudStorage/GoogleDrive-henriquesilveiradealmeida@gmail.com/Meu Drive/Consultoria/Pink Dream/pinkdream-code/data/processed"
        os.makedirs(output_dir, exist_ok=True)
        full_local_path = os.path.join(output_dir, filename_local)
        
        # 3. SALVA O ARQUIVO CSV FINAL (PROCESSADO)
        df_final.to_csv(full_local_path, index=False, sep=DELIMITER, encoding="utf-8-sig")
        logger.info(f"✅ Arquivo processado salvo localmente em {full_local_path} ({len(df_final)} linhas)")

        # 4. UPLOAD PARA O GCS (DO ARQUIVO PROCESSADO)
        upload_file_to_gcs(full_local_path, GCS_BUCKET_NAME, GCS_FOLDER_NAME)

    else:
        logger.warning("⚠ Nenhum produto encontrado.")

if __name__ == "__main__":
    asyncio.run(main())