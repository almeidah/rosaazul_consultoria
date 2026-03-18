import os
import asyncio
import aiohttp
import pandas as pd
import logging
import io 
from datetime import datetime
from google.cloud import storage 

# -------------------------------
# 0️⃣ Logging
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------------------
# 1️⃣ Carregar e Validar variáveis de ambiente
# -------------------------------
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

if not all([BASE_URL, USER, PASS]):
    logger.error("🚫 Erro de Configuração: Variáveis de ambiente MAGAZORD_BASE_URL, USER ou PASS não foram definidas.")
    raise EnvironmentError("Variáveis de ambiente Magazord ausentes. Configure o Cloud Run Job.")

# -------------------------------
# 2️⃣ Configurações
# -------------------------------
LIMIT = 100
REQUEST_TIMEOUT = 30
MAX_CONCURRENT_REQUESTS = 5
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "magazord-bd")
GCS_FOLDER_NAME = os.getenv("GCS_FOLDER_NAME", "rosaazul")
GCS_FILE_NAME = "product_derivation.csv" 

# -------------------------------
# 3️⃣ Função para buscar página de produtos
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
                                   timeout=REQUEST_TIMEOUT,
                                   ssl=False) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    data = result.get("data", {})
                    items = data.get("items", [])
                    has_more = data.get("has_more", False)
                    logger.info(f"✔ [Produtos Tratados] Página {page} | {len(items)} itens")
                    return items, has_more
                else:
                    logger.error(f"🚫 Erro {resp.status} na página {page}")
                    return [], False
    except Exception as e:
        logger.error(f"❌ Erro ao buscar página {page}: {e}")
        return [], False

# -------------------------------
# 4️⃣ Buscar todos os produtos
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
# 5️⃣ Função para upload no GCS
# -------------------------------
def upload_data_to_gcs(data_string: str, bucket_name: str, blob_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    try:
        logger.info(f"🔄 Iniciando upload para gs://{bucket_name}/{blob_name}")
        blob.upload_from_string(data_string, content_type='text/csv; charset=utf-8-sig')
        logger.info(f"✅ Upload bem-sucedido!")
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao fazer upload para GCS: {e}")
        raise e 

# -------------------------------
# 6️⃣ Função Principal do Módulo
# -------------------------------
async def executar_job():
    logger.info("🚀 Iniciando extração e tratamento de Produtos (Derivações)...")
    items = await fetch_all_produtos()

    if items:
        df = pd.DataFrame(items)

        # Remove colunas indesejadas
        cols_to_drop = [c for c in ['acompanha', 'categorias'] if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
        
        # Trata Derivações
        if 'derivacoes' in df.columns:
            logger.info("🛠️ Normalizando derivações...")
            # Explode a lista de derivações
            df = df.explode('derivacoes', ignore_index=True)
            # Normaliza o dicionário da coluna derivações
            derivacoes_normalized = pd.json_normalize(df['derivacoes']).add_suffix('_derivacao')
            # Combina com o original removendo a coluna bruta
            df = pd.concat([df.drop(columns=['derivacoes']), derivacoes_normalized], axis=1)

        # Adiciona a data de extração
        df["dataExtracao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"✅ {len(df)} linhas processadas. Gerando CSV em memória.")

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, sep=";", encoding="utf-8-sig")
        csv_string = csv_buffer.getvalue()

        blob_path = f"{GCS_FOLDER_NAME}/{GCS_FILE_NAME}"
        upload_data_to_gcs(csv_string, GCS_BUCKET_NAME, blob_path)
    else:
        logger.warning("⚠ Nenhum produto encontrado.")

if __name__ == "__main__":
    asyncio.run(executar_job())