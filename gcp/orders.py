import os
import asyncio
import aiohttp
import pandas as pd
import random
import logging
import io 
from datetime import datetime, timedelta
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
SITUACOES_APROVADO = [4, 5, 6, 7, 8]
LIMIT = 100
MAX_CONCURRENT_REQUESTS = 4
RETRY_ATTEMPTS = 5
BASE_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 30.0
REQUEST_TIMEOUT = 30

# Período (Padrão 1 dia para Jobs frequentes)
DIAS_ATRAS = int(os.getenv("DAYS_AGO", 30))
DATA_INICIO_STR = (datetime.today() - timedelta(days=DIAS_ATRAS)).strftime("%Y-%m-%d")
DATA_FIM_STR    = datetime.today().strftime("%Y-%m-%d")

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "magazord-bd")
GCS_FOLDER_NAME = os.getenv("GCS_FOLDER_NAME", "meujeans")
GCS_FILE_NAME = "orders.csv"

# -------------------------------
# 3️⃣ Semáforo
# -------------------------------
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# -------------------------------
# 4️⃣ Função para buscar uma página
# -------------------------------
async def buscar_pagina(session, page, data_inicio, data_fim):
    params = {
        "dataHora[gte]": data_inicio,
        "dataHora[lte]": data_fim,
        "situacao": ",".join(map(str, SITUACOES_APROVADO)),
        "limit": LIMIT,
        "page": page,
        "orderDirection": "asc"
    }

    tentativas = 0
    while tentativas < RETRY_ATTEMPTS:
        async with semaphore:
            try:
                async with session.get(f"{BASE_URL}/v2/site/pedido",
                                       auth=aiohttp.BasicAuth(USER, PASS),
                                       params=params,
                                       timeout=REQUEST_TIMEOUT,
                                       ssl=False) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        items = result.get("data", {}).get("items", [])
                        has_more = result.get("data", {}).get("has_more", False)
                        logger.info(f"✅ Página {page} carregada ({len(items)} pedidos)")
                        return items, has_more
                    elif resp.status == 429:
                        delay = min(MAX_RETRY_DELAY, BASE_RETRY_DELAY * (2 ** tentativas) + random.random())
                        logger.warning(f"⚠️ 429 na página {page}, esperando {delay:.1f}s")
                        await asyncio.sleep(delay)
                        tentativas += 1
                    else:
                        logger.error(f"❌ Erro {resp.status} na página {page}")
                        return [], False
            except Exception as e:
                logger.warning(f"⚠️ Erro na página {page} ({tentativas+1}/{RETRY_ATTEMPTS}): {e}")
                await asyncio.sleep(BASE_RETRY_DELAY)
                tentativas += 1
    return [], False

# -------------------------------
# 5️⃣ Função para buscar todos os pedidos
# -------------------------------
async def buscar_todos_pedidos(data_inicio, data_fim):
    todos_pedidos = []
    page = 1
    has_more = True

    async with aiohttp.ClientSession() as session:
        while has_more:
            items, has_more = await buscar_pagina(session, page, data_inicio, data_fim)
            if items:
                todos_pedidos.extend(items)
            else:
                break
            page += 1
    return todos_pedidos

# -------------------------------
# 6️⃣ Função para upload no GCS
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
# 7️⃣ Função Principal do Módulo
# -------------------------------
async def executar_job():
    logger.info(f"🚀 Buscando pedidos de {DATA_INICIO_STR} até {DATA_FIM_STR}...")
    todos_pedidos = await buscar_todos_pedidos(DATA_INICIO_STR, DATA_FIM_STR)

    if todos_pedidos:
        df = pd.DataFrame(todos_pedidos)
        logger.info(f"📂 {len(df)} pedidos processados. Gerando CSV.")

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding="utf-8-sig") 
        csv_string = csv_buffer.getvalue()

        blob_path = f"{GCS_FOLDER_NAME}/{GCS_FILE_NAME}"
        upload_data_to_gcs(csv_string, GCS_BUCKET_NAME, blob_path)
    else:
        logger.warning("⚠️ Nenhum pedido encontrado.")

if __name__ == "__main__":
    asyncio.run(executar_job())
