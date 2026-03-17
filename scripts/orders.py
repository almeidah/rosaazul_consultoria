import os
import asyncio
import aiohttp
import pandas as pd
import random
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.cloud import storage # Importar google.cloud.storage

# -------------------------------
# 0️⃣ Logging
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()  # Apenas console, sem arquivo
    ]
)
logger = logging.getLogger(__name__)

# -------------------------------
# 1️⃣ Variáveis de ambiente
# -------------------------------
load_dotenv()
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

# -------------------------------
# 2️⃣ Configurações
# -------------------------------
SITUACOES_APROVADO = [4, 5, 6, 7, 8]  # 👉 múltiplos status
LIMIT = 100
MAX_CONCURRENT_REQUESTS = 4
RETRY_ATTEMPTS = 5
BASE_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 30.0
REQUEST_TIMEOUT = 30

DATA_INICIO_STR = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")  # 30 dias atrás
DATA_FIM_STR    = datetime.today().strftime("%Y-%m-%d")  # Hoje

OUTPUT_DIR = r"/Users/henriquealmeida/Library/CloudStorage/GoogleDrive-henriquesilveiradealmeida@gmail.com/Meu Drive/Consultoria/Pink Dream/pinkdream-code/data/processed"
# Não é mais necessário criar a pasta aqui, pois faremos isso dentro do main antes de salvar o arquivo localmente.
# os.makedirs(OUTPUT_DIR, exist_ok=True) 

# Configurações do Google Cloud Storage
GCS_BUCKET_NAME = "pinkdata" # Seu bucket no GCS
GCS_FOLDER_NAME = "magazord" # Sua pasta dentro do bucket (prefíxo de objeto)

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
        "situacao": ",".join(map(str, SITUACOES_APROVADO)),  # 👉 envia separado por vírgulas
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
                                       timeout=REQUEST_TIMEOUT) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        items = result["data"]["items"]
                        has_more = result["data"].get("has_more", False)
                        logger.info(f"✅ Página {page} carregada ({len(items)} pedidos)")
                        return items, has_more
                    elif resp.status == 429:
                        delay = min(MAX_RETRY_DELAY, BASE_RETRY_DELAY * (2 ** tentativas) + random.random())
                        logger.warning(f"⚠️ 429 na página {page}, esperando {delay:.1f}s antes de tentar novamente ({tentativas+1}/{RETRY_ATTEMPTS})")
                        await asyncio.sleep(delay)
                        tentativas += 1
                    else:
                        logger.error(f"❌ Erro {resp.status} na página {page}: {await resp.text()}")
                        return [], False
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ Timeout na página {page} ({tentativas+1}/{RETRY_ATTEMPTS})")
                await asyncio.sleep(BASE_RETRY_DELAY)
                tentativas += 1
            except aiohttp.ClientError as e:
                logger.error(f"❌ Erro de conexão na página {page}: {e}")
                await asyncio.sleep(BASE_RETRY_DELAY)
                tentativas += 1
    logger.error(f"❌ Falha ao buscar página {page} após {RETRY_ATTEMPTS} tentativas")
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
            todos_pedidos.extend(items)
            # Se não houver mais itens mas o has_more ainda for True (erro na API),
            # ou se a lista de itens estiver vazia, sair do loop.
            if not items and has_more: 
                logger.warning(f"⚠ Não foram retornados itens na página {page}, mas a API indicou 'has_more'. Parando para evitar loop infinito.")
                break
            if not has_more: # Se a API indicar que não há mais páginas, sair.
                break
            page += 1

    return todos_pedidos

# -------------------------------
# 6️⃣ Função para upload no GCS (mantida dos scripts anteriores)
# -------------------------------
def upload_file_to_gcs(file_path: str, bucket_name: str, folder_name: str):
    """
    Faz o upload de um arquivo para um bucket do Google Cloud Storage.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Pega apenas o nome do arquivo do caminho completo
    file_name = os.path.basename(file_path)
    # Define o blob name como 'pasta/nome_do_arquivo'
    blob_name = f"{folder_name}/{file_name}"
    blob = bucket.blob(blob_name)

    try:
        logger.info(f"🔄 Iniciando upload de '{file_path}' para gs://{bucket_name}/{blob_name}")
        blob.upload_from_filename(file_path)
        logger.info(f"✅ Upload bem-sucedido para gs://{bucket_name}/{blob_name}")
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao fazer upload para GCS: {e}")
        return False


# -------------------------------
# 7️⃣ Main
# -------------------------------
async def main():
    logger.info(f"🔄 Buscando pedidos aprovados ({SITUACOES_APROVADO}) de {DATA_INICIO_STR} até {DATA_FIM_STR}...")
    todos_pedidos = await buscar_todos_pedidos(DATA_INICIO_STR, DATA_FIM_STR)

    if todos_pedidos:
        df = pd.DataFrame(todos_pedidos)
        
        # Define o nome do arquivo fixo para sobrescrever
        filename_local = "orders.csv"
        
        # Garante que a pasta de destino local exista
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        full_local_path = os.path.join(OUTPUT_DIR, filename_local)
        
        df.to_csv(full_local_path, index=False, encoding="utf-8-sig")
        logger.info(f"📂 {len(df)} pedidos salvos localmente em {full_local_path}")

        # --- Chamada para upload no GCS ---
        upload_file_to_gcs(full_local_path, GCS_BUCKET_NAME, GCS_FOLDER_NAME)
        # --------------------------------
    else:
        logger.warning("⚠️ Nenhum pedido encontrado com os status definidos para o período.")

# -------------------------------
# 8️⃣ Executar
# -------------------------------
if __name__ == "__main__":
    asyncio.run(main())