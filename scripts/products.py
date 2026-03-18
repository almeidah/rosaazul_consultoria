import os
import asyncio
import aiohttp
import pandas as pd
import logging
from dotenv import load_dotenv
from datetime import datetime
from google.cloud import storage # Importar google.cloud.storage

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
BASE_URL = os.getenv("MAGAZORD_BASE_URL")   # ex: https://urlmagazord.com.br/api
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
GCS_BUCKET_NAME = "magazord-bd" # Seu bucket no GCS
GCS_FOLDER_NAME = "rosaazul" # Sua pasta dentro do bucket (prefíxo de objeto)

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
# 5️⃣ Função para upload no GCS (mantida dos scripts anteriores)
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
# 6️⃣ Main
# -------------------------------
async def main():
    logger.info("🔄 Buscando produtos...")
    items = await fetch_all_produtos()

    if items:
        df = pd.DataFrame(items)

        # --- Adiciona a nova coluna com a data de extração ---
        extraction_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["dataExtracao"] = extraction_date
        # ----------------------------------------------------

        # Define o nome do arquivo fixo para sobrescrever
        filename_local = "products.csv" 

        output_dir = r"/Users/henriquealmeida/Library/CloudStorage/GoogleDrive-henriquesilveiradealmeida@gmail.com/Meu Drive/Consultoria/Rosa azul/rosaazul-code/rosaazul_consultoria/data/processed"
        os.makedirs(output_dir, exist_ok=True)
        full_local_path = os.path.join(output_dir, filename_local)
        
        df.to_csv(full_local_path, index=False, sep=";", encoding="utf-8-sig")
        logger.info(f"✅ Produtos salvos localmente em {full_local_path} ({len(df)} linhas)")

        # --- Chamada para upload no GCS ---
        upload_file_to_gcs(full_local_path, GCS_BUCKET_NAME, GCS_FOLDER_NAME)
        # --------------------------------

    else:
        logger.warning("⚠ Nenhum produto encontrado.")

if __name__ == "__main__":
    asyncio.run(main())