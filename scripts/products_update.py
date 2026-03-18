import os
import asyncio
import aiohttp
import pandas as pd
import logging
import random
from dotenv import load_dotenv
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
MAX_CONCURRENT = 5
semaphore = asyncio.Semaphore(MAX_CONCURRENT)

# 🕒 Configuração de Incremental
DAYS_AGO_UPDATE = 1 # Busca alterações das últimas 24h

# Configurações do Google Cloud Storage
GCS_BUCKET_NAME = "magazord-bd"
GCS_FOLDER_NAME = "rosaazul"
PASTA_DESTINO = r"/Users/henriquealmeida/Library/CloudStorage/GoogleDrive-henriquesilveiradealmeida@gmail.com/Meu Drive/Consultoria/Rosa azul/rosaazul-code/rosaazul_consultoria/data/processed"

# -------------------------------
# 3️⃣ Função para buscar Mapeamentos (Lookups)
# -------------------------------
async def get_brand_map(session):
    brand_map = {}
    page = 1
    while True:
        try:
            async with session.get(f"{BASE_URL}/v2/site/marca", auth=aiohttp.BasicAuth(USER, PASS), params={"limit": 100, "page": page}, ssl=False) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    items = data.get("data", {}).get("items", [])
                    for item in items:
                        brand_map[item["id"]] = item["nome"]
                    if not data.get("data", {}).get("has_more"): break
                    page += 1
                else: break
        except Exception: break
    logger.info(f"✔ Mapeadas {len(brand_map)} marcas")
    return brand_map

async def get_category_map(session):
    category_map = {}
    page = 1
    while True:
        try:
            async with session.get(f"{BASE_URL}/v2/site/categoria", auth=aiohttp.BasicAuth(USER, PASS), params={"limit": 100, "page": page}, ssl=False) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    items = data.get("data", {}).get("items", [])
                    for item in items:
                        category_map[item["id"]] = item["nome"]
                    if not data.get("data", {}).get("has_more"): break
                    page += 1
                else: break
        except Exception: break
    logger.info(f"✔ Mapeadas {len(category_map)} categorias")
    return category_map

# -------------------------------
# 4️⃣ Função para buscar Detalhe do Produto
# -------------------------------
async def fetch_produto_detail(session, codigo):
    url = f"{BASE_URL}/v2/site/produto/{codigo}"
    for attempt in range(3):
        async with semaphore:
            try:
                async with session.get(url, auth=aiohttp.BasicAuth(USER, PASS), timeout=REQUEST_TIMEOUT, ssl=False) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("data", {})
                    elif resp.status == 429:
                        await asyncio.sleep(random.uniform(1, 3) * (attempt + 1))
                    else:
                        return None
            except Exception:
                await asyncio.sleep(1)
    return None

# -------------------------------
# 5️⃣ Função para buscar IDs de Produtos Atualizados (Incremental)
# -------------------------------
async def fetch_updated_product_codes(session, date_filter):
    url = f"{BASE_URL}/v2/site/produto"
    page = 1
    all_codes = []
    logger.info(f"🔍 Filtrando produtos atualizados desde: {date_filter}")
    
    while True:
        params = {
            "limit": LIMIT, 
            "page": page, 
            "order": "id", 
            "orderDirection": "desc",
            "dataAtualizacao[gte]": date_filter
        }
        try:
            async with session.get(url, auth=aiohttp.BasicAuth(USER, PASS), params=params, ssl=False) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    items = result.get("data", {}).get("items", [])
                    for it in items:
                        all_codes.append(it["codigo"])
                    if not result.get("data", {}).get("has_more") or not items: break
                    page += 1
                    logger.info(f"✔ Lista Incremental: Página {page-1} processada ({len(all_codes)} acumulado)")
                else: 
                    logger.error(f"❌ Erro na lista incremental: {resp.status}")
                    break
        except Exception as e: 
            logger.error(f"❌ Exceção na lista incremental: {e}")
            break
    return all_codes

# -------------------------------
# 6️⃣ Função para upload no GCS
# -------------------------------
def upload_file_to_gcs(file_path: str, bucket_name: str, folder_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    file_name = os.path.basename(file_path)
    blob_name = f"{folder_name}/{file_name}"
    blob = bucket.blob(blob_name)
    try:
        logger.info(f"🔄 Iniciando upload para gs://{bucket_name}/{blob_name}")
        blob.upload_from_filename(file_path)
        logger.info(f"✅ Upload bem-sucedido!")
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao fazer upload: {e}")
        return False

# -------------------------------
# 7️⃣ Main
# -------------------------------
async def main():
    async with aiohttp.ClientSession() as session:
        # Define a data de corte (Ontem para garantir segurança nas 4 rodadas diárias)
        date_filter = (datetime.now() - timedelta(days=DAYS_AGO_UPDATE)).strftime("%Y-%m-%d %H:%M:%S")
        
        logger.info(f"🔄 Iniciando atualização incremental de produtos (>= {date_filter})")
        
        # 1. Buscar Mapas (Apenas uma vez)
        brand_map = await get_brand_map(session)
        category_map = await get_category_map(session)
        
        # 2. Listar códigos alterados
        codigos = await fetch_updated_product_codes(session, date_filter)
        
        if not codigos:
            logger.info("✅ Nenhum produto alterado no período. Finalizando.")
            return

        logger.info(f"📑 Total de {len(codigos)} produtos alterados para buscar detalhes.")
        
        # 3. Buscar detalhes em paralelo
        tarefas = [fetch_produto_detail(session, cod) for cod in codigos]
        resultados = await asyncio.gather(*tarefas)
        
        produtos_completos = []
        for p in resultados:
            if not p: continue
            
            # Mapear Marca
            p["marcaNome"] = brand_map.get(p.get("marca"), "N/A")
            
            # Mapear Categorias
            cat_list = p.get("categorias", [])
            p["categoriaNomes"] = "; ".join([category_map.get(c, str(c)) for c in cat_list])
            
            # Data de Extração
            p["dataExtracao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            produtos_completos.append(p)
            
        if produtos_completos:
            df = pd.DataFrame(produtos_completos)
            
            # Garantir pasta local
            os.makedirs(PASTA_DESTINO, exist_ok=True)
            filename_local = "products_update.csv"
            full_local_path = os.path.join(PASTA_DESTINO, filename_local)
            
            df.to_csv(full_local_path, index=False, sep=";", encoding="utf-8-sig")
            logger.info(f"📂 {len(df)} produtos atualizados salvos em {full_local_path}")
            
            upload_file_to_gcs(full_local_path, GCS_BUCKET_NAME, GCS_FOLDER_NAME)
        else:
            logger.warning("⚠ Nenhum dado processado após filtro.")

if __name__ == "__main__":
    asyncio.run(main())
