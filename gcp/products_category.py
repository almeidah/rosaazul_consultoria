import os
import asyncio
import aiohttp
import pandas as pd
import logging
import io 
import csv
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
LIMIT = 100
REQUEST_TIMEOUT = 30
MAX_CONCURRENT_REQUESTS = 3
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# 🕒 Configuração de Incremental
DAYS_AGO_UPDATE = int(os.getenv("DAYS_AGO_UPDATE", 1))
DATE_FILTER = (datetime.now() - timedelta(days=DAYS_AGO_UPDATE)).strftime("%Y-%m-%dT%H:%M:%SZ")

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "magazord-bd")
GCS_FOLDER_NAME = os.getenv("GCS_FOLDER_NAME", "rosaazul")
GCS_FILE_NAME = "products_category.csv" 

# -------------------------------
# 3️⃣ Função para buscar página de produtos
# -------------------------------
async def fetch_produto_page(session, page: int):
    url = f"{BASE_URL}/v2/site/produto"
    params = {
        "limit": LIMIT,
        "page": page,
        "order": "id",
        "orderDirection": "asc",
        "dataAtualizacaoInicio": DATE_FILTER
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
# 4️⃣ Buscar Categorias (Para construir a árvore)
# -------------------------------
async def fetch_all_categorias(session):
    url = f"{BASE_URL}/v2/site/categoria"
    page = 1
    cat_dict = {}
    while True:
        params = {"limit": 100, "page": page}
        try:
            async with session.get(url, auth=aiohttp.BasicAuth(USER, PASS), params=params, timeout=REQUEST_TIMEOUT, ssl=False) as resp:
                if resp.status == 200:
                    data = (await resp.json()).get("data", {})
                    items = data.get("items", [])
                    for item in items:
                        cat_dict[item["id"]] = {"nome": item["nome"], "pai": item.get("pai")}
                    if not data.get("has_more", False):
                        break
                    page += 1
                else:
                    break
        except Exception as e:
            logger.error(f"❌ Erro ao buscar categorias: {e}")
            break
    return cat_dict

def get_category_tree(cat_id, cat_dict):
    tree = []
    current = cat_id
    while current is not None and current in cat_dict:
        tree.insert(0, cat_dict[current]["nome"])
        current = cat_dict[current].get("pai")
    return tree

# -------------------------------
# 5️⃣ Buscar todos os produtos
# -------------------------------
async def fetch_all_produtos():
    async with aiohttp.ClientSession() as session:
        logger.info("🌲 Buscando árvore de categorias...")
        cat_dict = await fetch_all_categorias(session)
        logger.info(f"✔ {len(cat_dict)} categorias carregadas para enriquecimento.")

        page = 1
        all_items = []
        while True:
            items, has_more = await fetch_produto_page(session, page)
            if not items:
                break
            
            # Enriquecimento com a árvore de categorias
            for item in items:
                cat_list = item.get("categorias", [])
                if cat_list and len(cat_list) > 0:
                    tree = get_category_tree(cat_list[0], cat_dict)
                    item["categoria_nivel_1"] = tree[0] if len(tree) > 0 else None
                    item["categoria_nivel_2"] = tree[1] if len(tree) > 1 else None
                    item["categoria_nivel_3"] = tree[2] if len(tree) > 2 else None
                    item["categoria_nivel_4"] = tree[3] if len(tree) > 3 else None
                else:
                    item["categoria_nivel_1"] = None
                    item["categoria_nivel_2"] = None
                    item["categoria_nivel_3"] = None
                    item["categoria_nivel_4"] = None

            all_items.extend(items)
            
            if not has_more:
                break
            page += 1
            await asyncio.sleep(0.1)
        return all_items

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
    logger.info(f"🚀 Iniciando extração e tratamento de Produtos (Derivações) (>= {DATE_FILTER})...")
    items = await fetch_all_produtos()

    if items:
        df = pd.DataFrame(items)

        # Remove colunas indesejadas (mantemos categorias para análise de Curva ABC no BI)
        cols_to_drop = [c for c in ['acompanha'] if c in df.columns]
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

        # --- Tratamento de Segurança para o BigQuery ---
        # Remove quebras de linha e retornos de carro de todas as colunas de texto
        # para garantir que o BigQuery não se perca ao ler o CSV delimitado por vírgula.
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False).str.replace('\r', '', regex=False)
        # ----------------------------------------------------

        logger.info(f"✅ {len(df)} linhas processadas. Gerando CSV em memória com delimitador vírgula.")

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, sep=",", encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
        csv_string = csv_buffer.getvalue()

        blob_path = f"{GCS_FOLDER_NAME}/{GCS_FILE_NAME}"
        upload_data_to_gcs(csv_string, GCS_BUCKET_NAME, blob_path)
    else:
        logger.warning("⚠ Nenhum produto encontrado.")

if __name__ == "__main__":
    asyncio.run(executar_job())