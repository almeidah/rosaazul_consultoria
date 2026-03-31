import os
import asyncio
import aiohttp
import pandas as pd
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
LIMIT = 100
REQUEST_TIMEOUT = 30
MAX_CONCURRENT_REQUESTS = 3
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# 🕒 Configuração de Incremental
DAYS_AGO_UPDATE = int(os.getenv("DAYS_AGO_UPDATE", 1))
DATE_FILTER = (datetime.now() - timedelta(days=DAYS_AGO_UPDATE)).strftime("%Y-%m-%dT%H:%M:%SZ")

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "magazord-bd")
GCS_FOLDER_NAME = os.getenv("GCS_FOLDER_NAME", "rosaazul")
GCS_FILE_NAME = "products.csv" 

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
                    total_pages = data.get("total_pages", 1)
                    logger.info(f"✔ [Produtos Tratados] Página {page} | {len(items)} itens")
                    return items, total_pages
                else:
                    logger.error(f"🚫 Erro {resp.status} na página {page}")
                    return [], 0
    except Exception as e:
        logger.error(f"❌ Erro ao buscar página {page}: {e}")
        return [], 0

# -------------------------------
# 4️⃣ Buscar todos os produtos (EM PARALELO)
# -------------------------------
async def fetch_all_produtos():
    async with aiohttp.ClientSession() as session:
        all_items = []
        
        # 1. Primeira chamada instanciada
        logger.info("Iniciando requisição da página 1 para descobrir o total_pages de Produtos...")
        primeiros_itens, total_pages = await fetch_produto_page(session, 1)
        if not primeiros_itens:
            return []
            
        all_items.extend(primeiros_itens)
        logger.info(f"📊 Total de páginas reportadas pela API: {total_pages}")

        # 2. Chama o restante simultaneamente
        tarefas = []
        for page in range(2, total_pages + 1):
            tarefas.append(fetch_produto_page(session, page))

        if tarefas:
            logger.info(f"🚀 Disparando {len(tarefas)} chamadas de produtos em PARALELO...")
            resultados = await asyncio.gather(*tarefas)
            for items, _ in resultados:
                if items:
                    all_items.extend(items)
                    
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