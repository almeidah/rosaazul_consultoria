import os
import asyncio
import aiohttp
import pandas as pd
import logging
from datetime import datetime
from google.cloud import storage 
import io 

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

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "magazord-bd")
GCS_FOLDER_NAME = os.getenv("GCS_FOLDER_NAME", "rosaazul")
GCS_FILE_NAME = "stock.csv" 

# -------------------------------
# 3️⃣ Função para buscar estoque
# -------------------------------
async def fetch_estoque_page(session, offset: int, sku: str = None, produtoId: str = None):
    url = f"{BASE_URL}/v1/listEstoque"
    params = {"limit": LIMIT, "offset": offset, "ativo": "true"}
    if sku:
        params["produto"] = sku
    if produtoId:
        params["produtoPai"] = produtoId

    try:
        async with semaphore:
            async with session.get(url,
                                   auth=aiohttp.BasicAuth(USER, PASS),
                                   params=params,
                                   timeout=REQUEST_TIMEOUT,
                                   ssl=False) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    items = result.get("data", [])
                    total = result.get("total", 0)
                    logger.info(f"✔ Offset {offset} | {len(items)} itens | Status 200")
                    return items, total
                else:
                    logger.error(f"🚫 Erro {resp.status} na página com offset {offset}")
                    return [], 0
    except Exception as e:
        logger.error(f"❌ Erro ao buscar página com offset {offset}: {e}")
        return [], 0

# -------------------------------
# 4️⃣ Buscar todo o estoque (EM PARALELO)
# -------------------------------
async def fetch_all_estoque(sku: str = None, produtoId: str = None):
    async with aiohttp.ClientSession() as session:
        all_items = []
        
        # 1. Faz a primeira chamada (offset 0) para pegar itens e descobrir o total
        logger.info("Iniciando requisição da página 1 (offset 0) para descobrir o total de SKUs...")
        primeiros_itens, total = await fetch_estoque_page(session, 0, sku, produtoId)
        if not primeiros_itens:
            return []
            
        all_items.extend(primeiros_itens)
        logger.info(f"📊 Total de SKUs acusados pela API: {total}")

        # 2. Prepara as requisições para o resto das páginas
        tarefas = []
        # Começa do próximo offset (LIMIT) em saltos de LIMIT, até o total
        for offset in range(LIMIT, total, LIMIT):
            tarefas.append(fetch_estoque_page(session, offset, sku, produtoId))

        if tarefas:
            logger.info(f"🚀 Disparando {len(tarefas)} chamadas em modo ASSÍNCRONO e PARALELO...")
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
    logger.info("🚀 Buscando estoque...")
    items = await fetch_all_estoque()

    if items:
        df = pd.DataFrame(items)
        df["dataExtracao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        existing_columns = df.columns.tolist()
        desired_order_start = [
            "produto", "produtoPai", "descricaoProduto", "deposito", "ativo", 
            "quantidadeFisica", "quantidadeDisponivelVenda", "quantidadeReservadoSaida",
            "quantidadePrevistaEntrada", "quantidadeVirtual", "quantidadeMinimaEstoque",
            "custoVirtual", "custoMedio", "dataExtracao"
        ]
        ordered_columns = [col for col in desired_order_start if col in existing_columns]
        for col in existing_columns:
            if col not in ordered_columns:
                ordered_columns.append(col)

        df = df[ordered_columns]
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, sep=",", decimal=".", encoding="utf-8")
        csv_string = csv_buffer.getvalue()

        logger.info(f"✅ {len(df)} itens processados. Gerando CSV.")

        blob_path = f"{GCS_FOLDER_NAME}/{GCS_FILE_NAME}"
        upload_data_to_gcs(csv_string, GCS_BUCKET_NAME, blob_path)
    else:
        logger.warning("⚠ Nenhum item encontrado.")

if __name__ == "__main__":
    asyncio.run(executar_job())
