import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage
import random
import os
import logging

logger = logging.getLogger(__name__)

# -------------------------------
# Configurações Magazord
# -------------------------------
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

# -------------------------------
# Configurações GCP
# -------------------------------
GCS_BUCKET_NAME = "magazord-bd"
GCS_FOLDER_NAME = "rosaazul/orders_extras.csv"  # NOVO ARQUIVO SEPARADO

# Configuração de Filtros
LIMIT = 100
MAX_CONCURRENT = 3
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=60)
SITUACOES_APROVADO = [4, 5, 6, 7, 8]
DATE_FILTER = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d') # Últimos 5 dias

# -------------------------------
# 1️⃣ Busca Lista de Pedidos (Paginado)
# -------------------------------
async def fetch_pedidos_page(session, page, semaphore):
    url = f"{BASE_URL}/v2/site/pedido"
    params = {
        "dataHora[gte]": DATE_FILTER,
        #"situacao": ",".join(map(str, SITUACOES_APROVADO)), # Trazendo todos os status para não descasar
        "limit": LIMIT,
        "page": page
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
                    logger.info(f"✔ [Orders Extras] Página {page} | {len(items)} itens")
                    return items, total_pages
                else:
                    logger.error(f"🚫 Erro {resp.status} na página {page} de orders_extras")
                    return [], 0
    except Exception as e:
        logger.error(f"❌ Erro ao buscar página {page} de orders_extras: {e}")
        return [], 0

# -------------------------------
# 2️⃣ Busca Todos os Pedidos 
# -------------------------------
async def fetch_all_pedidos():
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        all_items = []
        
        # Primeira página
        primeiros_itens, total_pages = await fetch_pedidos_page(session, 1, semaphore)
        if not primeiros_itens:
            return []
            
        all_items.extend(primeiros_itens)
        
        # Demais páginas
        tarefas = []
        for page in range(2, total_pages + 1):
            tarefas.append(fetch_pedidos_page(session, page, semaphore))

        if tarefas:
            resultados = await asyncio.gather(*tarefas)
            for items, _ in resultados:
                if items:
                    all_items.extend(items)
                    
        return all_items

# -------------------------------
# 3️⃣ Buscar Detalhes dos Pedidos
# -------------------------------
async def buscar_detalhe_pedido(session, codigo_pedido, semaphore, attempt=0):
    url = f"{BASE_URL}/v2/site/pedido/{codigo_pedido}"
    
    async with semaphore:
        try:
            async with session.get(url, auth=aiohttp.BasicAuth(USER, PASS), timeout=REQUEST_TIMEOUT, ssl=False) as resp:
                if resp.status == 200:
                    detalhes = await resp.json()
                    pedido_data = detalhes.get("data", {})
                    
                    if pedido_data:
                        rastreios = pedido_data.get("arrayPedidoRastreio", [])
                        rastreio_1 = rastreios[0] if rastreios else {}

                        return {
                            "codigo": codigo_pedido,
                            "valorFreteTransportadora": rastreio_1.get("valorFreteTransportadora")
                            # 🔮 FUTURO: Se precisar de mais colunas, coloque aqui! 
                            # Ex: "novaColuna": pedido_data.get("novaColuna")
                        }
                elif resp.status == 429:
                    delay = random.uniform(1.0, 2.0) * (attempt + 1)
                    await asyncio.sleep(delay)
                else:
                    logger.warning(f"⚠️ Erro ao buscar extra do pedido {codigo_pedido}: {resp.status}")
        except Exception as e:
            logger.error(f"⚠️ Exception extra no pedido {codigo_pedido}: {e}")
    return None 

# -------------------------------
# 4️⃣ Processar Detalhes em Paralelo
# -------------------------------
async def processar_detalhes_pedidos(pedidos):
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        tarefas = [buscar_detalhe_pedido(session, p["codigo"], semaphore) for p in pedidos]
        
        resultados = await asyncio.gather(*tarefas)
        # Filtra os vazios
        return [r for r in resultados if r]

# -------------------------------
# 5️⃣ Upload GCS
# -------------------------------
def upload_data_to_gcs(data_string: str, bucket_name: str, blob_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    try:
        blob.upload_from_string(data_string, content_type='text/csv; charset=utf-8-sig')
        logger.info(f"✅ Upload orders_extras para gs://{bucket_name}/{blob_name}")
        return True
    except Exception as e:
        logger.error(f"❌ Erro upload GCS orders_extras: {e}")
        raise e 

# -------------------------------
# 6️⃣ Função Principal
# -------------------------------
async def executar_job():
    logger.info(f"🚀 Iniciando extração ORDERS_EXTRAS (>= {DATE_FILTER})...")
    pedidos_lista = await fetch_all_pedidos()
    
    if not pedidos_lista:
        logger.warning("Nenhum pedido extra encontrado no período.")
        return 
        
    logger.info(f"📥 Baixando detalhes de {len(pedidos_lista)} pedidos para as colunas extras...")
    detalhes_extras = await processar_detalhes_pedidos(pedidos_lista)
    
    if detalhes_extras:
        df = pd.DataFrame(detalhes_extras)
        csv_data = df.to_csv(index=False, encoding="utf-8-sig")
        upload_data_to_gcs(csv_data, GCS_BUCKET_NAME, GCS_FOLDER_NAME)
        logger.info(f"✅ Extrator de Orders Extras concluído com {len(df)} registros.")
    else:
        logger.warning("Nenhum detalhe extra processado.")

if __name__ == "__main__":
    asyncio.run(executar_job())
