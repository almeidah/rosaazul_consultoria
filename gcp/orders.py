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
MAX_CONCURRENT_REQUESTS = 2
RETRY_ATTEMPTS = 5
BASE_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 30.0
REQUEST_TIMEOUT = 30

# Período (Padrão 1 dia para Jobs frequentes)
DIAS_ATRAS = int(os.getenv("DAYS_AGO", 30))
DATA_INICIO_STR = (datetime.today() - timedelta(days=DIAS_ATRAS)).strftime("%Y-%m-%d")
DATA_FIM_STR    = datetime.today().strftime("%Y-%m-%d")

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "magazord-bd")
GCS_FOLDER_NAME = os.getenv("GCS_FOLDER_NAME", "rosaazul")
GCS_FILE_NAME = "orders.csv"

# -------------------------------
# 3️⃣ Semáforo
# -------------------------------
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# -------------------------------
# 4️⃣ Função para buscar o detalhe completo de um pedido
# -------------------------------
async def buscar_detalhe_pedido(session, codigo_pedido):
    detalhe_url = f"{BASE_URL}/v2/site/pedido/{codigo_pedido}"
    
    for attempt in range(RETRY_ATTEMPTS):
        async with semaphore:
            try:
                async with session.get(detalhe_url, 
                                       auth=aiohttp.BasicAuth(USER, PASS), 
                                       timeout=REQUEST_TIMEOUT,
                                       ssl=False) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        pedido_data = data.get("data", {})
                        
                        return {
                            "codigo": codigo_pedido,
                            "dataHora": pedido_data.get("dataHora"),
                            "situacao": pedido_data.get("situacao"),
                            "situacaoNome": pedido_data.get("situacaoNome"),
                            "dataHoraSituacao": pedido_data.get("dataHoraSituacao"),
                            "vendedor": pedido_data.get("vendedor"),
                            "apelidoVendedor": pedido_data.get("apelidoVendedor"),
                            "canalVenda": pedido_data.get("canalVenda"),
                            "formaPagamento": pedido_data.get("formaPagamento"),
                            "pessoaId": pedido_data.get("pessoaId"),
                            "pessoaNome": pedido_data.get("pessoaNome"),
                            "pessoaCpfCnpj": pedido_data.get("pessoaCpfCnpj"),
                            "pessoaEmail": pedido_data.get("pessoaEmail"),
                            "pessoaTelefone": pedido_data.get("pessoaTelefone"),
                            "valorTotal": pedido_data.get("valorTotal"),
                            "valorFrete": pedido_data.get("valorFrete"),
                            "valorDesconto": pedido_data.get("valorDesconto"),
                            "valorJuros": pedido_data.get("valorJuros"),
                            "valorOutrasDespesas": pedido_data.get("valorOutrasDespesas"),
                            "nfeNumero": pedido_data.get("nfeNumero"),
                            "nfeSerie": pedido_data.get("nfeSerie"),
                            "nfeChave": pedido_data.get("nfeChave"),
                            "transportadora": pedido_data.get("transportadora"),
                            "transportadoraNome": pedido_data.get("transportadoraNome"),
                            "servicoEnvio": pedido_data.get("servicoEnvio"),
                            "codigoRastreamento": pedido_data.get("codigoRastreamento"),
                            "dataPrevisaoEntrega": pedido_data.get("dataPrevisaoEntrega"),
                            "dataEntrega": pedido_data.get("dataEntrega"),
                            "enderecoLogradouro": pedido_data.get("enderecoLogradouro"),
                            "enderecoNumero": pedido_data.get("enderecoNumero"),
                            "enderecoBairro": pedido_data.get("enderecoBairro"),
                            "enderecoCidade": pedido_data.get("enderecoCidade"),
                            "enderecoUf": pedido_data.get("enderecoUf"),
                            "enderecoCep": pedido_data.get("enderecoCep")
                        }
                    elif resp.status == 429:
                        delay = min(MAX_RETRY_DELAY, BASE_RETRY_DELAY * (2 ** attempt) + random.random())
                        await asyncio.sleep(delay)
                    else:
                        return None
            except Exception:
                await asyncio.sleep(BASE_RETRY_DELAY)
    return None

# -------------------------------
# 5️⃣ Função para buscar uma página da lista
# -------------------------------
async def buscar_pagina_lista(session, page, data_inicio, data_fim):
    params = {
        "dataHoraUltimaAlteracaoSituacao[gte]": data_inicio,
        "dataHoraUltimaAlteracaoSituacao[lte]": data_fim,
        "situacao": ",".join(map(str, SITUACOES_APROVADO)),
        "limit": LIMIT,
        "page": page,
        "orderDirection": "asc"
    }

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
                    return items, has_more
                return [], False
        except Exception:
            return [], False

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
    async with aiohttp.ClientSession() as session:
        logger.info(f"🚀 Buscando todos os pedidos de {DATA_INICIO_STR} até {DATA_FIM_STR}...")
        
        page = 1
        has_more = True
        todos_codigos = []

        while has_more:
            items, has_more = await buscar_pagina_lista(session, page, DATA_INICIO_STR, DATA_FIM_STR)
            for item in items:
                todos_codigos.append(item["codigo"])
            logger.info(f"✅ Lista: Página {page} processada. Acumulado: {len(todos_codigos)} pedidos.")
            page += 1
            if not items: break

        logger.info(f"📑 Buscando detalhes completos de {len(todos_codigos)} pedidos...")
        tarefas = [buscar_detalhe_pedido(session, cod) for cod in todos_codigos]
        resultados = await asyncio.gather(*tarefas)
        
        pedidos_completos = [r for r in resultados if r is not None]
        
        if pedidos_completos:
            df = pd.DataFrame(pedidos_completos)
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
