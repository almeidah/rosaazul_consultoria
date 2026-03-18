import os
import asyncio
import aiohttp
import pandas as pd
import random
import logging
import io
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
# 1️⃣ Variáveis de ambiente
# -------------------------------
load_dotenv()
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

# -------------------------------
# 2️⃣ Configurações
# -------------------------------
SITUACOES_APROVADO = [4, 5, 6, 7, 8]
LIMIT = 100
MAX_CONCURRENT = 4 

# Período a ser consultado
DIAS_ATRAS = 5
DATA_INICIO = (datetime.today() - timedelta(days=DIAS_ATRAS)).strftime("%Y-%m-%d")
DATA_FIM    = datetime.today().strftime("%Y-%m-%d")

# Pasta destino local
PASTA_DESTINO = r"/Users/henriquealmeida/Library/CloudStorage/GoogleDrive-henriquesilveiradealmeida@gmail.com/Meu Drive/Consultoria/Rosa azul/rosaazul-code/rosaazul_consultoria/data/processed"

# Configurações do Google Cloud Storage
GCS_BUCKET_NAME = "magazord-bd"
GCS_FOLDER_NAME = "rosaazul"

# -------------------------------
# 3️⃣ Função para buscar a lista de pedidos de um dia
# -------------------------------
async def buscar_pedidos(session, data_inicio, data_fim):
    todos_pedidos = []
    page = 1
    while True:
        params = {
            "dataHora[gte]": data_inicio,
            "dataHora[lte]": data_fim,
            "situacao": ",".join(map(str, SITUACOES_APROVADO)),
            "limit": LIMIT,
            "page": page,
            "orderDirection": "asc"
        }
        try:
            async with session.get(f"{BASE_URL}/v2/site/pedido", 
                                   auth=aiohttp.BasicAuth(USER, PASS), 
                                   params=params, 
                                   ssl=False) as resp:
                if resp.status != 200:
                    logger.error(f"❌ Erro ao buscar pedidos na página {page} do dia {data_inicio}: {resp.status}")
                    break
                result = await resp.json()
                pedidos = result.get("data", {}).get("items", [])
                if not pedidos:
                    break
                todos_pedidos.extend(pedidos)
                logger.info(f"✅ Página {page} carregada ({len(pedidos)} pedidos)")
                page += 1
        except Exception as e:
            logger.error(f"❌ Exceção ao buscar pedidos na página {page}: {e}")
            break
    return todos_pedidos

# -------------------------------
# 4️⃣ Função para buscar detalhes de um pedido
# -------------------------------
async def buscar_detalhe(session, codigo_pedido):
    detalhe_url = f"{BASE_URL}/v2/site/pedido/{codigo_pedido}"
    
    for attempt in range(3): 
        try:
            async with session.get(detalhe_url, auth=aiohttp.BasicAuth(USER, PASS), ssl=False) as resp:
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
                        "enderecoComplemento": pedido_data.get("enderecoComplemento"),
                        "enderecoBairro": pedido_data.get("enderecoBairro"),
                        "enderecoCidade": pedido_data.get("enderecoCidade"),
                        "enderecoUf": pedido_data.get("enderecoUf"),
                        "enderecoCep": pedido_data.get("enderecoCep")
                    }
                elif resp.status == 429:
                    delay = random.uniform(1.0, 2.0) * (attempt + 1)
                    await asyncio.sleep(delay)
                else:
                    break 
        except Exception:
            await asyncio.sleep(1 * (attempt + 1)) 
    return None

# -------------------------------
# 5️⃣ Função para upload no GCS
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
        logger.error(f"❌ Erro ao fazer upload para GCS: {e}")
        return False

# -------------------------------
# 6️⃣ Função para processar pedidos de um dia
# -------------------------------
async def processar_dia(session, data_inicio, data_fim):
    pedidos = await buscar_pedidos(session, data_inicio, data_fim)
    logger.info(f"📌 Total de pedidos do dia {data_inicio}: {len(pedidos)}")

    tarefas = []
    todos_detalhes = []
    for p in pedidos:
        tarefas.append(buscar_detalhe(session, p["codigo"]))
        if len(tarefas) >= MAX_CONCURRENT:
            resultados = await asyncio.gather(*tarefas)
            tarefas = []
            todos_detalhes.extend([r for r in resultados if r])

    if tarefas:
        resultados = await asyncio.gather(*tarefas)
        todos_detalhes.extend([r for r in resultados if r])

    return todos_detalhes

# -------------------------------
# 7️⃣ Main Loop
# -------------------------------
async def main():
    async with aiohttp.ClientSession() as session:
        inicio = datetime.strptime(DATA_INICIO, "%Y-%m-%d")
        fim = datetime.strptime(DATA_FIM, "%Y-%m-%d")
        delta = timedelta(days=1)

        dia_atual = inicio
        todos_pedidos_periodo = []

        while dia_atual <= fim:
            data_str = dia_atual.strftime("%Y-%m-%d")
            logger.info(f"\n🔹 Processando pedidos do dia {data_str}")
            detalhes_dia = await processar_dia(session, data_str, data_str)
            todos_pedidos_periodo.extend(detalhes_dia)
            dia_atual += delta

        if todos_pedidos_periodo:
            df = pd.DataFrame(todos_pedidos_periodo)
            filename_local = "orders.csv"
            os.makedirs(PASTA_DESTINO, exist_ok=True)
            full_local_path = os.path.join(PASTA_DESTINO, filename_local)
            
            df.to_csv(full_local_path, index=False, encoding="utf-8-sig")
            logger.info(f"\n📂 Total de {len(df)} pedidos salvos em {full_local_path}")

            upload_file_to_gcs(full_local_path, GCS_BUCKET_NAME, GCS_FOLDER_NAME)
        else:
            logger.warning("⚠ Nenhum pedido encontrado.")

if __name__ == "__main__":
    asyncio.run(main())