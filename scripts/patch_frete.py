import asyncio
import aiohttp
import pandas as pd
import os
import glob
from datetime import datetime, timedelta
from google.cloud import storage
from dotenv import load_dotenv
import logging

load_dotenv()

# Configuração de Log
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configurações Magazord
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

# Configurações GCP
GCS_BUCKET_NAME = "magazord-bd"
GCS_FOLDER_NAME = "rosaazul/patch_frete_transportadora.csv"
PASTA_DESTINO = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed")

# Configuração de Filtros
SITUACOES_APROVADO = [4, 5, 6, 7, 8]
LIMIT = 100
MAX_CONCURRENT = 5 # Um pouco maior por ser uma extração leve

# Período
DIAS_ATRAS = 477
DATA_INICIO = (datetime.today() - timedelta(days=DIAS_ATRAS)).strftime("%Y-%m-%d")
DATA_FIM    = datetime.today().strftime("%Y-%m-%d")

# -------------------------------
# Upload GCS
# -------------------------------
def upload_file_to_gcs(local_file_path: str, bucket_name: str, blob_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    try:
        logger.info(f"🔄 Iniciando upload para gs://{bucket_name}/{blob_name}")
        blob.upload_from_filename(local_file_path)
        logger.info("✅ Upload bem-sucedido!")
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao fazer upload para GCS: {e}")
        return False

# -------------------------------
# Funções de Requisição
# -------------------------------
async def buscar_pedidos(session, data_inicio, data_fim):
    pedidos = []
    page = 1
    has_more = True
    while has_more:
        params = {
            "dataHora[gte]": data_inicio,
            "dataHora[lte]": data_fim,
            "situacao": ",".join(map(str, SITUACOES_APROVADO)),
            "limit": LIMIT,
            "page": page
        }
        try:
            async with session.get(f"{BASE_URL}/v2/site/pedido", 
                                   auth=aiohttp.BasicAuth(USER, PASS), 
                                   params=params, ssl=False) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    items = data.get("data", {}).get("items", [])
                    if items:
                        pedidos.extend(items)
                        logger.info(f"✅ Página {page} carregada ({len(items)} pedidos)")
                        page += 1
                    else:
                        has_more = False
                else:
                    logger.error(f"❌ Erro HTTP {resp.status} na página {page}")
                    has_more = False
        except Exception as e:
            logger.error(f"❌ Exceção na página {page}: {e}")
            has_more = False
    return pedidos

async def buscar_detalhe(session, codigo_pedido):
    try:
        async with session.get(f"{BASE_URL}/v2/site/pedido/{codigo_pedido}", 
                               auth=aiohttp.BasicAuth(USER, PASS), ssl=False) as resp:
            if resp.status == 200:
                detalhes = await resp.json()
                pedido_data = detalhes.get("data", {})
                
                if pedido_data:
                    rastreios = pedido_data.get("arrayPedidoRastreio", [])
                    rastreio_1 = rastreios[0] if rastreios else {}
                    
                    return {
                        "codigo": codigo_pedido,
                        "valorFreteTransportadora": rastreio_1.get("valorFreteTransportadora")
                    }
    except Exception as e:
        logger.error(f"⚠️ Erro no pedido {codigo_pedido}: {e}")
    return None

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
# Funções de Arquivos
# -------------------------------
def salvar_arquivo_mensal(dados, mes, pasta_destino, prefixo):
    if not dados:
        return
    df = pd.DataFrame(dados)
    os.makedirs(pasta_destino, exist_ok=True)
    arquivo = os.path.join(pasta_destino, f"{prefixo}_{mes}.csv")
    df.to_csv(arquivo, index=False, encoding="utf-8-sig")
    logger.info(f"📂 Arquivo mensal salvo: {arquivo} com {len(df)} registros.")

def juntar_arquivos_e_subir(pasta_destino, prefixo, arquivo_final, bucket_name, folder_name):
    padrao = os.path.join(pasta_destino, f"{prefixo}_*.csv")
    arquivos = glob.glob(padrao)
    
    if not arquivos:
        logger.warning("⚠ Nenhum arquivo mensal encontrado para juntar.")
        return
        
    logger.info(f"🔄 Juntando {len(arquivos)} arquivos mensais...")
    dfs = []
    for arq in sorted(arquivos):
        try:
            dfs.append(pd.read_csv(arq))
        except Exception as e:
            logger.error(f"❌ Erro ao ler {arq}: {e}")
            
    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        caminho_final = os.path.join(pasta_destino, arquivo_final)
        df_final.to_csv(caminho_final, index=False, encoding="utf-8-sig")
        logger.info(f"✅ Arquivo final consolidado gerado: {caminho_final} com {len(df_final)} registros totais.")
        
        # Subir para GCS
        upload_file_to_gcs(caminho_final, bucket_name, folder_name)

# -------------------------------
# Main Loop
# -------------------------------
async def main():
    async with aiohttp.ClientSession() as session:
        inicio = datetime.strptime(DATA_INICIO, "%Y-%m-%d")
        fim = datetime.strptime(DATA_FIM, "%Y-%m-%d")
        delta = timedelta(days=1)

        dia_atual = inicio
        mes_atual = inicio.strftime("%Y-%m")
        pedidos_mes_atual = []

        while dia_atual <= fim:
            mes_dia_atual = dia_atual.strftime("%Y-%m")
            
            if mes_dia_atual != mes_atual:
                if pedidos_mes_atual:
                    salvar_arquivo_mensal(pedidos_mes_atual, mes_atual, PASTA_DESTINO, "patch_frete_temp")
                mes_atual = mes_dia_atual
                pedidos_mes_atual = []

            data_str = dia_atual.strftime("%Y-%m-%d")
            logger.info(f"\n🔹 Processando dia {data_str}")
            detalhes_dia = await processar_dia(session, data_str, data_str)
            pedidos_mes_atual.extend(detalhes_dia)
            dia_atual += delta

        if pedidos_mes_atual:
            salvar_arquivo_mensal(pedidos_mes_atual, mes_atual, PASTA_DESTINO, "patch_frete_temp")

        juntar_arquivos_e_subir(PASTA_DESTINO, "patch_frete_temp", "patch_frete.csv", GCS_BUCKET_NAME, GCS_FOLDER_NAME)

if __name__ == "__main__":
    asyncio.run(main())
