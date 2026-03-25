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
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

# -------------------------------
# 2️⃣ Configurações
# -------------------------------
LIMIT = 100
REQUEST_TIMEOUT = 30
MAX_CONCURRENT_REQUESTS = 2
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# Configurações do Google Cloud Storage
GCS_BUCKET_NAME = "pinkdata" # Seu bucket no GCS
GCS_FOLDER_NAME = "magazord" # Sua pasta dentro do bucket (prefíxo de objeto)

# -------------------------------
# 3️⃣ Função para buscar estoque
# -------------------------------
async def fetch_estoque_page(session, offset: int, sku: str = None, produtoId: str = None):
    url = f"{BASE_URL}/v1/listEstoque"
    params = {"limit": LIMIT, "offset": offset}
    if sku:
        params["produto"] = sku
    if produtoId:
        params["produtoPai"] = produtoId

    try:
        async with semaphore:
            async with session.get(url,
                                   auth=aiohttp.BasicAuth(USER, PASS),
                                   params=params,
                                   timeout=REQUEST_TIMEOUT) as resp:
                text = await resp.text()
                if resp.status == 200:
                    result = await resp.json()
                    items = result.get("data", [])
                    has_more = len(items) == LIMIT

                    logger.info(f"✔ Offset {offset} | {len(items)} itens")
                    return items, has_more
                else:
                    logger.error(f"🚫 Erro {resp.status} na página com offset {offset}: {text}")
                    return [], False
    except Exception as e:
        logger.error(f"❌ Erro ao buscar página com offset {offset}: {e}")
        return [], False

# -------------------------------
# 4️⃣ Buscar todo o estoque
# -------------------------------
async def fetch_all_estoque(sku: str = None, produtoId: str = None):
    async with aiohttp.ClientSession() as session:
        offset = 0
        all_items = []

        while True:
            items, has_more = await fetch_estoque_page(session, offset, sku, produtoId)
            if not items:
                break
            all_items.extend(items)
            if not has_more:
                break
            offset += LIMIT
            await asyncio.sleep(0.1)

        return all_items

# -------------------------------
# 5️⃣ Função para upload no GCS
# -------------------------------
def upload_file_to_gcs(file_path: str, bucket_name: str, folder_name: str):
    """
    Faz o upload de um arquivo para um bucket do Google Cloud Storage.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Pega apenas o nome do arquivo do caminho completo
    file_name = os.path.basename(file_path)
    # Define o blob name (equivalente à chave S3) como 'pasta/nome_do_arquivo'
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
    logger.info("🔄 Buscando estoque...")
    items = await fetch_all_estoque()

    if items:
        df = pd.DataFrame(items)

        # --- Adiciona a nova coluna com a data de extração ---
        extraction_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["dataExtracao"] = extraction_date
        # ----------------------------------------------------

        # Primeiro, identifique todas as colunas disponíveis no DataFrame
        existing_columns = df.columns.tolist()

        # Defina a ordem desejada para as colunas.
        # Adicione "dataExtracao" ao final da lista de ordem desejada.
        desired_order_start = [
            "produto",
            "produtoPai",            # Código do Produto
            "descricaoProduto",     # Descrição do Produto
            "deposito",             # ID do Depósito
            "ativo",                # Status de Ativo
            "quantidadeFisica",
            "quantidadeDisponivelVenda",
            "quantidadeReservadoSaida",
            "quantidadePrevistaEntrada",
            "quantidadeVirtual",
            "quantidadeMinimaEstoque",
            "custoVirtual",
            "custoMedio",
            "dataExtracao"          # Nova coluna no final
        ]

        # Crie a lista final de colunas, colocando as desejadas primeiro
        ordered_columns = [col for col in desired_order_start if col in existing_columns]
        for col in existing_columns:
            if col not in ordered_columns:
                ordered_columns.append(col) # Adiciona qualquer outra coluna que possa existir

        # Reordenar o DataFrame
        df = df[ordered_columns]

        # Gera o nome do arquivo com timestamp para garantir unicidade
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_local = f"stock.csv"

        output_dir = r"/Users/henriquealmeida/Library/CloudStorage/GoogleDrive-henriquesilveiradealmeida@gmail.com/Meu Drive/Consultoria/Pink Dream/pinkdream-code/data/processed"
        os.makedirs(output_dir, exist_ok=True)
        full_local_path = os.path.join(output_dir, filename_local)

        df.to_csv(full_local_path, index=False, sep=";", encoding="utf-8-sig")
        logger.info(f"✅ Estoque salvo localmente em {full_local_path} ({len(df)} linhas)")

        # --- Chamada para upload no GCS ---
        upload_file_to_gcs(full_local_path, GCS_BUCKET_NAME, GCS_FOLDER_NAME)
        # --------------------------------

    else:
        logger.warning("⚠ Nenhum item encontrado.")

if __name__ == "__main__":
    asyncio.run(main())