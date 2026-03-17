import os
import requests # Mantém requests para requisições síncronas
import pandas as pd
import logging # Importa logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.cloud import storage # Importar google.cloud.storage

# -------------------------------
# 0️⃣ Logging
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
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
# 📅 Período a ser consultado
DATA_INICIO = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")  # 30 dias atrás
DATA_FIM    = datetime.today().strftime("%Y-%m-%d")  # Hoje

LIMIT = 100

# Pasta destino para salvar os arquivos localmente
OUTPUT_DIR = r"/Users/henriquealmeida/Library/CloudStorage/GoogleDrive-henriquesilveiradealmeida@gmail.com/Meu Drive/Consultoria/Pink Dream/pinkdream-code/data/processed"

# Configurações do Google Cloud Storage
GCS_BUCKET_NAME = "pinkdata" # Seu bucket no GCS
GCS_FOLDER_NAME = "magazord" # Sua pasta dentro do bucket (prefíxo de objeto)

# -------------------------------
# 3️⃣ Função para upload no GCS (mantida dos scripts anteriores)
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
# 4️⃣ Main (Síncrono)
# -------------------------------
def main():
    url = f"{BASE_URL}/v2/site/pedido"
    todos_pedidos = []
    page = 1

    logger.info(f"🔎 Buscando pedidos de {DATA_INICIO} até {DATA_FIM}...")

    while True:
        params = {
            "dataHora[gte]": DATA_INICIO,
            "dataHora[lte]": DATA_FIM,
            "limit": LIMIT,
            "page": page
        }

        try:
            response = requests.get(url, auth=(USER, PASS), params=params)

            if response.status_code == 200:
                result = response.json()["data"]
                pedidos = result["items"]

                if not pedidos:
                    logger.info(f"✅ Nenhuma pedido encontrado na página {page}. Finalizando busca.")
                    break

                todos_pedidos.extend(pedidos)
                logger.info(f"✅ Página {page} carregada ({len(pedidos)} pedidos)")

                if not result.get("has_more", False):
                    logger.info(f"✅ 'has_more' = False na página {page}. Finalizando busca.")
                    break

                page += 1
            else:
                logger.error(f"❌ Erro {response.status_code} na página {page}: {response.text}")
                break
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro de conexão ou requisição na página {page}: {e}")
            break
        except Exception as e:
            logger.error(f"❌ Erro inesperado na página {page}: {e}")
            break

    if todos_pedidos:
        df = pd.DataFrame(todos_pedidos)

        # --- Adiciona a nova coluna com a data de extração ---
        extraction_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["dataExtracao"] = extraction_date
        # ----------------------------------------------------

        # Define o nome do arquivo fixo para sobrescrever
        filename_local = "orders_by_situation.csv" # Nome diferente para este script

        # Garante que a pasta de destino local exista
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        full_local_path = os.path.join(OUTPUT_DIR, filename_local)
        
        df.to_csv(full_local_path, index=False, sep=";", encoding="utf-8-sig")
        logger.info(f"📂 {len(df)} pedidos salvos localmente em {full_local_path}")

        # Mostra as situações encontradas (como no original)
        situacoes = df[["pedidoSituacao", "pedidoSituacaoDescricao"]].drop_duplicates()
        logger.info("📋 Situações encontradas:")
        logger.info(f"\n{situacoes.to_string(index=False)}") # Formata para printar bonito no log
        
        # --- Chamada para upload no GCS ---
        upload_file_to_gcs(full_local_path, GCS_BUCKET_NAME, GCS_FOLDER_NAME)
        # --------------------------------

    else:
        logger.warning("⚠️ Nenhum pedido encontrado para o período.")

# -------------------------------
# 5️⃣ Executar
# -------------------------------
if __name__ == "__main__":
    main() # Este script é síncrono, não usa asyncio.run()