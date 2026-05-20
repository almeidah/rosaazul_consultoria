import os
import asyncio
import aiohttp
import pandas as pd
import logging
import csv
from dotenv import load_dotenv
from datetime import datetime
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
MAX_CONCURRENT_REQUESTS = 2
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

GCS_BUCKET_NAME = "magazord-bd"
GCS_FOLDER_NAME = "rosaazul"

# -------------------------------
# 3️⃣ Função para buscar página de produtos
# -------------------------------
async def fetch_produto_page(session, page: int):
    url = f"{BASE_URL}/v2/site/produto"
    params = {
        "limit": LIMIT,
        "page": page,
        "order": "id",
        "orderDirection": "asc"
    }

    try:
        async with semaphore:
            async with session.get(url,
                                   auth=aiohttp.BasicAuth(USER, PASS),
                                   params=params,
                                   timeout=REQUEST_TIMEOUT) as resp:
                text = await resp.text()
                if resp.status == 200:
                    result = await resp.json()
                    data = result.get("data", {})
                    items = data.get("items", [])
                    has_more = data.get("has_more", False)

                    logger.info(f"✔ Página {page} | {len(items)} itens")
                    return items, has_more
                else:
                    logger.error(f"🚫 Erro {resp.status} na página {page}: {text}")
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
                    # Usamos a primeira categoria da lista do produto como referência
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
def upload_file_to_gcs(file_path: str, bucket_name: str, folder_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    file_name = os.path.basename(file_path)
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
# 7️⃣ Main
# -------------------------------
async def main():
    logger.info("🔄 Buscando produtos...")
    items = await fetch_all_produtos()

    if items:
        df = pd.DataFrame(items)

        # ----------------------------------------------------
        # 🗑️ REMOVER COLUNAS DESNECESSÁRIAS (mantemos categorias)
        # ----------------------------------------------------
        cols_to_drop = [c for c in ['acompanha'] if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            logger.info("🗑️ Coluna acompanha removida com sucesso.")
        # ----------------------------------------------------

        # ----------------------------------------------------
        # 🛠️ TRATAMENTO DA COLUNA DERIVAÇÕES
        # ----------------------------------------------------
        if 'derivacoes' in df.columns:
            logger.info("🛠️ Tratando coluna 'derivacoes' (Explode & Normalize)...")
            
            # 1. Explode: Cria uma linha para cada variação
            df = df.explode('derivacoes', ignore_index=True)

            # 2. Normalize: Transforma o dicionário em colunas (id, codigo, nome, ativo)
            derivacoes_normalized = pd.json_normalize(df['derivacoes'])

            # 3. Renomear com SUFIXO (ex: id -> id_derivacao)
            derivacoes_normalized = derivacoes_normalized.add_suffix('_derivacao')

            # 4. Juntar o dataframe original com as novas colunas
            df = pd.concat([df.drop(columns=['derivacoes']), derivacoes_normalized], axis=1)
        # ----------------------------------------------------

        # --- Adiciona a data de extração ---
        df["dataExtracao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # --- Garantir Esquema Fixo para o BigQuery ---
        EXPECTED_COLUMNS = [
            "id", "nome", "palavraChave", "peso", "altura", "largura", "comprimento",
            "codigo", "tipo", "marca", "unidadeMedida", "ativo", "ncm", "cest", "origemFiscal",
            "dataLancamento", "dataAtualizacao", "categorias", 
            "categoria_nivel_1", "categoria_nivel_2", "categoria_nivel_3", "categoria_nivel_4",
            "modelo", "id_derivacao", "codigo_derivacao", "nome_derivacao", "ativo_derivacao", "dataExtracao"
        ]

        # 1. Cria colunas ausentes com valor nulo
        for col in EXPECTED_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # 2. Força a ordem exata das colunas e descarta colunas extras surpresas
        df = df[EXPECTED_COLUMNS]

        # --- Tratamento de Segurança para o BigQuery ---
        # Remove quebras de linha e retornos de carro de todas as colunas de texto
        # para garantir que o BigQuery não se perca ao ler o CSV delimitado por vírgula.
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False).str.replace('\r', '', regex=False)
        # ----------------------------------------------------

        filename_local = "products_category.csv"
        
        output_dir = r"/Users/henriquealmeida/Library/CloudStorage/GoogleDrive-henriquesilveiradealmeida@gmail.com/Meu Drive/Consultoria/Rosa azul/rosaazul-code/rosaazul_consultoria/data/processed"
        os.makedirs(output_dir, exist_ok=True)
        full_local_path = os.path.join(output_dir, filename_local)
        
        df.to_csv(full_local_path, index=False, sep=",", encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
        logger.info(f"✅ Produtos salvos localmente em {full_local_path} ({len(df)} linhas)")

        # --- Chamada para upload no GCS ---
        upload_file_to_gcs(full_local_path, GCS_BUCKET_NAME, GCS_FOLDER_NAME)
        # --------------------------------

    else:
        logger.warning("⚠ Nenhum produto encontrado.")

if __name__ == "__main__":
    asyncio.run(main())