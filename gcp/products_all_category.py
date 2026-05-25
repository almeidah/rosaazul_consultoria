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
MAX_CONCURRENT_REQUESTS = 10
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# 🕒 Configuração de Incremental
DAYS_AGO_UPDATE = int(os.getenv("DAYS_AGO_UPDATE", 1))
DATE_FILTER = (datetime.now() - timedelta(days=DAYS_AGO_UPDATE)).strftime("%Y-%m-%dT%H:%M:%SZ")

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "magazord-bd")
GCS_FOLDER_NAME = os.getenv("GCS_FOLDER_NAME", "rosaazul")
GCS_FILE_NAME = "products_all_category.csv" 

# Mapeamento oficial de IDs de Características do Magazord para nomes de colunas
MAP_CHARS = {
    1: "char_faixa_etaria_1",
    2: "char_genero_2",
    3: "char_cor_3",
    4: "char_tabelas_4",
    5: "char_composicao_5",
    6: "char_gancho_cintura_6",
    7: "char_bolsos_7",
    8: "char_modelagem_8",
    9: "char_colecao_9",
    10: "char_modelo_veste_10",
    11: "char_indicacao_tamanho_11",
    12: "char_cuidados_12",
    13: "char_garantia_satisfacao_13",
    14: "char_tecido_14",
    15: "char_cor_15",
    16: "char_modelagem_16",
    17: "char_composicao_17",
    19: "char_protecao_19",
    20: "char_estampa_20",
    22: "char_linha_artigo_22"
}

# -------------------------------
# 3️⃣ Funções de requisição
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

async def fetch_produto_caracteristicas(session, product_code: str):
    url = f"{BASE_URL}/v2/site/produto/{product_code}/caracteristica"
    try:
        async with semaphore:
            async with session.get(url,
                                   auth=aiohttp.BasicAuth(USER, PASS),
                                   timeout=REQUEST_TIMEOUT,
                                   ssl=False) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", [])
                else:
                    return []
    except Exception as e:
        logger.error(f"❌ Erro ao buscar características do produto {product_code}: {e}")
        return []

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

async def fetch_all_marcas(session):
    url = f"{BASE_URL}/v2/site/marca"
    page = 1
    brand_dict = {}
    while True:
        params = {"limit": 100, "page": page}
        try:
            async with session.get(url, auth=aiohttp.BasicAuth(USER, PASS), params=params, timeout=REQUEST_TIMEOUT, ssl=False) as resp:
                if resp.status == 200:
                    data = (await resp.json()).get("data", {})
                    items = data.get("items", [])
                    for item in items:
                        brand_dict[item["id"]] = item["nome"]
                    if not data.get("has_more", False):
                        break
                    page += 1
                else:
                    break
        except Exception as e:
            logger.error(f"❌ Erro ao buscar marcas: {e}")
            break
    return brand_dict

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

        logger.info("🏷 Buscando marcas...")
        brand_dict = await fetch_all_marcas(session)
        logger.info(f"✔ {len(brand_dict)} marcas carregadas para enriquecimento.")

        page = 1
        all_items = []

        PRODUCT_TYPES = {
            1: "Normal",
            2: "Grade",
            3: "Kit",
            4: "Consumo",
            5: "Conjunto"
        }

        # 1. Puxa todos os produtos das páginas
        while True:
            items, has_more = await fetch_produto_page(session, page)
            if not items:
                break
            
            # Enriquecimento com a árvore de categorias, marcas e tipo
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
                
                # Resolução de categorias
                if isinstance(cat_list, list):
                    item["categorias"] = "; ".join(cat_dict[cid]["nome"] for cid in cat_list if cid in cat_dict)

                # Resolução de Marca
                brand_id = item.get("marca")
                if brand_id is not None:
                    item["marca"] = brand_dict.get(brand_id, brand_id)

                # Resolução de Tipo
                tipo_id = item.get("tipo")
                if tipo_id is not None:
                    item["tipo"] = PRODUCT_TYPES.get(tipo_id, tipo_id)
                    
            all_items.extend(items)
            
            if not has_more:
                break
            page += 1
            await asyncio.sleep(0.1)

        # Busca as características dinâmicas de cada produto pai
        if all_items:
            logger.info(f"⚡ Buscando características dinâmicas de {len(all_items)} produtos pai...")
            chars_results = []
            chunk_size = 300 # Lotes de 300
            
            for i in range(0, len(all_items), chunk_size):
                chunk = all_items[i : i + chunk_size]
                logger.info(f"⏳ Processando lote {i} até {i + len(chunk)} de {len(all_items)}...")
                
                tasks = [fetch_produto_caracteristicas(session, item["codigo"]) for item in chunk]
                chunk_results = await asyncio.gather(*tasks)
                chars_results.extend(chunk_results)

            for item, chars in zip(all_items, chars_results):
                # Inicializa as características
                for col_name in MAP_CHARS.values():
                    item[col_name] = None
                
                # Preenche com os valores retornados do Magazord
                for char in chars:
                    c_id = char.get("codigo")
                    c_valor = char.get("valorDescritivo")
                    if c_valor is None:
                        c_valor = char.get("valor")
                    if c_valor is None:
                        c_valor = char.get("valorDescricao")
                    
                    if isinstance(c_valor, list):
                        c_valor = "; ".join(str(x) for x in c_valor)
                    elif c_valor is not None:
                        c_valor = str(c_valor)

                    if c_id in MAP_CHARS:
                        item[MAP_CHARS[c_id]] = c_valor

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
    logger.info(f"🚀 Iniciando extração e tratamento incremental de Produtos (com Categorias e Características) (>= {DATE_FILTER})...")
    items = await fetch_all_produtos()

    if items:
        df = pd.DataFrame(items)

        # Remove colunas indesejadas
        cols_to_drop = [c for c in ['acompanha'] if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
        
        # Trata Derivações
        if 'derivacoes' in df.columns:
            logger.info("🛠️ Normalizando derivações...")
            df = df.explode('derivacoes', ignore_index=True)
            derivacoes_normalized = pd.json_normalize(df['derivacoes']).add_suffix('_derivacao')
            df = pd.concat([df.drop(columns=['derivacoes']), derivacoes_normalized], axis=1)

        # Adiciona a data de extração
        df["dataExtracao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # --- Garantir Esquema Fixo para o BigQuery ---
        EXPECTED_COLUMNS = [
            "id", "nome", "palavraChave", "peso", "altura", "largura", "comprimento",
            "codigo", "tipo", "marca", "unidadeMedida", "ativo", "ncm", "cest", "origemFiscal",
            "dataLancamento", "dataAtualizacao", "categorias", 
            "categoria_nivel_1", "categoria_nivel_2", "categoria_nivel_3", "categoria_nivel_4",
            "modelo",
            "char_faixa_etaria_1", "char_genero_2", "char_cor_3", "char_tabelas_4", "char_composicao_5",
            "char_gancho_cintura_6", "char_bolsos_7", "char_modelagem_8", "char_colecao_9", "char_modelo_veste_10",
            "char_indicacao_tamanho_11", "char_cuidados_12", "char_garantia_satisfacao_13", "char_tecido_14",
            "char_cor_15", "char_modelagem_16", "char_composicao_17", "char_protecao_19", "char_estampa_20", "char_linha_artigo_22",
            "id_derivacao", "codigo_derivacao", "nome_derivacao", "ativo_derivacao", "dataExtracao"
        ]

        # 1. Cria colunas ausentes com valor nulo
        for col in EXPECTED_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # 2. Força a ordem exata das colunas e descarta colunas extras surpresas
        df = df[EXPECTED_COLUMNS]

        # --- Tratamento de Segurança para o BigQuery ---
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False).str.replace('\r', '', regex=False)

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
