##otimizado para evitar 429 e timeouts  

import os
import pandas as pd
import asyncio
import aiohttp
import random
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta

# -------------------------------
# 0️⃣ Configuração de Logging
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("magazord_data_extraction.log", encoding="utf-8"),
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
# 2️⃣ Configurações iniciais
# -------------------------------
SITUACAO_APROVADO = 8  # ID da situação de pedido "Aprovado" ou "Finalizado"
LIMIT = 100            # Máximo de registros por página na listagem de pedidos
MAX_CONCURRENT_REQUESTS = 4 # Limite REAL de requisições ativas simultaneamente à API
RETRY_ATTEMPTS = 5     # Número máximo de tentativas para requisições com erro (ex: 429, timeout)
BASE_RETRY_DELAY = 1.0 # Atraso inicial para retry (será exponencial)
MAX_RETRY_DELAY = 30.0 # Atraso máximo para retry
REQUEST_TIMEOUT_LIST = 30 # Timeout para requisições de lista de pedidos (em segundos)
REQUEST_TIMEOUT_DETAIL = 15 # Timeout para requisições de detalhe de pedido (em segundos)

# 📅 Período a ser consultado
# Formato YYYY-MM-DD
DATA_INICIO_STR = "2025-08-01" 
DATA_FIM_STR    = "2025-08-31"

# -------------------------------
# 3️⃣ Semáforo para controlar concorrência
# -------------------------------
# Isso limitará o número de requisições ativas simultaneamente à API
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# -------------------------------
# 4️⃣ Função para buscar pedidos de um único dia
# -------------------------------
async def buscar_pedidos(session, data_inicio_iso, data_fim_iso):
    todos_pedidos = []
    page = 1
    logger.info(f"🔄 Buscando lista de pedidos do dia {data_inicio_iso[:10]} (status: {SITUACAO_APROVADO})...")

    while True:
        params = {
            "dataHora[gte]": data_inicio_iso,
            "dataHora[lte]": data_fim_iso,
            "situacao": SITUACAO_APROVADO,
            "limit": LIMIT,
            "page": page,
            "orderDirection": "asc"
        }
        
        try:
            async with session.get(f"{BASE_URL}/v2/site/pedido", 
                                   auth=aiohttp.BasicAuth(USER, PASS), 
                                   params=params, 
                                   timeout=REQUEST_TIMEOUT_LIST) as resp:
                resp.raise_for_status() # Lança exceção para status 4xx/5xx
                result = await resp.json()
                
                if "data" not in result or "items" not in result["data"]:
                    logger.warning(f"Resposta inesperada para pedidos do dia {data_inicio_iso[:10]} na página {page}. Conteúdo: {result}")
                    break

                pedidos = result["data"]["items"]
                
                if not pedidos:
                    break
                
                todos_pedidos.extend(pedidos)
                logger.info(f"✅ Página {page} do dia {data_inicio_iso[:10]} carregada ({len(pedidos)} pedidos)")
                
                if not result["data"].get("has_more", False):
                    break
                
                page += 1
        except aiohttp.ClientResponseError as e:
            logger.error(f"❌ Erro HTTP ao buscar lista de pedidos do dia {data_inicio_iso[:10]} na página {page}: {e.status} - {e.message}. Resposta: {await e.response.text()}")
            break
        except aiohttp.ClientError as e:
            logger.error(f"❌ Erro de conexão ao buscar lista de pedidos do dia {data_inicio_iso[:10]} na página {page}: {e}")
            break
        except asyncio.TimeoutError:
            logger.error(f"❌ Timeout ({REQUEST_TIMEOUT_LIST}s) ao buscar lista de pedidos do dia {data_inicio_iso[:10]} na página {page}.")
            break
        except Exception as e:
            logger.error(f"❌ Erro inesperado ao buscar lista de pedidos do dia {data_inicio_iso[:10]} na página {page}: {e}")
            break
    return todos_pedidos

# -------------------------------
# 5️⃣ Função para buscar itens de um pedido com retry e semáforo
# -------------------------------
async def buscar_itens(session, pedido):
    codigo_pedido = pedido["codigo"]
    detalhe_url = f"{BASE_URL}/v2/site/pedido/{codigo_pedido}"
    itens = []

    tentativas = 0
    while tentativas < RETRY_ATTEMPTS:
        async with semaphore: # Adquire uma permissão do semáforo antes de fazer a requisição
            try:
                async with session.get(detalhe_url, 
                                       auth=aiohttp.BasicAuth(USER, PASS), 
                                       timeout=REQUEST_TIMEOUT_DETAIL) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # Verificar se 'data' existe e se 'data' tem 'arrayPedidoRastreio'
                        if "data" in data and isinstance(data["data"], dict):
                            for rastreio in data["data"].get("arrayPedidoRastreio", []):
                                for item in rastreio.get("pedidoItem", []):
                                    itens.append({
                                        "codigoPedido": codigo_pedido,
                                        "dataHora": data["data"].get("dataHora"),
                                        "cliente": data["data"].get("pessoaNome"),
                                        "produtoDerivacaoCodigo": item.get("produtoDerivacaoCodigo"),  # SKU
                                        "produtoId": item.get("produtoId"),
                                        "produtoDerivacaoId": item.get("produtoDerivacaoId"),
                                        "produtoNome": item.get("produtoNome"),
                                        "descricao": item.get("descricao"),
                                        "quantidade": item.get("quantidade"),
                                        "valorUnitario": item.get("valorUnitario"),
                                        "valorItem": item.get("valorItem"),
                                        "valorFrete": item.get("valorFrete"),
                                        "marca": item.get("marcaNome"),
                                        "categoria": item.get("categoria"),
                                        "linkProduto": item.get("linkProduto")
                                    })
                        break # Sucesso, sai do loop de retries
                    elif resp.status == 429:
                        delay = min(MAX_RETRY_DELAY, BASE_RETRY_DELAY * (2 ** tentativas) + random.uniform(0, 1)) # Backoff exponencial com jitter
                        logger.warning(f"⚠️ 429 para pedido {codigo_pedido}, esperando {delay:.1f}s antes de tentar de novo ({tentativas+1}/{RETRY_ATTEMPTS})")
                        await asyncio.sleep(delay)
                        tentativas += 1
                    else:
                        logger.error(f"⚠️ Erro ao buscar itens do pedido {codigo_pedido}: {resp.status} - {await resp.text()}")
                        break # Erro diferente de 429, não tenta novamente
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ Timeout ({REQUEST_TIMEOUT_DETAIL}s) ao buscar itens do pedido {codigo_pedido} ({tentativas+1}/{RETRY_ATTEMPTS})")
                await asyncio.sleep(BASE_RETRY_DELAY) # Pequeno delay antes de tentar novamente no timeout
                tentativas += 1
            except aiohttp.ClientError as e:
                logger.error(f"⚠️ Erro de conexão ao buscar itens do pedido {codigo_pedido}: {e} ({tentativas+1}/{RETRY_ATTEMPTS})")
                await asyncio.sleep(BASE_RETRY_DELAY)
                tentativas += 1
            except Exception as e:
                logger.error(f"⚠️ Erro inesperado ao buscar itens do pedido {codigo_pedido}: {e}")
                break # Erro inesperado, sai do loop
    return itens

# -------------------------------
# 6️⃣ Função para processar pedidos por dia
# Retorna a lista de itens coletados naquele dia
# -------------------------------
async def processar_dia(session, dia_atual_dt):
    # Formato ISO 8601 completo para a API
    data_inicio_iso = dia_atual_dt.strftime("%Y-%m-%dT00:00:00Z")
    data_fim_iso = dia_atual_dt.strftime("%Y-%m-%dT23:59:59Z")

    pedidos = await buscar_pedidos(session, data_inicio_iso, data_fim_iso)
    logger.info(f"📌 Total de pedidos do dia {data_inicio_iso[:10]} para detalhamento: {len(pedidos)}")

    if not pedidos:
        return []

    # Cria uma lista de tarefas para buscar os itens de cada pedido
    tarefas_itens = [buscar_itens(session, pedido) for pedido in pedidos]
    
    # Executa todas as tarefas de busca de itens em paralelo, respeitando o semáforo
    listas_de_itens_por_pedido = await asyncio.gather(*tarefas_itens)

    todos_itens_do_dia = []
    for sublist in listas_de_itens_por_pedido:
        todos_itens_do_dia.extend(sublist)
    
    logger.info(f"✅ {len(todos_itens_do_dia)} itens coletados para o dia {data_inicio_iso[:10]}.")
    return todos_itens_do_dia

# -------------------------------
# 7️⃣ Rodar todo o período e consolidar resultados
# -------------------------------
async def main():
    inicio_periodo = datetime.strptime(DATA_INICIO_STR, "%Y-%m-%d")
    fim_periodo = datetime.strptime(DATA_FIM_STR, "%Y-%m-%d")
    delta = timedelta(days=1)

    dia_atual = inicio_periodo
    all_collected_items = []

    logger.info(f"\n🚀 Iniciando coleta de dados de {inicio_periodo.strftime('%Y-%m-%d')} a {fim_periodo.strftime('%Y-%m-%d')}...")

    # Cria a sessão aiohttp uma vez para todas as requisições do main
    async with aiohttp.ClientSession() as session:
        while dia_atual <= fim_periodo:
            logger.info(f"\n--- Processando dia: {dia_atual.strftime('%Y-%m-%d')} ---")
            itens_do_dia = await processar_dia(session, dia_atual)
            all_collected_items.extend(itens_do_dia) # Adiciona os itens do dia à lista geral
            dia_atual += delta

    logger.info(f"\n🏁 Coleta de todos os dias finalizada. Total de {len(all_collected_items)} itens coletados.")

    # Salvar TODOS os itens em um único CSV
    if all_collected_items:
        df_final = pd.DataFrame(all_collected_items)
        
        # Gerar um nome de arquivo único para o período total
        nome_arquivo_final = f"order_items.csv"
        
        df_final.to_csv(nome_arquivo_final, index=False)
        logger.info(f"📂 Todos os {len(df_final)} itens salvos em {nome_arquivo_final}")
        logger.info("\n--- Primeiros 10 itens do arquivo final ---")
        logger.info(df_final.head(10).to_string()) # Usar to_string para melhor formatação no log
    else:
        logger.warning("⚠️ Nenhum item encontrado em todo o período para salvar.")

# -------------------------------
# 8️⃣ Executar
# -------------------------------
if __name__ == "__main__":
    asyncio.run(main())