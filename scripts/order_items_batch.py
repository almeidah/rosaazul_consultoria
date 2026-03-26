import os
import pandas as pd
import asyncio
import aiohttp
import random
import glob
from dotenv import load_dotenv
from datetime import datetime, timedelta
from google.cloud import storage 

# -------------------------------
# 0️⃣ Logging
# -------------------------------
import logging
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
# 2️⃣ Configurações iniciais
# -------------------------------
SITUACOES_APROVADO = [4, 5, 6, 7, 8]
LIMIT = 100
MAX_CONCURRENT = 3

# 📅 Período a ser consultado (Ajuste para a carga inicial desejada)
DIAS_ATRAS = 449
DATA_INICIO = (datetime.today() - timedelta(days=DIAS_ATRAS)).strftime("%Y-%m-%d")
DATA_FIM    = datetime.today().strftime("%Y-%m-%d")

# Pasta destino para salvar os arquivos localmente
PASTA_DESTINO = r"/Users/henriquealmeida/Library/CloudStorage/GoogleDrive-henriquesilveiradealmeida@gmail.com/Meu Drive/Consultoria/Rosa azul/rosaazul-code/rosaazul_consultoria/data/processed"

# Configurações do Google Cloud Storage
GCS_BUCKET_NAME = "magazord-bd"
GCS_FOLDER_NAME = "rosaazul"

# -------------------------------
# 3️⃣ Função para buscar pedidos de um único dia
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
            async with session.get(f"{BASE_URL}/v2/site/pedido", auth=aiohttp.BasicAuth(USER, PASS), params=params, ssl=False) as resp:
                if resp.status != 200:
                    logger.error(f"❌ Erro ao buscar pedidos na página {page} do dia {data_inicio}: {resp.status}")
                    break
                result = await resp.json()
                pedidos = result.get("data", {}).get("items", [])
                if not pedidos:
                    break
                todos_pedidos.extend(pedidos)
                logger.info(f"✅ Página {page} do dia {data_inicio} carregada ({len(pedidos)} pedidos)")
                page += 1
        except Exception as e:
            logger.error(f"❌ Exceção ao buscar pedidos na página {page} do dia {data_inicio}: {e}")
            break
    return todos_pedidos

# -------------------------------
# 4️⃣ Função para buscar itens de um pedido com todos os campos do cabeçalho
# -------------------------------
async def buscar_itens(session, pedido):
    codigo_pedido = pedido["codigo"]
    detalhe_url = f"{BASE_URL}/v2/site/pedido/{codigo_pedido}"
    itens = []

    for attempt in range(5):
        try:
            async with session.get(detalhe_url, auth=aiohttp.BasicAuth(USER, PASS), ssl=False) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pedido_data = data.get("data", {})
                    
                    # --- Extraindo Rastreamento e Nota Fiscal (do primeiro pacote) ---
                    rastreios = pedido_data.get("arrayPedidoRastreio", [])
                    rastreio_1 = rastreios[0] if rastreios else {}
                    nfs = rastreio_1.get("pedidoNotaFiscal", [])
                    nf_1 = nfs[0] if nfs else {}

                    # --- Extração de campos do cabeçalho (Header) do Pedido ---
                    header_fields = {
                        "codigoPedido": codigo_pedido,
                        "dataHora": pedido_data.get("dataHora"),
                        "situacao": pedido_data.get("pedidoSituacao"),
                        "situacaoNome": pedido_data.get("pedidoSituacaoDescricao"),
                        "dataHoraSituacao": pedido_data.get("dataHoraUltimaAlteracaoSituacao"), # Só existe na lista de pedidos
                        "vendedor": pedido_data.get("codigoVendedor"),
                        "apelidoVendedor": None, # Não aplicável no Magazord
                        "canalVenda": pedido_data.get("origem"),
                        "formaPagamento": pedido_data.get("formaPagamentoNome"),
                        "pessoaId": pedido_data.get("pessoaId"),
                        "cliente": pedido_data.get("pessoaNome"),
                        "pessoaCpfCnpj": pedido_data.get("pessoaCpfCnpj"),
                        "pessoaEmail": pedido_data.get("pessoaEmail"),
                        "pessoaTelefone": None, # Requer parâmetro extra na API
                        "valorTotalPedido": pedido_data.get("valorTotal"),
                        "valorFretePedido": pedido_data.get("valorFrete"),
                        "valorDescontoPedido": pedido_data.get("valorDesconto"),
                        "valorJurosPedido": pedido_data.get("valorAcrescimo"),
                        "valorOutrasDespesasPedido": None, # Não mapeado diretamente
                        "nfeNumero": nf_1.get("numero"),
                        "nfeSerie": nf_1.get("serieLegal"),
                        "nfeChave": nf_1.get("chave"),
                        "transportadora": rastreio_1.get("transportadoraNome"),
                        "transportadoraNome": rastreio_1.get("transportadoraNome"),
                        "servicoEnvio": rastreio_1.get("transportadoraServicoDescricao"),
                        "codigoRastreamento": rastreio_1.get("codigoRastreio"),
                        "dataPrevisaoEntrega": rastreio_1.get("dataLimiteEntregaCliente"),
                        "dataEntrega": rastreio_1.get("dataLimitePostagem"),
                        "enderecoLogradouro": pedido_data.get("logradouro"),
                        "enderecoNumero": pedido_data.get("numero"),
                        "enderecoComplemento": pedido_data.get("complemento"),
                        "enderecoBairro": pedido_data.get("bairro"),
                        "enderecoCidade": pedido_data.get("cidadeNome"),
                        "enderecoUf": pedido_data.get("estadoSigla"),
                        "enderecoCep": pedido_data.get("cep")
                    }

                    for rastreio in pedido_data.get("arrayPedidoRastreio", []):
                        for item in rastreio.get("pedidoItem", []):
                            item_completo = header_fields.copy()
                            item_completo.update({
                                "produtoDerivacaoCodigo": item.get("produtoDerivacaoCodigo"),
                                "produtoId": item.get("produtoId"),
                                "produtoDerivacaoId": item.get("produtoDerivacaoId"),
                                "produtoNome": item.get("produtoNome"),
                                "descricao": item.get("descricao"),
                                "quantidade": item.get("quantidade"),
                                "valorUnitario": item.get("valorUnitario"),
                                "valorItem": item.get("valorItem"),
                                "valorFreteItem": item.get("valorFrete"),
                                "marca": item.get("marcaNome"),
                                "categoria": item.get("categoria"),
                                "linkProduto": item.get("linkProduto")
                            })
                            itens.append(item_completo)
                    return itens
                elif resp.status == 429:
                    delay = random.uniform(1.0, 2.0) * (attempt + 1)
                    logger.warning(f"⚠️ 429 para pedido {codigo_pedido}, esperando {delay:.1f}s")
                    await asyncio.sleep(delay)
                else:
                    logger.warning(f"⚠️ Erro ao buscar itens do pedido {codigo_pedido}: {resp.status}")
                    break
        except Exception as e:
            logger.error(f"⚠️ Exception ao buscar itens do pedido {codigo_pedido}: {e}")
            await asyncio.sleep(1 * (attempt + 1))
    return []

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
# 6️⃣ Função principal para processar pedidos por dia
# -------------------------------
async def processar_dia(session, data_inicio, data_fim):
    pedidos = await buscar_pedidos(session, data_inicio, data_fim)
    logger.info(f"📌 Total de pedidos do dia {data_inicio}: {len(pedidos)}")

    tarefas = []
    todos_itens = []
    for pedido in pedidos:
        tarefas.append(buscar_itens(session, pedido))
        if len(tarefas) >= MAX_CONCURRENT:
            resultados = await asyncio.gather(*tarefas)
            tarefas = []
            for sublist in resultados:
                if sublist:
                    todos_itens.extend(sublist)

    if tarefas:
        resultados = await asyncio.gather(*tarefas)
        for sublist in resultados:
            if sublist:
                todos_itens.extend(sublist)

    return todos_itens

# -------------------------------
# 7️⃣ Funções auxiliares para arquivos mensais
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
        sucesso = upload_file_to_gcs(caminho_final, bucket_name, folder_name)
        
        # Apagar os arquivos mensais após sucesso
        if sucesso:
            for arq in arquivos:
                try:
                    os.remove(arq)
                except Exception as e:
                    logger.error(f"❌ Erro ao excluir {arq}: {e}")
            logger.info("🗑️ Arquivos mensais temporários apagados.")

# -------------------------------
# 8️⃣ Rodar todo o período
# -------------------------------
async def main():
    async with aiohttp.ClientSession() as session:
        inicio = datetime.strptime(DATA_INICIO, "%Y-%m-%d")
        fim = datetime.strptime(DATA_FIM, "%Y-%m-%d")
        delta = timedelta(days=1)

        dia_atual = inicio
        mes_atual = inicio.strftime("%Y-%m")
        itens_mes_atual = []

        while dia_atual <= fim:
            mes_dia_atual = dia_atual.strftime("%Y-%m")
            
            # Mudou de mês, salva o que acumulou no mês anterior
            if mes_dia_atual != mes_atual:
                if itens_mes_atual:
                    salvar_arquivo_mensal(itens_mes_atual, mes_atual, PASTA_DESTINO, "order_items_temp")
                mes_atual = mes_dia_atual
                itens_mes_atual = []

            data_inicio = dia_atual.strftime("%Y-%m-%d")
            logger.info(f"\n🔹 Processando pedidos do dia {data_inicio}")
            itens_dia = await processar_dia(session, data_inicio, data_inicio)
            itens_mes_atual.extend(itens_dia)
            dia_atual += delta

        # Salva o último mês residual
        if itens_mes_atual:
            salvar_arquivo_mensal(itens_mes_atual, mes_atual, PASTA_DESTINO, "order_items_temp")

        # Concatena os meses gerados e sobe para o GCS
        juntar_arquivos_e_subir(PASTA_DESTINO, "order_items_temp", "order_items.csv", GCS_BUCKET_NAME, GCS_FOLDER_NAME)

if __name__ == "__main__":
    asyncio.run(main())
