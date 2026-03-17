import os
import pandas as pd
import asyncio
import aiohttp
import random
from dotenv import load_dotenv
from datetime import datetime, timedelta

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
SITUACAO_APROVADO = 8
LIMIT = 100
MAX_CONCURRENT = 5  # número de pedidos processados em paralelo

# 📅 Período a ser consultado
DATA_INICIO = "2025-08-27"
DATA_FIM    = "2025-08-31"

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
            "situacao": SITUACAO_APROVADO,
            "limit": LIMIT,
            "page": page,
            "orderDirection": "asc"
        }
        async with session.get(f"{BASE_URL}/v2/site/pedido", auth=aiohttp.BasicAuth(USER, PASS), params=params) as resp:
            if resp.status != 200:
                print(f"❌ Erro ao buscar pedidos: {resp.status}")
                break
            result = await resp.json()
            pedidos = result["data"]["items"]
            if not pedidos:
                break
            todos_pedidos.extend(pedidos)
            print(f"✅ Página {page} do dia {data_inicio} carregada ({len(pedidos)} pedidos)")
            page += 1
    return todos_pedidos

# -------------------------------
# 4️⃣ Função para buscar itens de um pedido com retry
# -------------------------------
async def buscar_itens(session, pedido):
    codigo_pedido = pedido["codigo"]
    detalhe_url = f"{BASE_URL}/v2/site/pedido/{codigo_pedido}"
    itens = []

    while True:
        try:
            async with session.get(detalhe_url, auth=aiohttp.BasicAuth(USER, PASS)) as resp:
                if resp.status == 200:
                    data = await resp.json()
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
                    break
                elif resp.status == 429:
                    delay = random.uniform(1.0, 2.0)
                    print(f"⚠️ 429 para pedido {codigo_pedido}, esperando {delay:.1f}s antes de tentar de novo")
                    await asyncio.sleep(delay)
                else:
                    print(f"⚠️ Erro ao buscar itens do pedido {codigo_pedido}: {resp.status}")
                    break
        except Exception as e:
            print(f"⚠️ Exception ao buscar itens do pedido {codigo_pedido}: {e}")
            await asyncio.sleep(1)
    return itens

# -------------------------------
# 5️⃣ Função principal para processar pedidos por dia
# -------------------------------
async def processar_dia(session, data_inicio, data_fim):
    pedidos = await buscar_pedidos(session, data_inicio, data_fim)
    print(f"📌 Total de pedidos do dia {data_inicio}: {len(pedidos)}")

    tarefas = []
    todos_itens = []
    for pedido in pedidos:
        tarefas.append(buscar_itens(session, pedido))
        if len(tarefas) >= MAX_CONCURRENT:
            resultados = await asyncio.gather(*tarefas)
            tarefas = []
            for sublist in resultados:
                todos_itens.extend(sublist)

    if tarefas:
        resultados = await asyncio.gather(*tarefas)
        for sublist in resultados:
            todos_itens.extend(sublist)

    return todos_itens

# -------------------------------
# 6️⃣ Rodar todo o período
# -------------------------------
async def main():
    async with aiohttp.ClientSession() as session:
        inicio = datetime.strptime(DATA_INICIO, "%Y-%m-%d")
        fim = datetime.strptime(DATA_FIM, "%Y-%m-%d")
        delta = timedelta(days=1)

        dia_atual = inicio
        todos_itens_periodo = []

        while dia_atual <= fim:
            data_inicio = dia_atual.strftime("%Y-%m-%d")
            data_fim = data_inicio
            print(f"\n🔹 Processando pedidos do dia {data_inicio}")
            itens_dia = await processar_dia(session, data_inicio, data_fim)
            todos_itens_periodo.extend(itens_dia)
            dia_atual += delta

        if todos_itens_periodo:
            df_itens = pd.DataFrame(todos_itens_periodo)
            arquivo_final = "itens_pedidos_range.csv"
            df_itens.to_csv(arquivo_final, index=False)
            print(f"\n📂 Total de {len(df_itens)} itens salvos em {arquivo_final}")

# -------------------------------
# 7️⃣ Executar
# -------------------------------
asyncio.run(main())
