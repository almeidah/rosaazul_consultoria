import asyncio
import logging
from datetime import datetime

# ⚠️ Importe todos os seus módulos auxiliares.
import order_items
import stock
import products
import orders
import products_tratados # ⬅️ Novo módulo adicionado


# -------------------------------
# 0️⃣ Logging
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------------------
# 1️⃣ Função Principal de Orquestração
# -------------------------------
async def main_pipeline():
    logger.info("==================================================")
    logger.info(f"🚀 INICIANDO PIPELINE DE EXTRAÇÃO MAGAZORD - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("==================================================")

    # 1. EXTRAÇÃO DE ITENS DE PEDIDOS
    try:
        logger.info("\n[ETAPA 1/5] Chamando Extrator de ITENS DE PEDIDOS (order_items)...")
        await order_items.executar_job() 
        logger.info("[ETAPA 1/5] ✅ ITENS DE PEDIDOS CONCLUÍDO.")
    except Exception as e:
        logger.error(f"[ETAPA 1/5] ❌ FALHA no Processamento de Itens de Pedidos: {e}")

    # 2. EXTRAÇÃO DE ESTOQUE
    try:
        logger.info("\n[ETAPA 2/5] Chamando Extrator de ESTOQUE (stock)...")
        await stock.executar_job()
        logger.info("[ETAPA 2/5] ✅ ESTOQUE CONCLUÍDO.")
    except Exception as e:
        logger.error(f"[ETAPA 2/5] ❌ FALHA no Processamento de Estoque: {e}")

    # 3. EXTRAÇÃO DE PRODUTOS
    try:
        logger.info("\n[ETAPA 3/5] Chamando Extrator de PRODUTOS (products)...")
        await products.executar_job()
        logger.info("[ETAPA 3/5] ✅ PRODUTOS CONCLUÍDOS.")
    except Exception as e:
        logger.error(f"[ETAPA 3/5] ❌ FALHA nos Produtos: {e}")

    # 4. EXTRAÇÃO DE PEDIDOS (Cabeçalho)
    try:
        logger.info("\n[ETAPA 4/5] Chamando Extrator de PEDIDOS - Cabeçalho (orders)...")
        await orders.executar_job()
        logger.info("[ETAPA 4/5] ✅ PEDIDOS (Cabeçalho) CONCLUÍDOS.")
    except Exception as e:
        logger.error(f"[ETAPA 4/5] ❌ FALHA nos Pedidos (Cabeçalho): {e}")

    # 5. EXTRAÇÃO DE PRODUTOS TRATADOS (Derivações)
    try:
        logger.info("\n[ETAPA 5/5] Chamando Extrator de PRODUTOS TRATADOS (products_tratados)...")
        await products_tratados.executar_job()
        logger.info("[ETAPA 5/5] ✅ PRODUTOS TRATADOS CONCLUÍDOS.")
    except Exception as e:
        logger.error(f"[ETAPA 5/5] ❌ FALHA nos Produtos Tratados: {e}")

# -------------------------------
# 2️⃣ Executar (Ponto de entrada do Container)
# -------------------------------
if __name__ == "__main__":
    asyncio.run(main_pipeline())