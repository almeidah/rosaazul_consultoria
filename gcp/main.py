import asyncio
import logging
from datetime import datetime

# ⚠️ Importe todos os seus módulos auxiliares.
import order_items
import stock
import products
import products_category
import products_all_category
import orders
import orders_extras


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
    # 5. EXTRAÇÃO DE PRODUTOS CATEGORIAS
    try:
        logger.info("\n[ETAPA 5/6] Chamando Extrator de PRODUTOS CATEGORIAS (products_category)...")
        await products_category.executar_job()
        logger.info("[ETAPA 5/6] ✅ PRODUTOS CATEGORIAS CONCLUÍDOS.")
    except Exception as e:
        logger.error(f"[ETAPA 5/6] ❌ FALHA nos Produtos Categorias: {e}")

    # 5b. EXTRAÇÃO DE PRODUTOS COM TODAS AS CATEGORIAS (products_all_category)
    try:
        logger.info("\n[ETAPA 5b] Chamando Extrator de PRODUTOS E CATEGORIAS COMPLETO (products_all_category)...")
        await products_all_category.executar_job()
        logger.info("[ETAPA 5b] ✅ PRODUTOS E CATEGORIAS COMPLETO CONCLUÍDOS.")
    except Exception as e:
        logger.error(f"[ETAPA 5b] ❌ FALHA nos Produtos e Categorias Completo: {e}")

    # 6. EXTRAÇÃO DE PEDIDOS EXTRAS (Frete/Transportadora)
    try:
        logger.info("\n[ETAPA 6/6] Chamando Extrator de PEDIDOS EXTRAS (orders_extras)...")
        await orders_extras.executar_job()
        logger.info("[ETAPA 6/6] ✅ PEDIDOS EXTRAS CONCLUÍDOS.")
    except Exception as e:
        logger.error(f"[ETAPA 6/6] ❌ FALHA nos Pedidos Extras: {e}")



# -------------------------------
# 2️⃣ Executar (Ponto de entrada do Container)
# -------------------------------
if __name__ == "__main__":
    asyncio.run(main_pipeline())