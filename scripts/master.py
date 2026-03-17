import asyncio
import order_items  # script de pedidos
import stock        # script de estoque
import products     # script de produtos
import orders       # script de pedidos cabeça
import status_order # script de status de pedidos
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

async def main():
    logger.info("🚀 Iniciando processamento de pedidos...")
    await order_items.main()
    logger.info("✅ Processamento de pedidos concluído.\n")

    logger.info("🚀 Iniciando processamento de estoque...")
    await stock.main()
    logger.info("✅ Processamento de estoque concluído.\n")

    logger.info("🚀 Iniciando processamento de produtos...")
    await products.main()
    logger.info("✅ Processamento de produtos concluído.\n")

    logger.info("🚀 Iniciando processamento de pedidos cabeça...")
    await orders.main()
    logger.info("✅ Processamento de pedidos cabeça concluído.\n")

    logger.info("🚀 Iniciando processamento de status de pedidos...")
    await status_order.main()
    logger.info("✅ Processamento de status de pedidos concluído.\n")

if __name__ == "__main__":
    asyncio.run(main())
