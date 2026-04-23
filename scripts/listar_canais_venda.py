import os
import aiohttp
import asyncio
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env local
load_dotenv()

BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

async def listar_canais():
    if not all([BASE_URL, USER, PASS]):
        print("❌ Erro: Variáveis de ambiente MAGAZORD_BASE_URL, MAGAZORD_USER ou MAGAZORD_PASS não encontradas no .env.")
        return

    url = f"{BASE_URL}/v2/site/marketplace"
    
    print("🔍 Buscando Canais de Venda (Marketplaces / Lojas Próprias) no Magazord...\n")
    print("-" * 50)
    print(f"{'CÓDIGO (origem)':<18} | {'NOME DO CANAL'}")
    print("-" * 50)

    try:
        async with aiohttp.ClientSession() as session:
            # Desabilitando validação de SSL por causa dos avisos de certificado do Magazord localmente
            async with session.get(url, auth=aiohttp.BasicAuth(USER, PASS), ssl=False) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    items = data.get("data", {}).get("items", [])
                    
                    if not items:
                        print("Nenhum canal encontrado.")
                        return

                    # Ordenando para ficar mais visual
                    items_ordenados = sorted(items, key=lambda x: int(x.get("id", 0)))
                    
                    for item in items_ordenados:
                        id_canal = item.get("id")
                        nome_canal = item.get("nome")
                        print(f" {id_canal:<17} | {nome_canal}")
                    
                    print("-" * 50)
                    print("\n✅ Consulta finalizada com sucesso.")
                    print("💡 Dica: No script de extrator, o código acima vira a coluna 'canalVenda' (origem).")
                else:
                    erro = await resp.text()
                    print(f"❌ Erro HTTP {resp.status} ao consultar API: {erro}")
    except Exception as e:
        print(f"❌ Erro ao conectar com a API: {e}")

if __name__ == "__main__":
    asyncio.run(listar_canais())
