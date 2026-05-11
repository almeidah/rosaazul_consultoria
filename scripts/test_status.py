import asyncio
import aiohttp
import os
import json
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

async def test_status():
    async with aiohttp.ClientSession() as session:
        status_map = {}
        # Fetch a robust sample of pages
        for x in [1, 5, 20, 50, 100, 200, 400, 600, 800, 1000]:
            print(f"Buscando pagina {x}...")
            params = {"limit": 100, "page": x}
            resp = await session.get(f"{BASE_URL}/v2/site/pedido", auth=aiohttp.BasicAuth(USER, PASS), params=params, ssl=False)
            if resp.status == 200:
                data = await resp.json()
                for p in data.get("data", {}).get("items", []):
                    situacao_id = p.get("pedidoSituacao")
                    situacao_desc = p.get("pedidoSituacaoDescricao")
                    if situacao_id and situacao_desc:
                        status_map[situacao_id] = situacao_desc
        
        print("\n--- STATUS ENCONTRADOS ---")
        for k, v in sorted(status_map.items()):
            print(f"ID {k}: {v}")

asyncio.run(test_status())
