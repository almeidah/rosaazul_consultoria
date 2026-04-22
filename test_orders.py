import asyncio
import aiohttp
import os
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

async def test():
    params = {
        "dataHoraUltimaAlteracaoSituacao[gte]": "2026-04-20",
        "dataHoraUltimaAlteracaoSituacao[lte]": "2026-04-21",
        "situacao": "4,5,6,7,8",
        "limit": 100,
        "page": 1,
        "orderDirection": "asc"
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{BASE_URL}/v2/site/pedido", auth=aiohttp.BasicAuth(USER, PASS), params=params, ssl=False) as resp:
            print(f"Status: {resp.status}")
            text = await resp.text()
            print(f"Body: {text[:300]}")

asyncio.run(test())
