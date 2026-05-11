import asyncio
import aiohttp
import os
import json
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

async def test_list():
    async with aiohttp.ClientSession() as s:
        p = {"limit": 1}
        r = await s.get(f"{BASE_URL}/v2/site/pedido", auth=aiohttp.BasicAuth(USER, PASS), params=p, ssl=False)
        d = await r.json()
        item = d["data"]["items"][0]
        print(json.dumps(item, indent=2))

if __name__ == '__main__':
    asyncio.run(test_list())
