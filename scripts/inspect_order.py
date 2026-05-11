import asyncio
import aiohttp
import os
import json
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

async def test_order_detail():
    async with aiohttp.ClientSession() as session:
        params = {"limit": 5, "situacao": "4,5,6,7,8"}
        resp = await session.get(f"{BASE_URL}/v2/site/pedido", auth=aiohttp.BasicAuth(USER, PASS), params=params, ssl=False)
        data = await resp.json()
        items = data.get("data", {}).get("items", [])
        
        for p in items:
            pedido_id = p["codigo"]
            resp_det = await session.get(f"{BASE_URL}/v2/site/pedido/{pedido_id}", auth=aiohttp.BasicAuth(USER, PASS), ssl=False)
            det_data = await resp_det.json()
            pedido_info = det_data.get("data", {})
            
            print(f"PEDIDO {pedido_id}: chaves -> {list(pedido_info.keys())}")
            if pedido_info.get("arrayPedidoItem"):
                print("TEM arrayPedidoItem NO RAIZ")
                print(json.dumps(pedido_info["arrayPedidoItem"][0], indent=2))
                return

if __name__ == '__main__':
    asyncio.run(test_order_detail())
