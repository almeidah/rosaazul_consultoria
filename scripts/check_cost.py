import asyncio
import aiohttp
import os
import json
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

async def test():
    async with aiohttp.ClientSession() as s:
        # Get one product
        r = await s.get(f"{BASE_URL}/v2/site/produto", auth=aiohttp.BasicAuth(USER, PASS), params={"limit": 1}, ssl=False)
        d = await r.json()
        items = d.get("data", {}).get("items", [])
        if items:
            produto_id = items[0]["codigo"]
            r2 = await s.get(f"{BASE_URL}/v2/site/produto/{produto_id}", auth=aiohttp.BasicAuth(USER, PASS), ssl=False)
            d2 = await r2.json()
            print("Produto KEYS:", list(d2.get("data", {}).keys()))
            print("CUSTO Produto:", d2.get("data", {}).get("precoCusto"))
            
        # Pega pedidos mais recentes (limit 100) e tenta achar UM com items
        params = {"limit": 100, "situacao": "4,5,6,7,8"}
        resp = await s.get(f"{BASE_URL}/v2/site/pedido", auth=aiohttp.BasicAuth(USER, PASS), params=params, ssl=False)
        data = await resp.json()
        
        for p in data.get("data", {}).get("items", []):
            pedido_id = p["codigo"]
            resp_det = await s.get(f"{BASE_URL}/v2/site/pedido/{pedido_id}", auth=aiohttp.BasicAuth(USER, PASS), ssl=False)
            det_data = await resp_det.json()
            pedido_info = det_data.get("data", {})
            
            for rastr in pedido_info.get("arrayPedidoRastreio", []):
                if rastr.get("pedidoItem"):
                    item = rastr["pedidoItem"][0]
                    print("ITEM DO PEDIDO KEYS:", list(item.keys()))
                    for k in item.keys():
                        if "cust" in k.lower() or "cost" in k.lower() or "preco" in k.lower():
                            print(f"{k}: {item[k]}")
                    return

if __name__ == '__main__':
    asyncio.run(test())
