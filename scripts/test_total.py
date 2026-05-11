import asyncio
import aiohttp
import os
import json
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

async def test_order():
    async with aiohttp.ClientSession() as session:
        params = {"situacao": "4,5,6,7,8", "limit": 5}
        resp = await session.get(f"{BASE_URL}/v2/site/pedido", auth=aiohttp.BasicAuth(USER, PASS), params=params, ssl=False)
        data = await resp.json()
        items = data.get("data", {}).get("items", [])
        
        for p_aba in items:
            pedido_id = p_aba["codigo"]
            resp_det = await session.get(f"{BASE_URL}/v2/site/pedido/{pedido_id}", auth=aiohttp.BasicAuth(USER, PASS), ssl=False)
            det_data = await resp_det.json()
            p = det_data.get("data", {})
            
            total = float(p.get("valorTotal", 0))
            frete = float(p.get("valorFrete", 0))
            desc = float(p.get("valorDesconto", 0))
            juros = float(p.get("valorAcrescimo", 0))
            
            print(f"Pedido: {pedido_id} | Total Final Pago: {total:.2f} | Frete: {frete:.2f} | Desconto: {desc:.2f} | Juros: {juros:.2f}")

asyncio.run(test_order())
