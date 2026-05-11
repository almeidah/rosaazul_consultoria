import asyncio
import aiohttp
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

async def contar_status_atuais():
    # Olhar os últimos 15 dias para ver o que está "parado" nos transitional status
    data_inicio = (datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
    
    async with aiohttp.ClientSession() as session:
        counts = {}
        names = {}
        page = 1
        has_more = True
        
        print(f"🚀 Verificando pedidos desde {data_inicio}...")
        
        while has_more:
            params = {
                "limit": 100, 
                "page": page,
                "dataHora[gte]": data_inicio
            }
            try:
                async with session.get(f"{BASE_URL}/v2/site/pedido", 
                                      auth=aiohttp.BasicAuth(USER, PASS), 
                                      params=params, ssl=False) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        items = data.get("data", {}).get("items", [])
                        
                        if not items:
                            has_more = False
                            break
                            
                        for p in items:
                            s_id = p.get("pedidoSituacao")
                            s_name = p.get("pedidoSituacaoDescricao")
                            counts[s_id] = counts.get(s_id, 0) + 1
                            names[s_id] = s_name
                        
                        page += 1
                        if page > 100: # Limite de segurança
                            has_more = False
                    else:
                        print(f"❌ Erro na página {page}: {resp.status}")
                        has_more = False
            except Exception as e:
                print(f"❌ Exceção: {e}")
                has_more = False
                    
        print("\n--- RESUMO DE STATUS (ÚLTIMOS 15 DIAS NO MAGAZORD) ---")
        print(f"{'ID':<5} | {'SITUAÇÃO':<30} | {'TOTAL'}")
        print("-" * 50)
        for s_id in sorted(counts.keys()):
            print(f"{s_id:<5} | {str(names.get(s_id, 'Desconhecido')):<30} | {counts[s_id]}")

if __name__ == '__main__':
    asyncio.run(contar_status_atuais())
