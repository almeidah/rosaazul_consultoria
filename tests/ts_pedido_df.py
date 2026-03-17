import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

BASE_URL = os.getenv("MAGAZORD_BASE_URL")
USER = os.getenv("MAGAZORD_USER")
PASS = os.getenv("MAGAZORD_PASS")

# Endpoint pedidos
url = f"{BASE_URL}/v2/site/pedido"

print("🔄 Buscando pedidos...")
response = requests.get(url, auth=(USER, PASS))

if response.status_code == 200:
    data = response.json()["data"]["items"]
    
    # Converter em DataFrame para visualizar melhor
    df = pd.DataFrame(data)
    print(df.head(10))  # Mostra os 10 primeiros
    
    # (Opcional) Salvar em Excel/CSV
    df.to_excel("pedidos1.xlsx", index=False)
    print("📂 Pedidos salvos em pedidos.xlsx")
else:
    print("❌ Erro:", response.status_code, response.text)
