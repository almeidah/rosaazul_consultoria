import os
import requests
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

def test_connection():
    base_url = os.getenv("MAGAZORD_BASE_URL")
    username = os.getenv("MAGAZORD_USER")
    password = os.getenv("MAGAZORD_PASS")

    if not all([base_url, username, password]):
        print("❌ Erro: Alguma das variáveis de ambiente (URL, Usuário ou Senha) não foi encontrada no .env")
        return

    print(f"--- Testando Conexão Magazord ---")
    print(f"URL: {base_url}")
    
    # Casting para string para evitar erro de indexação no linting (Pyre2)
    user_str = str(username)
    print(f"Usuário: {user_str[:10]}...") 

    # Endpoint de exemplo (Pedidos)
    endpoint = f"{base_url}/v2/site/pedido"
    params = {"limit": 1}

    try:
        # Realiza a requisição usando Basic Auth
        response = requests.get(
            endpoint, 
            auth=(username, password), 
            params=params,
            timeout=10
        )

        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            print("✅ Sucesso! Conexão estabelecida e credenciais validadas.")
            data = response.json()
            items_count = len(data.get("data", {}).get("items", []))
            print(f"Destaque: Conseguimos ler {items_count} item(ns).")
        else:
            print("❌ Falha na conexão.")
            print(f"Resposta: {response.text}")

    except Exception as e:
        print(f"💥 Erro ao tentar conectar: {e}")

if __name__ == "__main__":
    test_connection()
