# 1. Usa a versão do Python
FROM python:3.12.8-slim

# 2. Define o diretório de trabalho dentro do container
WORKDIR /automacoes_magazord 

# 3. Copia o arquivo de requisitos para aproveitar o cache
COPY requirements.txt .

# 4. Instala as dependências de forma eficiente
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copia todos os seus arquivos de código
# Copia main.py e todos os módulos auxiliares para /automacoes_magazord
COPY . .

# 6. Comando de execução (Entrypoint)
# O Cloud Run Job chamará o main.py para iniciar seu pipeline.
CMD ["python", "main.py"]