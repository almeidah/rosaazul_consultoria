# 🚀 Guia de Deploy: Extrator Magazord para GCP

Este guia documenta o passo a passo completo para configurar o ambiente de extração no Google Cloud Platform (GCP) com foco em múltiplos clientes.

## 1. Configuração Inicial do Projeto
Primeiro, verifique se o `gcloud` está apontando para o projeto correto:
```bash
gcloud config set project sandbox-magazord
```

Habilite as APIs necessárias:
```bash
gcloud services enable \
    artifactregistry.googleapis.com \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    cloudscheduler.googleapis.com
```

## 2. Criar Repositório de Imagens (Artifact Registry)
Crie o repositório onde as imagens Docker serão armazenadas:
```bash
gcloud artifacts repositories create magazord-images \
    --repository-format=docker \
    --location=us-east1 \
    --description="Repositorio para imagens de extracao Magazord"
```

## 3. Construir e Enviar a Imagem do Cliente
Execute este comando dentro da pasta `gcp/`. Ele criará uma "caixa" específica para o cliente dentro do seu repositório:

```bash
gcloud builds submit --tag us-east1-docker.pkg.dev/sandbox-magazord/magazord-images/meujeans:v1 .
```

---

## 4. Criar o Job para um NOVO CLIENTE
Para cada cliente novo, você cria um Job diferente usando a MESMA imagem. 

### Cliente: Meu Jeans
```bash
gcloud run jobs create extrator-meujeans \
    --image us-east1-docker.pkg.dev/sandbox-magazord/magazord-images/extrator-magazord:v1 \
    --region us-east1 \
    --memory 512Mi \
    --cpu 1 \
    --set-env-vars "MAGAZORD_BASE_URL=https://meujeans.painel.magazord.com.br/api" \
    --set-env-vars "MAGAZORD_USER=..." \
    --set-env-vars "MAGAZORD_PASS=..." \
    --set-env-vars "DAYS_AGO=30" \
    --set-env-vars "GCS_BUCKET_NAME=magazord-bd" \
    --set-env-vars "GCS_FOLDER_NAME=meujeans"
```

### Cliente: Próximo Cliente (Exemplo)
```bash
gcloud run jobs create extrator-outro-cliente \
    --image us-east1-docker.pkg.dev/sandbox-magazord/magazord-images/extrator-magazord:v1 \
    --region us-east1 \
    --memory 512Mi \
    --cpu 1 \
    --set-env-vars "MAGAZORD_BASE_URL=https://outro.painel.magazord.com.br/api" \
    --set-env-vars "MAGAZORD_USER=USUARIO_DELE" \
    --set-env-vars "MAGAZORD_PASS=SENHA_DELE" \
    --set-env-vars "DAYS_AGO=30" \
    --set-env-vars "GCS_BUCKET_NAME=magazord-bd" \
    --set-env-vars "GCS_FOLDER_NAME=outro-cliente"
```

---

## 5. Agendar Execuções (Cloud Scheduler)
Para cada Job de cliente, você cria um agendamento:

```bash
gcloud scheduler jobs create http trigger-meujeans \
    --schedule="0 8,14,20 * * *" \
    --uri="https://us-east1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/sandbox-magazord/jobs/extrator-meujeans:run" \
    --http-method=POST \
    --oauth-service-account-email="SUA_CONTA_DE_SERVICO@sandbox-magazord.iam.gserviceaccount.com" \
    --location=us-east1
```

---
## Dicas Importantes:
1. **Pasta Automática:** Se você mudar a variável `GCS_FOLDER_NAME`, o script criará a pasta automaticamente dentro do bucket. 
2. **Atualização do Código:** Se você melhorar o código, rode o Passo 3 novamente. Todos os Jobs de todos os clientes usarão a versão nova automaticamente se você mantiver a tag `:v1` ou usar `:latest`.
