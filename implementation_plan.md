# 🛠 Guia Completo: Deploy Magazord do Zero (Manual)

Este guia cobre desde a configuração inicial do projeto até o agendamento final, ideal para quando você precisar replicar este processo em uma nova conta ou projeto.

## Passo 1: Configuração do Projeto e APIs
Abra o seu terminal e execute os comandos abaixo para preparar o terreno:

```bash
# 1. Definir o ID do projeto (substitua pelo ID real)
gcloud config set project sandbox-magazord

# 2. Fazer login na conta Google
gcloud auth login

# 3. Habilitar as APIs fundamentais
gcloud services enable \
    artifactregistry.googleapis.com \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    cloudscheduler.googleapis.com
```

## Passo 2: Criar o Repositório de Imagens (Artifact Registry)
O Artifact Registry é onde guardamos as versões do seu sistema ("containers"):

```bash
gcloud artifacts repositories create magazord-images \
    --repository-format=docker \
    --location=us-east1 \
    --description="Repositorio para imagens de extracao Magazord"
```

## Passo 3: Construir e Enviar a Imagem
Entre na pasta `gcp/` do seu projeto e rode o comando que transforma o código em um container na nuvem:

```bash
cd gcp
gcloud builds submit --tag us-east1-docker.pkg.dev/sandbox-magazord/magazord-images/extrator-magazord:v1 .
```

> [!IMPORTANT]
> **Sobre Permissões:** Se este passo falhar, vá ao console Google Cloud > IAM e garanta que a conta `...-compute@developer.gserviceaccount.com` ou a conta do Cloud Build tenha a permissão de **Artifact Registry Administrator** e **Storage Admin**.

## Passo 4: Criar o Job no Cloud Run (Via Console)
1. Vá em [Cloud Run > Jobs](https://console.cloud.google.com/run/jobs).
2. Clique em **CREATE JOB**.
3. Selecione a imagem: `us-east1-docker.pkg.dev/sandbox-magazord/magazord-images/extrator-magazord:v1`.
4. Nome: `extrator-magazord-job`.
5. Em **Containers > Variables**, adicione (UM POR UM):
    - `MAGAZORD_BASE_URL`, `MAGAZORD_USER`, `MAGAZORD_PASS`
    - `DAYS_AGO`: 30
    - `GCS_BUCKET_NAME`: `magazord-bd`
    - `GCS_FOLDER_NAME`: `meujeans`
6. Clique em **CREATE**.

## Passo 5: Agendar a Automação (Cloud Scheduler)
1. Vá em [Cloud Scheduler](https://console.cloud.google.com/cloudscheduler).
2. Clique em **CREATE JOB**.
3. Frequência: `0 8,14,20 * * *` (roda 3x ao dia).
4. Target: **HTTP**.
5. URL: Cole o link de execução do seu Cloud Run Job.
6. Method: **POST**.
7. Auth: **Add OIDC token**.
8. Selecione a conta de serviço padrão do App Engine ou Compute Engine.

---
**Teste Final:** No painel do Cloud Run Job, clique em **EXECUTE** para ver se a primeira extração de 30 dias funciona!
