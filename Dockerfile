# Use uma imagem base oficial do Python
FROM python:3.9-slim

# Define variáveis de ambiente para Python
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Instala ferramentas de build essenciais (gcc, etc.) e outras dependências do sistema
# A imagem python:3.9-slim é baseada em Debian/Ubuntu, então usamos apt-get
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Define o diretório de trabalho no container
WORKDIR /app

# Copia o arquivo de dependências primeiro
COPY requirements.txt ./

# Instala as dependências Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    python -m spacy download pt_core_news_sm

# Copia TUDO do seu projeto para /app no container
COPY . .

# Expõe a porta
EXPOSE 5000

# CMD para rodar o web_app.py que está na raiz /app
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "web_app:app"]

# -------------------------------------------------------------------------
# CMD PARA DEPURAÇÃO (SE O ACIMA FALHAR, COMENTE-O E DESCOMENTE ESTE)
# -------------------------------------------------------------------------
# CMD ["sh", "-c", "echo 'DEBUG: PYTHONPATH é ${PYTHONPATH}' && echo 'DEBUG: Current Directory:' && pwd && echo 'DEBUG: Conteúdo de /app:' && ls -Rla /app && echo 'DEBUG: Conteúdo de /app/src/main/resources:' && ls -Rla /app/src/main/resources && echo 'DEBUG: Tentando rodar Gunicorn...' && gunicorn --bind 0.0.0.0:5000 --chdir src/main/resources pln_processor:app"]