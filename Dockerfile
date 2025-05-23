# Use uma imagem base oficial do Python
FROM python:3.9-slim

# Define variáveis de ambiente para Python
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Define o diretório de trabalho no container
WORKDIR /app

# Copia o arquivo de dependências primeiro
COPY requirements.txt ./

# Instala as dependências
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    python -m spacy download pt_core_news_sm # BAIXA O MODELO SPACY AQUI

# Copia TUDO do seu projeto para /app no container
# Isso inclui web_app.py, src/, ontologies/, etc.
COPY . .

# Expõe a porta
EXPOSE 5000

# CMD para rodar o web_app.py que está na raiz /app
# Gunicorn vai procurar por /app/web_app.py e a instância 'app' dentro dele.
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "web_app:app"]