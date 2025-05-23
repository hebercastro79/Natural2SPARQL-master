# Use uma imagem base oficial do Python
FROM python:3.9-slim

# Define o diretório de trabalho no container
WORKDIR /app

# Copia o arquivo de dependências primeiro para aproveitar o cache do Docker
COPY requirements.txt ./

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante dos arquivos da aplicação para o diretório de trabalho no container
# Isso copiará a pasta 'src', 'ontologies', etc., para dentro de '/app'
COPY . .

# REMOVA ESTA LINHA DE DEBUG (se ainda estiver lá)
# RUN ls -la /app

# Expõe a porta que a aplicação Flask usa (conforme definido em app.py ou gunicorn)
EXPOSE 5000

# Comando para rodar a aplicação quando o container iniciar
# Gunicorn vai mudar para o diretório 'src' (que estará em /app/src) antes de carregar 'app:app'
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--chdir", "src", "app:app"]