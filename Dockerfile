# Use uma imagem base oficial do Python
FROM python:3.9-slim

# Define o diretório de trabalho no container
WORKDIR /app

# Copia o arquivo de dependências primeiro para aproveitar o cache do Docker
COPY requirements.txt ./

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante dos arquivos da aplicação para o diretório de trabalho no container
COPY . .

# (Opcional, mas bom para debug inicial) Lista o conteúdo do diretório /app
RUN ls -la /app

# Expõe a porta que a aplicação Flask usa (conforme definido em app.py)
EXPOSE 5000

# Comando para rodar a aplicação quando o container iniciar
# Use Gunicorn para um servidor mais robusto, ou python app.py para o dev server
# Certifique-se que gunicorn está em requirements.txt se for usar
# CMD ["python", "app.py"]
# OU (recomendado para produção/PaaS):
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app:app"]