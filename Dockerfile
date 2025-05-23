# Use uma imagem base oficial do Python
FROM python:3.9-slim

# Define variáveis de ambiente para Python
ENV PYTHONDONTWRITEBYTECODE 1  # Impede o Python de escrever arquivos .pyc
ENV PYTHONUNBUFFERED 1         # Força o stdout/stderr a serem enviados diretamente (bom para logs em containers)

# Define o diretório de trabalho no container
WORKDIR /app

# Copia o arquivo de dependências primeiro para aproveitar o cache do Docker
COPY requirements.txt ./

# Instala as dependências
# Usar --no-cache-dir para reduzir o tamanho da imagem
# Adicionar --upgrade pip para garantir que estamos usando uma versão recente do pip
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia o restante dos arquivos da aplicação para o diretório de trabalho no container
# Isso copiará a pasta 'src', 'ontologies', etc., para dentro de '/app'
COPY . .

# Adiciona o diretório /app/src ao PYTHONPATH
# Isso ajuda o Python a encontrar módulos dentro de /app/src
ENV PYTHONPATH "${PYTHONPATH}:/app/src"

# Expõe a porta que a aplicação Flask usa (conforme definido em app.py ou gunicorn)
EXPOSE 5000

# -------------------------------------------------------------------------
# CMD DE PRODUÇÃO (DEVE FUNCIONAR SE A ESTRUTURA ESTIVER CORRETA)
# Gunicorn vai mudar para o diretório 'src' (que estará em /app/src) antes de carregar 'app:app'
# O app:app refere-se ao arquivo app.py (módulo app) e à variável app dentro dele.
# CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--chdir", "src", "app:app"] # MANTENHA ESTA LINHA COMENTADA POR ENQUANTO
# -------------------------------------------------------------------------

# -------------------------------------------------------------------------
# CMD PARA DEPURAÇÃO (ATIVE-O SE O CMD DE PRODUÇÃO ACIMA FALHAR)
# Para ativar: comente o CMD de produção acima e DESCOMENTE este CMD de depuração.
# Este CMD irá listar o conteúdo dos diretórios /app e /app/src, o PYTHONPATH,
# e então tentará executar o Gunicorn.
# Observe a saída desses comandos nos logs de runtime do Render.
# -------------------------------------------------------------------------
CMD ["sh", "-c", "echo 'DEBUG: PYTHONPATH é ${PYTHONPATH}' && echo 'DEBUG: Current Directory:' && pwd && echo 'DEBUG: Conteúdo de /app:' && ls -Rla /app && echo 'DEBUG: Conteúdo de /app/src:' && ls -Rla /app/src && echo 'DEBUG: Tentando rodar Gunicorn com chdir src...' && gunicorn --bind 0.0.0.0:5000 --chdir src app:app"] # MANTENHA ESTA LINHA DESCOMENTADA PARA DEPURAÇÃO