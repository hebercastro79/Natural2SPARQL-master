from flask import Flask, request, jsonify, render_template_string
import subprocess
import json
import os

app = Flask(__name__)

# Caminho para o script pln_processor.py DENTRO DO CONTAINER
# O Dockerfile copiará tudo para /app, então a estrutura será /app/src/main/resources/pln_processor.py
PLN_PROCESSOR_SCRIPT_PATH = "/app/src/main/resources/pln_processor.py"

# Diretório onde o pln_processor.py espera encontrar seus arquivos de resources
# que é o mesmo diretório do script pln_processor.py
CWD_FOR_PLN = "/app/src/main/resources"

# HTML simples para a interface
HTML_FORM = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Teste Natural2SPARQL (PLN)</title>
    <style>
        body { font-family: sans-serif; margin: 20px; background-color: #f4f4f4; color: #333; }
        .container { background-color: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
        h1 { color: #5a5a5a; }
        textarea, button { width: 100%; padding: 10px; margin-bottom: 10px; border-radius: 4px; border: 1px solid #ddd; box-sizing: border-box; }
        textarea { height: 80px; }
        button { background-color: #007bff; color: white; cursor: pointer; font-size: 16px; }
        button:hover { background-color: #0056b3; }
        pre { background-color: #eee; padding: 15px; border-radius: 4px; white-space: pre-wrap; word-wrap: break-word; }
        .error { color: red; font-weight: bold; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Teste do Processador PLN - Natural2SPARQL</h1>
        <form id="plnForm">
            <textarea id="pergunta" name="pergunta" placeholder="Digite sua pergunta em linguagem natural aqui..."></textarea>
            <button type="submit">Processar Pergunta</button>
        </form>
        <h2>Resultado:</h2>
        <pre id="resultado"><code id="resultado-json">Aguardando pergunta...</code></pre>
    </div>
    <script>
        document.getElementById('plnForm').addEventListener('submit', async function(event) {
            event.preventDefault();
            const pergunta = document.getElementById('pergunta').value;
            const resultadoJsonEl = document.getElementById('resultado-json');
            const resultadoContainerEl = document.getElementById('resultado');
            
            resultadoJsonEl.textContent = 'Processando...';
            resultadoContainerEl.className = ''; // Reseta classes de erro

            try {
                const response = await fetch('/processar_pergunta_pln', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ pergunta: pergunta }),
                });
                
                const data = await response.json();
                
                if (response.ok) {
                    resultadoJsonEl.textContent = JSON.stringify(data, null, 2);
                } else {
                    resultadoJsonEl.textContent = JSON.stringify(data, null, 2);
                    resultadoContainerEl.classList.add('error');
                }
            } catch (error) {
                resultadoJsonEl.textContent = 'Erro na comunicação com o servidor: ' + error.message;
                resultadoContainerEl.classList.add('error');
            }
        });
    </script>
</body>
</html>
"""

@app.route('/', methods=['GET'])
def index():
    return HTML_FORM

@app.route('/processar_pergunta_pln', methods=['POST'])
def processar_pergunta_pln_endpoint():
    data = request.get_json()
    if not data or 'pergunta' not in data:
        return jsonify({"erro": "Pergunta não fornecida no corpo JSON"}), 400
    
    pergunta_usuario = data['pergunta']
    
    # Garante que a variável de ambiente PYTHONUNBUFFERED seja passada, se definida.
    # Outras variáveis de ambiente importantes para o spaCy/torch também serão herdadas.
    env_vars = dict(os.environ)

    try:
        process = subprocess.run(
            ['python', PLN_PROCESSOR_SCRIPT_PATH, pergunta_usuario],
            capture_output=True,
            text=True,
            check=False, # Mudamos para False para capturar a saída mesmo se der erro
            cwd=CWD_FOR_PLN,
            env=env_vars
        )
        
        # Tenta parsear a saída JSON do script, seja sucesso ou erro JSON do PLN
        # O pln_processor.py já formata seus erros como JSON e usa sys.exit(0) ou sys.exit(1)
        try:
            resultado_pln = json.loads(process.stdout if process.stdout else process.stderr)
            # Retorna o status code do processo PLN se for um erro estruturado dele
            if process.returncode != 0 and "erro" in resultado_pln:
                 return jsonify(resultado_pln), 400 # ou 500, dependendo da semântica do erro PLN
            elif process.returncode != 0: # Erro não JSON do PLN
                return jsonify({
                    "erro": "PLN falhou com saída não-JSON",
                    "pln_stdout": process.stdout,
                    "pln_stderr": process.stderr,
                    "pln_exit_code": process.returncode
                }), 500
            return jsonify(resultado_pln) # Sucesso
            
        except json.JSONDecodeError:
            # Se a saída não for JSON, retorna como erro genérico do PLN
            return jsonify({
                "erro": "PLN retornou saída não-JSON ou vazia.",
                "pln_stdout": process.stdout,
                "pln_stderr": process.stderr,
                "pln_exit_code": process.returncode
            }), 500

    except Exception as e_geral:
        return jsonify({"erro": f"Erro inesperado no servidor ao chamar PLN: {str(e_geral)}"}), 500

# Para Gunicorn, ele não usa esta parte. Mas é bom para teste local.
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))