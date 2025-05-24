from flask import Flask, request, jsonify, send_from_directory
import subprocess
import json
import os
from rdflib import Graph
from rdflib.plugins.sparql import prepareQuery
import logging
import sys # <--- ADICIONE ESTA LINHA

# Configuração do logging do Flask (faça isso ANTES de criar o 'app')
# Para ver logs no Render, eles precisam ir para stdout/stderr
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Se quiser logs mais detalhados durante o desenvolvimento, mude para logging.DEBUG
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

app = Flask(__name__, static_folder='src/main/resources/static')
# Define um logger específico para a aplicação Flask para facilitar o rastreamento
flask_logger = logging.getLogger('flask.app') # Usado em vez de app.logger para consistência com a config acima

# --- CONFIGURAÇÕES DE CAMINHO DENTRO DO CONTAINER ---
BASE_APP_DIR = "/app" # WORKDIR do Dockerfile

# Caminho para o script pln_processor.py
PLN_PROCESSOR_SCRIPT_PATH = os.path.join(BASE_APP_DIR, "src", "main", "resources", "pln_processor.py")
# Diretório de trabalho para o pln_processor.py (onde ele espera seus .txt, .json)
CWD_FOR_PLN = os.path.join(BASE_APP_DIR, "src", "main", "resources")

# Diretório dos templates SPARQL (.txt)
SPARQL_TEMPLATES_DIR = os.path.join(BASE_APP_DIR, "src", "main", "resources", "Templates")

# Caminho para o arquivo de ontologia principal (agora na raiz /app)
ONTOLOGY_FILE_PATH = os.path.join(BASE_APP_DIR, "ontologiaB3.ttl")

# Carregar a ontologia uma vez quando a aplicação inicia
graph = Graph()
if os.path.exists(ONTOLOGY_FILE_PATH):
    flask_logger.info(f"Carregando ontologia de: {ONTOLOGY_FILE_PATH}")
    try:
        graph.parse(ONTOLOGY_FILE_PATH, format="turtle")
        flask_logger.info(f"Ontologia carregada com {len(graph)} triplas.")
    except Exception as e:
        flask_logger.error(f"Erro CRÍTICO ao carregar ontologia: {e}. A aplicação pode não funcionar corretamente.")
else:
    flask_logger.error(f"ARQUIVO DE ONTOLOGIA NÃO ENCONTRADO EM: {ONTOLOGY_FILE_PATH}. As consultas SPARQL falharão.")


# --- ROTA PARA SERVIR O HTML DA INTERFACE PRINCIPAL ---
@app.route('/', methods=['GET'])
def index():
    flask_logger.info(f"Tentando servir: {app.static_folder} / index2.html")
    try:
        return send_from_directory(app.static_folder, 'index2.html')
    except Exception as e:
        flask_logger.error(f"Erro ao tentar servir o index2.html: {e}")
        return "Erro ao carregar a interface principal. Verifique os logs do servidor.", 500

# --- ENDPOINT PRINCIPAL DE PROCESSAMENTO ---
@app.route('/processar_pergunta', methods=['POST'])
def processar_pergunta_completa():
    data = request.get_json()
    if not data or 'pergunta' not in data:
        return jsonify({"erro": "Pergunta não fornecida no corpo JSON", "sparqlQuery": "N/A"}), 400
    
    pergunta_usuario = data['pergunta']
    flask_logger.info(f"Recebida pergunta: '{pergunta_usuario}'")

    # 1. Chamar o PLN Processor
    pln_output_json = None
    output_str_pln = ""
    try:
        flask_logger.info(f"Chamando PLN: python {PLN_PROCESSOR_SCRIPT_PATH} '{pergunta_usuario}' com CWD: {CWD_FOR_PLN}")
        process_pln = subprocess.run(
            ['python', PLN_PROCESSOR_SCRIPT_PATH, pergunta_usuario],
            capture_output=True,
            text=True,
            check=False, 
            cwd=CWD_FOR_PLN,
            env=dict(os.environ)
        )
        
        output_str_pln = process_pln.stdout if process_pln.stdout.strip() else process_pln.stderr
        flask_logger.debug(f"Saída bruta PLN (stdout): {process_pln.stdout[:500]}...")
        flask_logger.debug(f"Saída bruta PLN (stderr): {process_pln.stderr[:500]}...")
        flask_logger.info(f"Código de saída do PLN: {process_pln.returncode}")

        if not output_str_pln.strip():
            flask_logger.error("PLN não produziu saída (stdout/stderr).")
            return jsonify({"erro": "PLN não produziu saída.", "sparqlQuery": "N/A (Erro no PLN)"}), 500

        pln_output_json = json.loads(output_str_pln)
        
        if "erro" in pln_output_json:
            flask_logger.error(f"Erro estruturado retornado pelo PLN: {pln_output_json['erro']}")
            return jsonify(pln_output_json), 400

        if "template_nome" not in pln_output_json or "mapeamentos" not in pln_output_json:
            flask_logger.error(f"Saída do PLN inesperada (faltando template_nome ou mapeamentos): {pln_output_json}")
            return jsonify({"erro": "Saída do PLN inválida ou incompleta.", "sparqlQuery": "N/A"}), 500

    except json.JSONDecodeError as jde:
        flask_logger.error(f"Erro ao decodificar JSON do PLN: {jde}. Saída PLN que causou erro: {output_str_pln}")
        return jsonify({"erro": "Erro ao decodificar saída do PLN.", "sparqlQuery": "N/A (Erro no PLN)", "debug_pln_output": output_str_pln}), 500
    except Exception as e_pln:
        flask_logger.error(f"Erro genérico ao executar PLN: {e_pln}", exc_info=True)
        return jsonify({"erro": f"Erro crítico ao executar o processador PLN: {str(e_pln)}", "sparqlQuery": "N/A (Erro no PLN)"}), 500

    template_nome = pln_output_json.get("template_nome")
    mapeamentos = pln_output_json.get("mapeamentos", {})
    flask_logger.info(f"PLN retornou: template='{template_nome}', mapeamentos='{mapeamentos}'")

    # 2. Carregar e Preencher Template SPARQL
    sparql_query_string_final = "Consulta SPARQL não pôde ser gerada."
    sparql_query_template_content = "Template SPARQL não carregado."
    try:
        template_filename = f"{template_nome.replace(' ', '_')}.txt"
        template_file_path = os.path.join(SPARQL_TEMPLATES_DIR, template_filename)
        flask_logger.info(f"Tentando carregar template SPARQL de: {template_file_path}")

        if not os.path.exists(template_file_path):
            flask_logger.error(f"Arquivo de template SPARQL não encontrado: {template_file_path}")
            return jsonify({"erro": f"Template SPARQL '{template_filename}' não encontrado.", "sparqlQuery": "N/A"}), 500

        with open(template_file_path, 'r', encoding='utf-8') as f_template:
            sparql_query_template_content = f_template.read()
        
        sparql_query_string_final = sparql_query_template_content
        
        for placeholder_key, valor_raw in mapeamentos.items():
            valor_sparql_formatado = ""
            valor_str_raw = str(valor_raw)

            if placeholder_key == "#DATA#":
                valor_sparql_formatado = f'"{valor_str_raw}"^^xsd:date'
            elif placeholder_key == "#ENTIDADE_NOME#":
                valor_escapado = valor_str_raw.replace('\\', '\\\\').replace('"', '\\"')
                valor_sparql_formatado = f'"{valor_escapado}"'
            elif placeholder_key == "#VALOR_DESEJADO#":
                if ":" not in valor_str_raw and not valor_str_raw.startswith("<"):
                    valor_sparql_formatado = f'b3:{valor_str_raw}'
                else:
                    valor_sparql_formatado = valor_str_raw
            elif placeholder_key == "#SETOR#":
                valor_escapado = valor_str_raw.replace('\\', '\\\\').replace('"', '\\"')
                valor_sparql_formatado = f'"{valor_escapado}"'
            else:
                flask_logger.warning(f"Placeholder não tratado explicitamente '{placeholder_key}'. Usando como string literal.")
                valor_escapado = valor_str_raw.replace('\\', '\\\\').replace('"', '\\"')
                valor_sparql_formatado = f'"{valor_escapado}"'

            flask_logger.info(f"Substituindo '{placeholder_key}' por '{valor_sparql_formatado}' no template SPARQL.")
            sparql_query_string_final = sparql_query_string_final.replace(str(placeholder_key), valor_sparql_formatado)
        
        flask_logger.info(f"Consulta SPARQL final gerada: \n{sparql_query_string_final}")

    except Exception as e_template:
        flask_logger.error(f"Erro ao processar template SPARQL: {e_template}", exc_info=True)
        return jsonify({"erro": f"Erro ao gerar consulta SPARQL: {str(e_template)}", "sparqlQuery": sparql_query_template_content}), 500

    # 3. Executar Consulta SPARQL
    resposta_formatada_final = "Não foi possível executar a consulta ou não houve resultados."
    try:
        if not graph or len(graph) == 0:
            flask_logger.error("Ontologia não carregada ou vazia, não é possível executar a consulta.")
            return jsonify({"erro": "Falha ao carregar a ontologia base ou está vazia.", "sparqlQuery": sparql_query_string_final}), 500
        
        query_obj = prepareQuery(sparql_query_string_final)
        
        flask_logger.info("Executando consulta SPARQL...")
        qres = graph.query(query_obj)
        
        resultados_temp_list = []
        if qres.type == 'SELECT':
            for row in qres:
                try:
                    resultados_temp_list.append(row.asdict())
                except AttributeError: 
                    result_row_dict = {}
                    for i, var_name in enumerate(qres.vars):
                        result_row_dict[str(var_name)] = str(row[i]) if row[i] is not None else None
                    resultados_temp_list.append(result_row_dict)

            if not resultados_temp_list:
                resposta_formatada_final = "Nenhum resultado encontrado."
            else:
                resposta_formatada_final = json.dumps(resultados_temp_list)
        
        elif qres.type == 'ASK':
            resposta_formatada_final = json.dumps({"resultado_ask": bool(qres.askAnswer)})
        
        elif qres.type == 'CONSTRUCT' or qres.type == 'DESCRIBE':
            resposta_formatada_final = qres.serialize(format='turtle')
            if not resposta_formatada_final.strip():
                resposta_formatada_final = "Nenhum resultado para CONSTRUCT/DESCRIBE."
        else:
            resposta_formatada_final = f"Tipo de consulta não suportado para formatação padrão: {qres.type}"

        flask_logger.info(f"Consulta SPARQL executada. Tipo de resultado: {qres.type}. Resposta (início): {str(resposta_formatada_final)[:200]}...")

    except Exception as e_sparql:
        flask_logger.error(f"Erro ao executar consulta SPARQL: {e_sparql}", exc_info=True)
        flask_logger.error(f"Consulta que falhou: \n{sparql_query_string_final}")
        return jsonify({"erro": f"Erro ao executar consulta SPARQL: {str(e_sparql)}", "sparqlQuery": sparql_query_string_final}), 500

    # 4. Retornar tudo para o frontend
    return jsonify({
        "sparqlQuery": sparql_query_string_final,
        "resposta": resposta_formatada_final 
    })

if __name__ == '__main__':
    local_port = int(os.environ.get("PORT", 5001)) 
    flask_logger.info(f"Iniciando servidor Flask de desenvolvimento em http://0.0.0.0:{local_port}")
    app.run(host='0.0.0.0', port=local_port, debug=True)