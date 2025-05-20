import spacy
import sys
import json
import os
import logging
import re
from difflib import get_close_matches
from datetime import datetime

# --- Configuração do Logging ---
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format, stream=sys.stderr)
logger = logging.getLogger("PLN_Processor")

# --- Constantes e Caminhos ---
script_dir = os.path.dirname(os.path.abspath(__file__))
RESOURCES_DIR = script_dir
logger.info(f"Diretório base dos resources: {RESOURCES_DIR}")

SINONIMOS_PATH = os.path.join(RESOURCES_DIR, "resultado_similaridade.txt")
PERGUNTAS_INTERESSE_PATH = os.path.join(RESOURCES_DIR, "perguntas_de_interesse.txt")
MAPA_EMPRESAS_JSON_PATH = os.path.join(RESOURCES_DIR, "empresa_nome_map.json")

logger.info(f"Caminho dicionário sinônimos: {SINONIMOS_PATH}")
logger.info(f"Caminho perguntas interesse: {PERGUNTAS_INTERESSE_PATH}")
logger.info(f"Caminho mapa empresas JSON: {MAPA_EMPRESAS_JSON_PATH}")

# --- Carregamento de Recursos ---
nlp = None
try:
    logger.info("Carregando modelo spaCy 'pt_core_news_sm'...")
    nlp = spacy.load("pt_core_news_sm")
    logger.info("Modelo spaCy carregado com sucesso.")
except OSError:
    logger.error("Erro CRÍTICO: Modelo spaCy 'pt_core_news_sm' não encontrado. Execute: python -m spacy download pt_core_news_sm")
    sys.exit(1)
except Exception as e:
     logger.error(f"Erro CRÍTICO inesperado ao carregar modelo spaCy: {e}")
     sys.exit(1)

empresa_nome_map = {}
try:
    if not os.path.exists(MAPA_EMPRESAS_JSON_PATH):
         logger.error(f"Erro CRÍTICO: Arquivo de mapa de empresas JSON não encontrado em: {MAPA_EMPRESAS_JSON_PATH}.")
         sys.exit(1)
    with open(MAPA_EMPRESAS_JSON_PATH, 'r', encoding='utf-8') as f:
        logger.info(f"Abrindo mapa JSON de: {MAPA_EMPRESAS_JSON_PATH}")
        empresa_nome_map = json.load(f)
        logger.info(f"Carregados {len(empresa_nome_map)} mapeamentos do JSON.")
        if not empresa_nome_map:
             logger.warning("Mapa de empresas JSON carregado, mas está vazio.")
except json.JSONDecodeError as e:
     logger.error(f"Erro CRÍTICO ao decodificar o JSON do mapa de empresas em {MAPA_EMPRESAS_JSON_PATH}: {e}")
     sys.exit(1)
except Exception as e:
     logger.error(f"Erro CRÍTICO inesperado ao carregar o mapa de empresas JSON: {e}")
     sys.exit(1)

sinonimos_map = {}
linhas_lidas_sinonimos = 0
linhas_validas_sinonimos = 0
try:
    if not os.path.exists(SINONIMOS_PATH):
         logger.warning(f"Arquivo de sinônimos não encontrado em {SINONIMOS_PATH}. Mapeamento de valor pode falhar.")
    else:
        with open(SINONIMOS_PATH, 'r', encoding='utf-8') as f:
            logger.info(f"Processando dicionário de sinônimos '{os.path.basename(SINONIMOS_PATH)}'...")
            for line_num, line in enumerate(f, 1):
                linhas_lidas_sinonimos += 1
                line = line.strip()
                if not line or line.startswith('#'): continue
                parts = line.split(';')
                if len(parts) == 2:
                    keyword = parts[0].strip().lower()
                    valor_ontologia = parts[1].strip()
                    if keyword and valor_ontologia:
                        if keyword in sinonimos_map:
                             logger.warning(f"Sinônimo duplicado encontrado na linha {line_num}: '{keyword}'. Usando o último valor '{valor_ontologia}'.")
                        sinonimos_map[keyword] = valor_ontologia
                        linhas_validas_sinonimos += 1
                    else:
                         logger.debug(f"Linha {line_num} ignorada no dicionário (chave ou valor vazio): {line}")
                else:
                    logger.debug(f"Linha {line_num} ignorada no dicionário (formato inválido - sem ';'): {line}")
            logger.info(f"Dicionário '{os.path.basename(SINONIMOS_PATH)}' carregado: {len(sinonimos_map)} chaves únicas ({linhas_validas_sinonimos} linhas válidas de {linhas_lidas_sinonimos}).")
            if linhas_validas_sinonimos == 0 and linhas_lidas_sinonimos > 0:
                 logger.error("ERRO: Nenhuma linha válida encontrada no dicionário de sinônimos. Verifique o formato 'chave;valor' e o encoding UTF-8.")
except Exception as e:
     logger.error(f"Erro CRÍTICO ao carregar dicionário de sinônimos: {e}")
     sys.exit(1)

# --- Funções Auxiliares ---
def normalizar_texto(texto):
    if not texto: return ""
    mapa_acentos = str.maketrans("áàâãéèêíìîóòôõúùûçÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ", "aaaaeeeiiioooouuucAAAAEEEIIIOOOOUUUC")
    return texto.lower().translate(mapa_acentos)

def extrair_entidades_data(doc):
    entidades_ner = {"ORG": [], "DATE": [], "LOC": [], "MISC": [], "PER": []}
    keywords_valor = []
    data_encontrada = None

    spans = spacy.util.filter_spans(doc.ents)
    for ent in spans:
        tipo = ent.label_
        texto_entidade = ent.text
        if tipo in entidades_ner:
            if texto_entidade not in entidades_ner[tipo]:
                entidades_ner[tipo].append(texto_entidade)
        else:
            logger.debug(f"Tipo NER não esperado: {tipo} ('{texto_entidade}')")

    date_pattern = re.compile(r'\b(\d{1,2}/\d{1,2}/\d{4})\b|\b(\d{4}-\d{1,2}-\d{1,2})\b')
    match = date_pattern.search(doc.text)
    if match:
        date_str = match.group(1) or match.group(2)
        try:
            parsed_date = datetime.strptime(date_str, '%d/%m/%Y') if '/' in date_str else datetime.strptime(date_str, '%Y-%m-%d')
            data_encontrada = parsed_date.strftime('%Y-%m-%d')
            logger.info(f"Data explícita encontrada e validada: '{date_str}' -> {data_encontrada}")
        except ValueError:
            logger.warning(f"Formato de data encontrado ('{date_str}') mas inválido.")
            data_encontrada = None
    else:
         logger.info("Nenhuma data explícita encontrada.")

    texto_pergunta_lower = doc.text.lower() # Normaliza a pergunta inteira uma vez
    found_keywords = set()
    sorted_sinonimos_keys = sorted(sinonimos_map.keys(), key=len, reverse=True)

    for keyword_sinonimo in sorted_sinonimos_keys:
         # Usa re.escape para tratar caracteres especiais no keyword_sinonimo
         # Adiciona \b para garantir que são palavras inteiras
         pattern = r'\b' + re.escape(keyword_sinonimo) + r'\b'
         if re.search(pattern, texto_pergunta_lower):
             found_keywords.add(keyword_sinonimo)
             # Se quiser priorizar o mais longo/específico e parar:
             # break

    keywords_valor = list(found_keywords)

    return entidades_ner, data_encontrada, keywords_valor


def selecionar_template(pergunta_usuario, perguntas_interesse_path):
    pergunta_usuario_norm = normalizar_texto(pergunta_usuario)
    templates_interesse = {}
    perguntas_exemplos_norm = []
    perguntas_originais = []

    try:
        if not os.path.exists(perguntas_interesse_path):
             logger.error(f"Erro CRÍTICO: Arquivo de perguntas de interesse não encontrado em {perguntas_interesse_path}.")
             return None, 0.0
        with open(perguntas_interesse_path, 'r', encoding='utf-8') as f:
            logger.info(f"Arquivo '{os.path.basename(perguntas_interesse_path)}' lido com sucesso usando encoding 'utf-8'.")
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'): continue
                parts = line.split(';', 1)
                if len(parts) == 2:
                    template_id = parts[0].strip()
                    pergunta_exemplo = parts[1].strip()
                    if template_id and pergunta_exemplo:
                         templates_interesse[pergunta_exemplo] = template_id
                         perguntas_exemplos_norm.append(normalizar_texto(pergunta_exemplo))
                         perguntas_originais.append(pergunta_exemplo)
                    else:
                         logger.debug(f"Linha {line_num} ignorada em perguntas_interesse (ID ou pergunta vazia): {line}")
                else:
                    logger.debug(f"Linha {line_num} ignorada em perguntas_interesse (formato inválido - sem ';'): {line}")
            if not templates_interesse:
                 logger.error(f"ERRO: Nenhuma pergunta de interesse válida carregada de {perguntas_interesse_path}. Verifique formato e encoding.")
                 return None, 0.0
    except Exception as e:
         logger.error(f"Erro CRÍTICO ao ler perguntas de interesse: {e}")
         return None, 0.0

    matches = get_close_matches(pergunta_usuario_norm, perguntas_exemplos_norm, n=1, cutoff=0.65)

    if matches:
        best_match_norm = matches[0]
        try:
            index = perguntas_exemplos_norm.index(best_match_norm)
            best_match_original = perguntas_originais[index]
            template_id_selecionado = templates_interesse[best_match_original]
            similaridade_score = len(set(pergunta_usuario_norm.split()) & set(best_match_norm.split())) / float(len(set(pergunta_usuario_norm.split()) | set(best_match_norm.split()))) if len(set(pergunta_usuario_norm.split()) | set(best_match_norm.split())) > 0 else 0
            logger.info(f"Template selecionado: '{template_id_selecionado}' (Baseado na similaridade com: '{best_match_original}', Score Jaccard Aprox: {similaridade_score:.4f})")
            return template_id_selecionado, similaridade_score
        except ValueError:
             logger.error(f"Erro interno: Match normalizado '{best_match_norm}' não encontrado nos índices.")
             return None, 0.0
    else:
        logger.warning(f"Nenhum template similar encontrado para: '{pergunta_usuario}' (cutoff=0.65)")
        return None, 0.0

# *** FUNÇÃO MAPEAMENTO CORRIGIDA PARA O CASO DA VALE ***
def mapear_placeholders(entidades_ner, data_encontrada, keywords_valor, template_id):
    mapeamentos = {}
    tipo_entidade_final = "N/A"

    if data_encontrada:
        mapeamentos["#DATA#"] = data_encontrada
        logger.info(f"  - Placeholder #DATA# mapeado para: '{data_encontrada}'")

    placeholder_entidade_valor = None
    entidade_origem_texto = None # Texto original da entidade NER ORG

    if entidades_ner.get("ORG"):
        entidade_origem_texto = entidades_ner["ORG"][0]
        chave_mapa_entidade = entidade_origem_texto.upper().replace(" ", "") # Ex: "VALE", "GERDAU", "CSN", "CBAV3"

        if chave_mapa_entidade in empresa_nome_map:
            map_value_list = empresa_nome_map[chave_mapa_entidade]

            if isinstance(map_value_list, list) and len(map_value_list) > 0:
                primeiro_valor_mapeado = map_value_list[0] # Ex: "VALE_LABEL" ou "CMIN3"

                if template_id == "Template 2A":
                    # Para Template 2A, queremos a LABEL da empresa
                    if primeiro_valor_mapeado.endswith("_LABEL"):
                        # A chave é a label que queremos (ex: VALE, GERDAU)
                        placeholder_entidade_valor = chave_mapa_entidade
                        tipo_entidade_final = "LABEL (de _LABEL para T2A)"
                        logger.info(f"  - Placeholder #ENTIDADE_NOME# (T2A) mapeado para LABEL: '{placeholder_entidade_valor}' (Origem NER: '{entidade_origem_texto}')")
                    else:
                        # A entidade NER já era um ticker ou mapeou para um ticker.
                        # Se o usuário perguntou sobre um ticker no Template 2A, idealmente buscaríamos a empresa desse ticker.
                        # Por simplicidade, se não for uma _LABEL, vamos usar o primeiro ticker como "nome" para a query.
                        # Isso pode não ser ideal, mas evita quebrar se a entrada for um ticker.
                        placeholder_entidade_valor = primeiro_valor_mapeado
                        tipo_entidade_final = "TICKER (Mapeado para T2A - tentativa de usar como label)"
                        logger.warning(f"  - Placeholder #ENTIDADE_NOME# (T2A) para '{entidade_origem_texto}' resultou em Ticker '{placeholder_entidade_valor}'. Template 2A espera uma label.")
                else: # Para templates como 1A, 1B (que precisam de um Ticker)
                    if primeiro_valor_mapeado.endswith("_LABEL"):
                        # Mapeou para uma Label, mas precisamos do Ticker.
                        logger.info(f"Entidade '{entidade_origem_texto}' (chave: {chave_mapa_entidade}) mapeada para LABEL, mas template '{template_id}' espera Ticker. Derivando ticker...")
                        # Tenta buscar o ticker principal. Ex: se chave_mapa_entidade é "VALE", tenta "VALE3"
                        ticker_principal_chave = chave_mapa_entidade + "3"
                        if ticker_principal_chave in empresa_nome_map and \
                           isinstance(empresa_nome_map[ticker_principal_chave], list) and \
                           len(empresa_nome_map[ticker_principal_chave]) > 0:
                            placeholder_entidade_valor = empresa_nome_map[ticker_principal_chave][0]
                            tipo_entidade_final = "TICKER (Derivado da Label via Chave Ticker+3)"
                            logger.info(f"  - Placeholder #ENTIDADE_NOME# (para {template_id}) mapeado para TICKER derivado: '{placeholder_entidade_valor}' (Origem NER: '{entidade_origem_texto}')")
                        else:
                            # Se não achou com +3, tenta com +4 (para PN)
                            ticker_principal_chave_alt4 = chave_mapa_entidade + "4"
                            if ticker_principal_chave_alt4 in empresa_nome_map and \
                               isinstance(empresa_nome_map[ticker_principal_chave_alt4], list) and \
                               len(empresa_nome_map[ticker_principal_chave_alt4]) > 0:
                                placeholder_entidade_valor = empresa_nome_map[ticker_principal_chave_alt4][0]
                                tipo_entidade_final = "TICKER (Derivado da Label via Chave Ticker+4)"
                                logger.info(f"  - Placeholder #ENTIDADE_NOME# (para {template_id}) mapeado para TICKER derivado (alt4): '{placeholder_entidade_valor}' (Origem NER: '{entidade_origem_texto}')")
                            else:
                                # Se não achou nem com +3 nem +4, loga erro
                                logger.error(f"Não foi possível derivar ticker para a label '{chave_mapa_entidade}'. Verifique o mapeamento JSON se possui entrada para o ticker principal (ex: {ticker_principal_chave}).")
                                placeholder_entidade_valor = entidade_origem_texto # Último recurso, pode falhar na query
                                tipo_entidade_final = "FALHA_DERIVAR_TICKER_PARA_LABEL"
                    else:
                        # O valor mapeado já é uma lista de tickers, pega o primeiro
                        placeholder_entidade_valor = primeiro_valor_mapeado
                        tipo_entidade_final = "TICKER (Mapeado direto)"
                        logger.info(f"  - Placeholder #ENTIDADE_NOME# (para {template_id}) mapeado para TICKER: '{placeholder_entidade_valor}' (Origem NER: '{entidade_origem_texto}', Lista: {map_value_list})")
            else:
                 logger.warning(f"Mapeamento encontrado para '{chave_mapa_entidade}' mas lista vazia ou formato inválido: {map_value_list}")
                 placeholder_entidade_valor = entidade_origem_texto
                 tipo_entidade_final = "NER ORG (Falha Mapeamento Formato)"
        elif re.match(r"^[A-Z]{4}\d{1,2}$", chave_mapa_entidade): # A própria ORG detectada é um Ticker
             placeholder_entidade_valor = chave_mapa_entidade
             tipo_entidade_final = "TICKER (NER ORG é Ticker)"
             logger.info(f"  - Placeholder #ENTIDADE_NOME# mapeado para TICKER: '{placeholder_entidade_valor}' (Origem NER: '{entidade_origem_texto}', Detectado como Ticker)")
        else:
            # Entidade ORG não está no mapa e não é ticker
            logger.warning(f"Entidade ORG '{entidade_origem_texto}' não encontrada no mapa JSON e não é Ticker. Usando texto original.")
            placeholder_entidade_valor = entidade_origem_texto # Usa o texto original da entidade
            tipo_entidade_final = "NER ORG (Não Mapeado)"
    else:
         logger.info("Nenhuma entidade ORG encontrada para #ENTIDADE_NOME#.")

    if placeholder_entidade_valor:
        mapeamentos["#ENTIDADE_NOME#"] = placeholder_entidade_valor
    else:
         logger.warning("Placeholder #ENTIDADE_NOME# não pôde ser mapeado.")

    valor_mapeado = None
    keyword_origem = None
    keyword_encontrada_para_valor = None
    keywords_valor_sorted = sorted([kw for kw in keywords_valor if kw in sinonimos_map], key=len, reverse=True)

    if keywords_valor_sorted:
        keyword_encontrada_para_valor = keywords_valor_sorted[0]
        valor_mapeado = sinonimos_map[keyword_encontrada_para_valor]
        keyword_origem = keyword_encontrada_para_valor
        logger.info(f"  - Placeholder #VALOR_DESEJADO# mapeado para: '{valor_mapeado}' (Keyword encontrada: '{keyword_origem}')")
        mapeamentos["#VALOR_DESEJADO#"] = valor_mapeado
    else:
        logger.info("Nenhuma keyword da pergunta foi encontrada no dicionário de sinônimos para #VALOR_DESEJADO#.")

    logger.info(f"Mapeamentos finais encontrados: {mapeamentos}")
    return mapeamentos, tipo_entidade_final

def main(pergunta):
    logger.info(f"Pergunta do usuário recebida: '{pergunta}'")

    if not nlp:
        logger.error("Modelo NLP não carregado.")
        return {"erro": "Serviço de PLN indisponível."}
    doc = nlp(pergunta)
    entidades_ner, data_encontrada, keywords_valor = extrair_entidades_data(doc)
    logger.info(f"  NER (spaCy - final agrupado): {entidades_ner}")
    logger.info(f"  Keywords para valor: {keywords_valor}")
    logger.info("Análise NLP concluída.")

    template_id, similaridade = selecionar_template(pergunta, PERGUNTAS_INTERESSE_PATH)
    if not template_id:
        logger.error("Não foi possível selecionar um template adequado.")
        return {"erro": "Não foi possível entender o tipo da pergunta."}
    logger.info(f"Template selecionado: '{template_id}' (Similaridade estimada: {similaridade:.4f}).")

    mapeamentos, tipo_entidade_detectada = mapear_placeholders(entidades_ner, data_encontrada, keywords_valor, template_id)
    if mapeamentos is None:
         return {"erro": f"Erro durante mapeamento de placeholders ({tipo_entidade_detectada})."}
    logger.info("Mapeamento de placeholders concluído.")

    placeholders_requeridos_mock = {
         "Template 1A": ["#DATA#", "#ENTIDADE_NOME#", "#VALOR_DESEJADO#"],
         "Template 1B": ["#DATA#", "#ENTIDADE_NOME#", "#VALOR_DESEJADO#"],
         "Template 2A": ["#ENTIDADE_NOME#", "#VALOR_DESEJADO#"]
    }
    requeridos = set(placeholders_requeridos_mock.get(template_id, []))
    mapeados_keys = set(mapeamentos.keys())
    faltando = requeridos - mapeados_keys

    # Validação mais rigorosa para placeholders essenciais
    essenciais_faltando = []
    if template_id in ["Template 1A", "Template 1B"]:
        if "#ENTIDADE_NOME#" not in mapeados_keys: essenciais_faltando.append("#ENTIDADE_NOME#")
        if "#DATA#" not in mapeados_keys: essenciais_faltando.append("#DATA#")
        if "#VALOR_DESEJADO#" not in mapeados_keys: essenciais_faltando.append("#VALOR_DESEJADO#")
    elif template_id == "Template 2A":
        if "#ENTIDADE_NOME#" not in mapeados_keys: essenciais_faltando.append("#ENTIDADE_NOME#")
        # #VALOR_DESEJADO# (codigo) é esperado, mas a query 2A ainda funciona sem ele se focarmos na entidade.
        # Adicionaremos à lista de faltantes, mas pode não ser um erro fatal dependendo da query.
        if "#VALOR_DESEJADO#" not in mapeados_keys: logger.warning("Placeholder #VALOR_DESEJADO# (esperado 'codigo') não mapeado para Template 2A.")


    if essenciais_faltando:
         logger.error(f"Validação FALHOU para {template_id}: Placeholders essenciais faltando: {essenciais_faltando}")
         return {"erro": f"Informação faltando para processar a pergunta: {', '.join(essenciais_faltando)}"}
    else:
         logger.info(f"Validação de placeholders OK para template '{template_id}'.")

    resposta_json = {
        "template_nome": template_id,
        "mapeamentos": mapeamentos,
        "_debug_info": {
             "pergunta_original": pergunta,
             "pergunta_normalizada": normalizar_texto(pergunta),
             "nlp_ner": entidades_ner,
             "nlp_data": data_encontrada,
             "nlp_keywords_valor": keywords_valor,
             "template_similaridade": round(similaridade, 4),
             "placeholders_requeridos": list(requeridos), # Mostra os idealmente requeridos
             "tipo_entidade_final": tipo_entidade_detectada
         }
    }
    print(json.dumps(resposta_json, ensure_ascii=False))
    logger.info("Resposta JSON enviada com sucesso.")
    return resposta_json

if __name__ == "__main__":
    pid = os.getpid()
    logger.info(f"--- INICIANDO EXECUÇÃO PLN_PROCESSOR.PY (PID: {pid}) ---")
    logger.info(f"--- Argumentos Recebidos: {sys.argv} ---")

    if len(sys.argv) > 1:
        try:
            decoded_args = [arg.encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding) if hasattr(sys.stdout, 'encoding') and sys.stdout.encoding else arg for arg in sys.argv[1:]]
            pergunta_do_usuario = " ".join(decoded_args)
        except Exception as decode_err:
             logger.warning(f"Erro ao decodificar argumentos da linha de comando, usando raw: {decode_err}")
             pergunta_do_usuario = " ".join(sys.argv[1:])
        try:
            resultado = main(pergunta_do_usuario)
            if isinstance(resultado, dict) and "erro" in resultado:
                 logger.error(f"Processamento falhou com erro interno: {resultado['erro']}")
                 print(json.dumps(resultado, ensure_ascii=False))
                 sys.exit(1)
            elif isinstance(resultado, dict):
                logger.info("--- Processamento PLN Script concluído com sucesso (status 0) ---")
                sys.exit(0)
            else:
                 logger.error("Função main retornou valor inesperado.")
                 print(json.dumps({"erro": "Erro interno inesperado no script Python."}, ensure_ascii=False))
                 sys.exit(1)
        except SystemExit as e:
             raise e
        except Exception as e:
             logger.exception("Erro crítico não tratado durante a execução principal.")
             print(json.dumps({"erro": f"Erro crítico no servidor Python: {str(e)}"}, ensure_ascii=False))
             sys.exit(1)
    else:
        logger.error("Erro: Nenhuma pergunta fornecida como argumento.")
        print(json.dumps({"erro": "Nenhuma pergunta fornecida."}))
        sys.exit(1)