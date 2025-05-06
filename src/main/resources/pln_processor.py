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
# Log para stderr para ser capturado pelo Java ProcessBuilder
logging.basicConfig(level=logging.INFO, format=log_format, stream=sys.stderr)
logger = logging.getLogger("PLN_Processor")

# --- Constantes e Caminhos ---
# Determina o diretório base (onde o script está localizado)
script_dir = os.path.dirname(os.path.abspath(__file__))
RESOURCES_DIR = script_dir
logger.info(f"Diretório base dos resources: {RESOURCES_DIR}")

SINONIMOS_PATH = os.path.join(RESOURCES_DIR, "resultado_similaridade.txt")
PERGUNTAS_INTERESSE_PATH = os.path.join(RESOURCES_DIR, "perguntas_de_interesse.txt")
MAPA_EMPRESAS_JSON_PATH = os.path.join(RESOURCES_DIR, "empresa_nome_map.json")

logger.info(f"Caminho dicionário sinônimos: {SINONIMOS_PATH}")
logger.info(f"Caminho perguntas interesse: {PERGUNTAS_INTERESSE_PATH}")
logger.info(f"Caminho mapa empresas JSON: {MAPA_EMPRESAS_JSON_PATH}")

# --- Carregamento de Recursos (com tratamento de erro robusto) ---

# Carrega o modelo spaCy
nlp = None
try:
    logger.info("Carregando modelo spaCy 'pt_core_news_sm'...")
    nlp = spacy.load("pt_core_news_sm")
    logger.info("Modelo spaCy carregado com sucesso.")
except OSError:
    logger.error("Erro CRÍTICO: Modelo spaCy 'pt_core_news_sm' não encontrado.")
    logger.error("Execute: python -m spacy download pt_core_news_sm")
    sys.exit(1) # Encerra imediatamente se o modelo não estiver disponível
except Exception as e:
     logger.error(f"Erro CRÍTICO inesperado ao carregar modelo spaCy: {e}")
     sys.exit(1) # Encerra

# Carrega mapa de empresas
empresa_nome_map = {}
try:
    if not os.path.exists(MAPA_EMPRESAS_JSON_PATH):
         logger.error(f"Erro CRÍTICO: Arquivo de mapa de empresas JSON não encontrado em: {MAPA_EMPRESAS_JSON_PATH}.")
         sys.exit(1) # Essencial, encerra se não encontrar
    with open(MAPA_EMPRESAS_JSON_PATH, 'r', encoding='utf-8') as f: # Garante UTF-8
        logger.info(f"Abrindo mapa JSON de: {MAPA_EMPRESAS_JSON_PATH}")
        empresa_nome_map = json.load(f)
        logger.info(f"Carregados {len(empresa_nome_map)} mapeamentos do JSON.")
        if not empresa_nome_map:
             logger.warning("Mapa de empresas JSON carregado, mas está vazio.")
except json.JSONDecodeError as e:
     logger.error(f"Erro CRÍTICO ao decodificar o JSON do mapa de empresas em {MAPA_EMPRESAS_JSON_PATH}: {e}")
     sys.exit(1) # JSON inválido é crítico
except Exception as e:
     logger.error(f"Erro CRÍTICO inesperado ao carregar o mapa de empresas JSON: {e}")
     sys.exit(1)

# Carrega dicionário de sinônimos
sinonimos_map = {}
linhas_lidas_sinonimos = 0
linhas_validas_sinonimos = 0
try:
    if not os.path.exists(SINONIMOS_PATH):
         logger.warning(f"Arquivo de sinônimos não encontrado em {SINONIMOS_PATH}. Mapeamento de valor pode falhar.")
    else:
        with open(SINONIMOS_PATH, 'r', encoding='utf-8') as f: # Garante UTF-8
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
                 # Decide se sai ou continua. É crítico para templates 1A/1B.
                 # sys.exit(1)

except Exception as e:
     logger.error(f"Erro CRÍTICO ao carregar dicionário de sinônimos: {e}")
     sys.exit(1) # Dicionário é essencial para templates 1A/1B


# --- Funções Auxiliares ---

def normalizar_texto(texto):
    """Remove acentos e converte para minúsculas."""
    if not texto: return ""
    mapa_acentos = str.maketrans("áàâãéèêíìîóòôõúùûçÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ", "aaaaeeeiiioooouuucAAAAEEEIIIOOOOUUUC")
    return texto.lower().translate(mapa_acentos)

def extrair_entidades_data(doc):
    """Extrai entidades ORG, datas e keywords relevantes usando spaCy."""
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

    # Extração de keywords para #VALOR_DESEJADO#
    texto_pergunta_lower = doc.text.lower()
    found_keywords = set()

    # Busca por correspondência exata das chaves do dicionário na pergunta
    # Isso é mais robusto do que tokenizar e verificar cada token
    # Ordena as chaves do dicionário pela mais longa primeiro para priorizar termos compostos
    sorted_sinonimos_keys = sorted(sinonimos_map.keys(), key=len, reverse=True)

    for keyword_sinonimo in sorted_sinonimos_keys:
         # Usa limites de palavra (\b) para evitar matches parciais dentro de outras palavras
         pattern = r'\b' + re.escape(keyword_sinonimo) + r'\b'
         if re.search(pattern, texto_pergunta_lower):
             found_keywords.add(keyword_sinonimo)
             # Opcional: parar no primeiro match se quiser apenas a keyword mais específica/longa
             # break

    keywords_valor = list(found_keywords) # Mantém todas as keywords encontradas para debug

    return entidades_ner, data_encontrada, keywords_valor


def selecionar_template(pergunta_usuario, perguntas_interesse_path):
    """Seleciona o template mais similar à pergunta do usuário."""
    pergunta_usuario_norm = normalizar_texto(pergunta_usuario)
    templates_interesse = {}
    perguntas_exemplos_norm = []
    perguntas_originais = []

    try:
        if not os.path.exists(perguntas_interesse_path):
             logger.error(f"Erro CRÍTICO: Arquivo de perguntas de interesse não encontrado em {perguntas_interesse_path}.")
             return None, 0.0 # Retorna None para indicar falha
        with open(perguntas_interesse_path, 'r', encoding='utf-8') as f: # Garante UTF-8
            logger.info(f"Arquivo '{os.path.basename(perguntas_interesse_path)}' lido com sucesso usando encoding 'utf-8'.")
            linhas_lidas = 0
            for line in f:
                linhas_lidas += 1
                line = line.strip()
                if not line or line.startswith('#'): continue
                parts = line.split(';', 1) # Divide apenas no primeiro ';'
                if len(parts) == 2:
                    template_id = parts[0].strip()
                    pergunta_exemplo = parts[1].strip()
                    if template_id and pergunta_exemplo:
                         templates_interesse[pergunta_exemplo] = template_id
                         perguntas_exemplos_norm.append(normalizar_texto(pergunta_exemplo))
                         perguntas_originais.append(pergunta_exemplo)
                    else:
                         logger.debug(f"Linha ignorada em perguntas_interesse (ID ou pergunta vazia): {line}")
                else:
                    logger.debug(f"Linha ignorada em perguntas_interesse (formato inválido - sem ';'): {line}")

            if not templates_interesse:
                 logger.error(f"ERRO: Nenhuma pergunta de interesse válida carregada de {perguntas_interesse_path}. Verifique formato e encoding.")
                 return None, 0.0

    except Exception as e:
         logger.error(f"Erro CRÍTICO ao ler perguntas de interesse: {e}")
         return None, 0.0

    # Aumenta o número de matches retornados e ajusta o cutoff
    matches = get_close_matches(pergunta_usuario_norm, perguntas_exemplos_norm, n=3, cutoff=0.6) # Pega os 3 melhores

    if matches:
        best_match_norm = matches[0] # Usa o melhor match
        try:
            index = perguntas_exemplos_norm.index(best_match_norm)
            best_match_original = perguntas_originais[index]
            template_id_selecionado = templates_interesse[best_match_original]
            # Calcula uma pontuação de similaridade simples (exemplo)
            similaridade = len(set(pergunta_usuario_norm.split()) & set(best_match_norm.split())) / len(set(pergunta_usuario_norm.split()) | set(best_match_norm.split()))

            logger.info(f"Template selecionado: '{template_id_selecionado}' (Baseado na similaridade com: '{best_match_original}', Score Aprox: {similaridade:.4f})")
            return template_id_selecionado, similaridade
        except ValueError:
             logger.error(f"Erro interno: Match normalizado '{best_match_norm}' não encontrado nos índices.")
             return None, 0.0
    else:
        logger.warning(f"Nenhum template similar encontrado para: '{pergunta_usuario}' (cutoff=0.6)")
        return None, 0.0


# *** FUNÇÃO MAPEAMENTO CORRIGIDA ***
def mapear_placeholders(entidades_ner, data_encontrada, keywords_valor, template_id):
    """Mapeia os placeholders com base nas informações extraídas e no template selecionado."""
    mapeamentos = {}
    tipo_entidade_final = "N/A" # Para debug

    # 1. Mapear #DATA#
    if data_encontrada:
        mapeamentos["#DATA#"] = data_encontrada
        logger.info(f"  - Placeholder #DATA# mapeado para: '{data_encontrada}'")

    # 2. Mapear #ENTIDADE_NOME# - Lógica depende do template
    placeholder_entidade_valor = None
    entidade_origem = None

    if entidades_ner.get("ORG"):
        primeira_org = entidades_ner["ORG"][0]
        entidade_origem = primeira_org
        chave_mapa_normalizada = primeira_org.upper().replace(" ", "")

        if chave_mapa_normalizada in empresa_nome_map:
            map_value = empresa_nome_map[chave_mapa_normalizada]

            if isinstance(map_value, list) and len(map_value) > 0:
                is_label_entry = map_value[0].endswith("_LABEL")

                if template_id == "Template 2A":
                    # Template 2A sempre quer a LABEL se a entrada for um nome mapeado para _LABEL
                    if is_label_entry:
                        # Retorna a chave original normalizada (que deve ser a label correta se o JSON estiver certo)
                        # Ex: Se a chave for "GERDAU", retorna "GERDAU". Se for "VALE", retorna "VALE".
                        placeholder_entidade_valor = chave_mapa_normalizada
                        tipo_entidade_final = "LABEL (de _LABEL para T2A)"
                        logger.info(f"  - Placeholder #ENTIDADE_NOME# (T2A) mapeado para LABEL: '{placeholder_entidade_valor}' (Origem NER: '{entidade_origem}')")
                    else:
                        # Se a chave mapeia para tickers, mas o template é 2A, o que fazer?
                        # Decisão: Usar o primeiro ticker. Buscar tickers de um ticker específico não é comum.
                        placeholder_entidade_valor = map_value[0]
                        tipo_entidade_final = "TICKER (Mapeado para T2A - Incomum)"
                        logger.warning(f"  - Placeholder #ENTIDADE_NOME# (T2A) mapeado para TICKER: '{placeholder_entidade_valor}' (Origem NER: '{entidade_origem}'). Isso pode não ser o esperado para Template 2A.")

                else: # Para templates como 1A, 1B, etc., que precisam de um Ticker
                    if is_label_entry:
                         # É uma label, precisamos derivar o ticker principal.
                         logger.info(f"Entidade '{primeira_org}' mapeada para LABEL, mas template '{template_id}' espera Ticker. Derivando ticker...")
                         # Estratégia: Assumir que a chave do ticker principal existe no mapa.
                         # Ex: Se a chave da label é "VALE", procura pela chave "VALE3".
                         ticker_principal_key = chave_mapa_normalizada.replace("_LABEL", "") + "3" # Tenta VALE3, GGBR3 etc.
                         # Tentativa alternativa: ticker ON pode ser chave + 3, PN pode ser chave + 4
                         ticker_principal_key_alt = chave_mapa_normalizada.replace("_LABEL", "") + "4"

                         if ticker_principal_key in empresa_nome_map and isinstance(empresa_nome_map[ticker_principal_key], list):
                              placeholder_entidade_valor = empresa_nome_map[ticker_principal_key][0] # Pega o valor (que é o próprio ticker)
                              tipo_entidade_final = "TICKER (Derivado da Label via +3)"
                              logger.info(f"  - Placeholder #ENTIDADE_NOME# (T1A/B) mapeado para TICKER derivado: '{placeholder_entidade_valor}' (Origem NER: '{entidade_origem}')")
                         elif ticker_principal_key_alt in empresa_nome_map and isinstance(empresa_nome_map[ticker_principal_key_alt], list):
                              placeholder_entidade_valor = empresa_nome_map[ticker_principal_key_alt][0]
                              tipo_entidade_final = "TICKER (Derivado da Label via +4)"
                              logger.info(f"  - Placeholder #ENTIDADE_NOME# (T1A/B) mapeado para TICKER derivado (alt): '{placeholder_entidade_valor}' (Origem NER: '{entidade_origem}')")
                         else:
                              # Fallback: Verifica se a própria chave da label também mapeia para uma lista de tickers (redundância no JSON)
                              label_key_direct = chave_mapa_normalizada.replace("_LABEL", "")
                              if label_key_direct in empresa_nome_map and isinstance(empresa_nome_map[label_key_direct], list) and len(empresa_nome_map[label_key_direct]) > 0 and not empresa_nome_map[label_key_direct][0].endswith("_LABEL"):
                                   placeholder_entidade_valor = empresa_nome_map[label_key_direct][0] # Pega o primeiro ticker
                                   tipo_entidade_final = "TICKER (Derivado da Label - fallback direto)"
                                   logger.info(f"  - Placeholder #ENTIDADE_NOME# (T1A/B) mapeado para TICKER derivado (fallback direto): '{placeholder_entidade_valor}' (Origem NER: '{entidade_origem}')")
                              else:
                                   logger.error(f"Não foi possível derivar ticker para a label '{chave_mapa_normalizada}'. Verifique o mapeamento JSON se possui entrada para o ticker principal (ex: VALE3).")
                                   placeholder_entidade_valor = primeira_org # Último recurso
                                   tipo_entidade_final = "FALHA_DERIVAR_TICKER"
                    else:
                        # Já é uma lista de tickers, pega o primeiro
                        placeholder_entidade_valor = map_value[0]
                        tipo_entidade_final = "TICKER (Mapeado direto)"
                        logger.info(f"  - Placeholder #ENTIDADE_NOME# (T1A/B) mapeado para TICKER: '{placeholder_entidade_valor}' (Origem NER: '{entidade_origem}', Lista: {map_value})")
            else:
                 logger.warning(f"Mapeamento encontrado para '{chave_mapa_normalizada}' mas lista vazia ou formato inválido: {map_value}")
                 placeholder_entidade_valor = primeira_org
                 tipo_entidade_final = "NER ORG (Falha Mapeamento)"

        elif re.match(r"^[A-Z]{4}\d{1,2}$", chave_mapa_normalizada): # A própria ORG é um Ticker
             placeholder_entidade_valor = chave_mapa_normalizada
             tipo_entidade_final = "TICKER (NER ORG é Ticker)"
             logger.info(f"  - Placeholder #ENTIDADE_NOME# mapeado para TICKER: '{placeholder_entidade_valor}' (Origem NER: '{entidade_origem}', Detectado como Ticker)")
        else:
            # Entidade ORG não está no mapa e não é ticker
            logger.warning(f"Entidade ORG '{primeira_org}' não encontrada no mapa JSON e não é Ticker. Usando texto original.")
            placeholder_entidade_valor = primeira_org
            tipo_entidade_final = "NER ORG (Não Mapeado)"
    else:
         logger.info("Nenhuma entidade ORG encontrada para #ENTIDADE_NOME#.")

    if placeholder_entidade_valor:
        mapeamentos["#ENTIDADE_NOME#"] = placeholder_entidade_valor
    else:
         logger.warning("Placeholder #ENTIDADE_NOME# não pôde ser mapeado.")


    # 3. Mapear #VALOR_DESEJADO#
    valor_mapeado = None
    keyword_origem = None
    keyword_encontrada = None
    # Prioriza keywords mais específicas (e mais longas se houver empate)
    keywords_valor_sorted = sorted([kw for kw in keywords_valor if kw in sinonimos_map], key=len, reverse=True)

    if keywords_valor_sorted:
        keyword_encontrada = keywords_valor_sorted[0] # Pega a mais longa/específica encontrada
        valor_mapeado = sinonimos_map[keyword_encontrada]
        keyword_origem = keyword_encontrada
        logger.info(f"  - Placeholder #VALOR_DESEJADO# mapeado para: '{valor_mapeado}' (Keyword encontrada: '{keyword_origem}')")
        mapeamentos["#VALOR_DESEJADO#"] = valor_mapeado
    else:
        logger.info("Nenhuma keyword da pergunta foi encontrada no dicionário de sinônimos para #VALOR_DESEJADO#.")


    logger.info(f"Mapeamentos finais encontrados: {mapeamentos}")
    return mapeamentos, tipo_entidade_final


# --- Função Principal ---
def main(pergunta):
    """Função principal que processa a pergunta e retorna o JSON de resultado."""
    logger.info(f"Pergunta do usuário recebida: '{pergunta}'")

    logger.info("--- Passo 1: Executando NLP (spaCy)... ---")
    if not nlp:
        logger.error("Modelo NLP não carregado.")
        return {"erro": "Serviço de PLN indisponível."}
    doc = nlp(pergunta)
    entidades_ner, data_encontrada, keywords_valor = extrair_entidades_data(doc)
    logger.info(f"  NER (spaCy - final agrupado): {entidades_ner}")
    logger.info(f"  Keywords para valor: {keywords_valor}")
    logger.info("Análise NLP concluída.")

    logger.info("--- Passo 2: Selecionando Template por similaridade... ---")
    template_id, similaridade = selecionar_template(pergunta, PERGUNTAS_INTERESSE_PATH)
    if not template_id:
        logger.error("Não foi possível selecionar um template adequado.")
        return {"erro": "Não foi possível entender o tipo da pergunta."}
    logger.info(f"Template selecionado: '{template_id}' (Similaridade estimada: {similaridade:.4f}).")

    logger.info("--- Passo 3: Mapeando placeholders com base na NLP e dicionários... ---")
    mapeamentos, tipo_entidade_detectada = mapear_placeholders(entidades_ner, data_encontrada, keywords_valor, template_id)
    if mapeamentos is None: # Verifica se o mapeamento retornou erro
         return {"erro": f"Erro durante mapeamento de placeholders ({tipo_entidade_detectada})."}
    logger.info("Mapeamento de placeholders concluído.")

    logger.info("--- Passo 4: Validando placeholders mapeados contra os requeridos pelo template (simples)... ---")
    placeholders_requeridos_mock = {
         "Template 1A": ["#DATA#", "#ENTIDADE_NOME#", "#VALOR_DESEJADO#"],
         "Template 1B": ["#DATA#", "#ENTIDADE_NOME#", "#VALOR_DESEJADO#"],
         "Template 2A": ["#ENTIDADE_NOME#", "#VALOR_DESEJADO#"]
    }
    requeridos = set(placeholders_requeridos_mock.get(template_id, []))
    mapeados_keys = set(mapeamentos.keys())

    # Validação específica mais rigorosa
    valid = True
    faltando = []
    if template_id in ["Template 1A", "Template 1B"]:
        if "#ENTIDADE_NOME#" not in mapeados_keys: faltando.append("#ENTIDADE_NOME#")
        if "#DATA#" not in mapeados_keys: faltando.append("#DATA#")
        if "#VALOR_DESEJADO#" not in mapeados_keys: faltando.append("#VALOR_DESEJADO#")
        if faltando: valid = False
    elif template_id == "Template 2A":
        if "#ENTIDADE_NOME#" not in mapeados_keys: faltando.append("#ENTIDADE_NOME#")
        # VALOR_DESEJADO (codigo) é opcional para a query funcionar
        if faltando: valid = False

    if valid:
         logger.info(f"Validação de placeholders OK para template '{template_id}'.")
    else:
         logger.error(f"Validação FALHOU para {template_id}: Placeholders essenciais faltando: {faltando}")
         return {"erro": f"Informação faltando para processar a pergunta: {', '.join(faltando)}"}

    logger.info("--- Passo 5: Construindo a resposta JSON final... ---")
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
             "placeholders_requeridos": list(requeridos),
             "tipo_entidade_final": tipo_entidade_detectada
         }
    }

    logger.info("--- Passo 6: Enviando resposta JSON para stdout... ---")
    print(json.dumps(resposta_json, ensure_ascii=False)) # Garante UTF-8
    logger.info("Resposta JSON enviada com sucesso.")
    return resposta_json


# --- Bloco de Execução ---
if __name__ == "__main__":
    pid = os.getpid()
    logger.info("--- ================================= ---")
    logger.info(f"--- INICIANDO EXECUÇÃO PLN_PROCESSOR.PY (PID: {pid}) ---")
    logger.info(f"--- Argumentos Recebidos: {sys.argv} ---")
    logger.info(f"--- Diretório Atual (CWD): {os.getcwd()} ---")
    logger.info(f"--- Diretório do Script: {script_dir} ---")

    if len(sys.argv) > 1:
        # Decodifica argumentos da linha de comando (importante se o Java não passar UTF-8)
        try:
            # Tenta decodificar como UTF-8, fallback para latin-1 se falhar
            decoded_args = [arg.encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding) if hasattr(sys.stdout, 'encoding') and sys.stdout.encoding else arg for arg in sys.argv[1:]]
            pergunta_do_usuario = " ".join(decoded_args)
        except Exception as decode_err:
             logger.warning(f"Erro ao decodificar argumentos da linha de comando, usando raw: {decode_err}")
             pergunta_do_usuario = " ".join(sys.argv[1:])

        try:
            resultado = main(pergunta_do_usuario)
            if isinstance(resultado, dict) and "erro" in resultado:
                 logger.error(f"Processamento falhou com erro: {resultado['erro']}")
                 # Imprime erro JSON no stdout
                 print(json.dumps(resultado, ensure_ascii=False))
                 sys.exit(1)
            elif isinstance(resultado, dict): # Sucesso
                logger.info("--- Processamento PLN Script concluído com sucesso (status 0) ---")
                sys.exit(0)
            else: # Retorno inesperado
                 logger.error("Função main retornou valor inesperado.")
                 print(json.dumps({"erro": "Erro interno inesperado no script Python."}, ensure_ascii=False))
                 sys.exit(1)
        except SystemExit as e:
             raise e # Não captura sys.exit()
        except Exception as e:
             logger.exception("Erro crítico não tratado durante a execução principal.")
             print(json.dumps({"erro": f"Erro crítico no servidor Python: {e}"}, ensure_ascii=False))
             sys.exit(1)
    else:
        logger.error("Erro: Nenhuma pergunta fornecida como argumento.")
        print(json.dumps({"erro": "Nenhuma pergunta fornecida."}))
        sys.exit(1)