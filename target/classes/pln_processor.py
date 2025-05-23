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

# --- Funções Auxiliares (normalizar_texto DEVE ser definida ANTES de ser usada nos carregamentos) ---
def normalizar_texto(texto_input):
    if not texto_input: return ""
    texto_lower = texto_input.lower()
    # Mapa para remover acentos comuns em português e algumas pontuações
    mapa_acentos = str.maketrans(
        "áàâãäéèêëíìîïóòôõöúùûüç", 
        "aaaaaeeeeiiiiooooouuuuc", 
        ".,;:!?()'\"<>[]{}@#$%^&*-+=~`|\\" 
    )
    texto_sem_acentos_pontuacao = texto_lower.translate(mapa_acentos)
    # Remove espaços duplicados e das pontas
    texto_limpo = ' '.join(texto_sem_acentos_pontuacao.split())
    return texto_limpo.strip()

# --- Constantes e Caminhos ---
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError: 
    script_dir = os.getcwd()
RESOURCES_DIR = script_dir
logger.info(f"Diretório base dos resources (RESOURCES_DIR) assumido: {RESOURCES_DIR}")

SINONIMOS_PATH = os.path.join(RESOURCES_DIR, "resultado_similaridade.txt")
PERGUNTAS_INTERESSE_PATH = os.path.join(RESOURCES_DIR, "perguntas_de_interesse.txt")
MAPA_EMPRESAS_JSON_PATH = os.path.join(RESOURCES_DIR, "empresa_nome_map.json")
SETOR_MAP_JSON_PATH = os.path.join(RESOURCES_DIR, "setor_map.json") # Usando o nome padrão

logger.info(f"Caminho sinônimos (valor desejado): {SINONIMOS_PATH}")
logger.info(f"Caminho perguntas interesse (seleção template): {PERGUNTAS_INTERESSE_PATH}")
logger.info(f"Caminho mapa empresas (entidade nome): {MAPA_EMPRESAS_JSON_PATH}")
logger.info(f"Caminho mapa setores (setor): {SETOR_MAP_JSON_PATH}")

# --- Carregamento de Recursos ---
def exit_with_json_error(error_message, template_id_info=None, mapeamentos_info=None, exit_code=1):
    logger.error(f"ERRO CRÍTICO PLN: {error_message}")
    error_payload = {
        "erro": error_message, 
        "template_nome": template_id_info if template_id_info else None, 
        "mapeamentos": mapeamentos_info if mapeamentos_info else {}
    }
    print(json.dumps(error_payload, ensure_ascii=False))
    sys.exit(exit_code)

nlp = None
try:
    logger.info("Carregando modelo spaCy 'pt_core_news_sm'...")
    nlp = spacy.load("pt_core_news_sm")
    logger.info("Modelo spaCy carregado com sucesso.")
except OSError:
    exit_with_json_error("Configuração crítica: Modelo spaCy 'pt_core_news_sm' não encontrado. Execute: python -m spacy download pt_core_news_sm")
except Exception as e:
    exit_with_json_error(f"Configuração crítica: Erro inesperado ao carregar modelo spaCy: {str(e)}")

empresa_nome_map = {}
try:
    if not os.path.exists(MAPA_EMPRESAS_JSON_PATH):
        exit_with_json_error(f"Configuração crítica: Arquivo de mapa de empresas JSON não encontrado em: {MAPA_EMPRESAS_JSON_PATH}")
    with open(MAPA_EMPRESAS_JSON_PATH, 'r', encoding='utf-8') as f:
        empresa_nome_map_raw = json.load(f)
        # Chaves do JSON são normalizadas (lower, sem acentos, sem espaços)
        empresa_nome_map = {normalizar_texto(k.replace(" ", "")): v for k, v in empresa_nome_map_raw.items()}
    logger.info(f"Mapa de empresas JSON carregado (chaves normalizadas): {len(empresa_nome_map)} mapeamentos.")
    if not empresa_nome_map: logger.warning(f"Mapa de empresas JSON ({MAPA_EMPRESAS_JSON_PATH}) está vazio.")
except Exception as e:
    exit_with_json_error(f"Configuração crítica: Erro ao carregar mapa de empresas JSON ({MAPA_EMPRESAS_JSON_PATH}): {str(e)}")

sinonimos_map = {} 
try:
    if not os.path.exists(SINONIMOS_PATH):
        logger.warning(f"Arquivo de sinônimos ({SINONIMOS_PATH}) não encontrado.")
    else:
        with open(SINONIMOS_PATH, 'r', encoding='utf-8') as f_sin:
            for line_s in f_sin:
                line_s = line_s.strip()
                if not line_s or line_s.startswith('#'): continue
                parts_s = line_s.split(';')
                if len(parts_s) == 2 and parts_s[0].strip() and parts_s[1].strip():
                    sinonimos_map[normalizar_texto(parts_s[0].strip())] = parts_s[1].strip()
        logger.info(f"Dicionário de sinônimos (para #VALOR_DESEJADO#) carregado (chaves normalizadas): {len(sinonimos_map)} chaves.")
        if not sinonimos_map and os.path.exists(SINONIMOS_PATH): logger.warning(f"Dicionário de sinônimos ({SINONIMOS_PATH}) carregado, mas vazio.")
except Exception as e_sin:
    logger.error(f"Erro (não crítico) ao carregar dicionário de sinônimos ({SINONIMOS_PATH}): {str(e_sin)}")

setor_map_global = {} 
try:
    if not os.path.exists(SETOR_MAP_JSON_PATH):
        exit_with_json_error(f"Configuração crítica: Arquivo de mapa de setores JSON não encontrado em: {SETOR_MAP_JSON_PATH}")
    with open(SETOR_MAP_JSON_PATH, 'r', encoding='utf-8') as f_set:
        setor_map_raw = json.load(f_set)
        # Chaves do JSON são normalizadas. Valores são mantidos como estão no JSON (com acentos e capitalização).
        setor_map_global = {normalizar_texto(k): v for k, v in setor_map_raw.items()} 
    logger.info(f"Mapa de setores JSON carregado (chaves normalizadas, valores originais do JSON): {len(setor_map_global)} mapeamentos.")
    if not setor_map_global: logger.warning(f"Mapa de setores JSON ({SETOR_MAP_JSON_PATH}) está vazio. Mapeamento de #SETOR# falhará.")
except Exception as e_set:
    exit_with_json_error(f"Configuração crítica: Erro ao carregar mapa de setores JSON ({SETOR_MAP_JSON_PATH}): {str(e_set)}")


def extrair_entidades_data(doc_spacy):
    entidades_ner_dict = {"ORG": [], "DATE": [], "LOC": [], "MISC": [], "PER": []}
    keywords_valor_encontradas = [] 
    data_formatada_iso = None
    for ent in spacy.util.filter_spans(doc_spacy.ents):
        if ent.label_ in entidades_ner_dict and ent.text not in entidades_ner_dict[ent.label_]:
            entidades_ner_dict[ent.label_].append(ent.text)
    match_obj_date = re.compile(r'\b(\d{1,2}/\d{1,2}/\d{4})\b|\b(\d{4}-\d{1,2}-\d{1,2})\b').search(doc_spacy.text)
    if match_obj_date:
        date_str = match_obj_date.group(1) or match_obj_date.group(2)
        try:
            data_formatada_iso = (datetime.strptime(date_str, '%d/%m/%Y') if '/' in date_str else datetime.strptime(date_str, '%Y-%m-%d')).strftime('%Y-%m-%d')
        except ValueError: pass
    
    texto_norm_para_keywords = normalizar_texto(doc_spacy.text)
    # Chaves do sinonimos_map já estão normalizadas
    for kw_norm_mapa_sinonimos in sorted(sinonimos_map.keys(), key=len, reverse=True): 
         if re.search(r'\b' + re.escape(kw_norm_mapa_sinonimos) + r'\b', texto_norm_para_keywords):
             keywords_valor_encontradas.append(kw_norm_mapa_sinonimos)
    return entidades_ner_dict, data_formatada_iso, keywords_valor_encontradas

def selecionar_template(pergunta_usuario_original, path_perguntas_interesse):
    pergunta_usuario_norm_para_match = normalizar_texto(pergunta_usuario_original)
    map_id_por_exemplo_original = {} 
    lista_exemplos_norm_para_difflib = []
    lista_exemplos_originais = []
    try:
        if not os.path.exists(path_perguntas_interesse):
             logger.error(f"CRÍTICO (selecionar_template): {path_perguntas_interesse} não encontrado.")
             return None, 0.0
        with open(path_perguntas_interesse, 'r', encoding='utf-8') as f_pi:
            for line_pi in f_pi:
                line_pi_strip = line_pi.strip()
                if not line_pi_strip or line_pi_strip.startswith('#'): continue
                parts_pi = line_pi_strip.split(';', 1)
                if len(parts_pi) == 2 and parts_pi[0].strip() and parts_pi[1].strip():
                    template_id_lido = parts_pi[0].strip() 
                    pergunta_exemplo_lida = parts_pi[1].strip()
                    map_id_por_exemplo_original[pergunta_exemplo_lida] = template_id_lido
                    pergunta_exemplo_sem_ph = re.sub(r'<[^>]+>', '', pergunta_exemplo_lida)
                    lista_exemplos_norm_para_difflib.append(normalizar_texto(pergunta_exemplo_sem_ph))
                    lista_exemplos_originais.append(pergunta_exemplo_lida)
            if not map_id_por_exemplo_original:
                 logger.error(f"ERRO (selecionar_template): Nenhuma pergunta de interesse válida carregada de {path_perguntas_interesse}.")
                 return None, 0.0
    except Exception as e_pi:
         logger.error(f"CRÍTICO (selecionar_template): Erro ao ler perguntas de interesse: {str(e_pi)}")
         return None, 0.0

    matches_difflib = get_close_matches(pergunta_usuario_norm_para_match, lista_exemplos_norm_para_difflib, n=1, cutoff=0.60) 
    if matches_difflib:
        melhor_match_exemplo_norm_sem_ph = matches_difflib[0]
        try:
            idx_match = lista_exemplos_norm_para_difflib.index(melhor_match_exemplo_norm_sem_ph)
            melhor_match_exemplo_original_com_ph = lista_exemplos_originais[idx_match]
            template_id_final_selecionado = map_id_por_exemplo_original[melhor_match_exemplo_original_com_ph]
            set_usuario = set(pergunta_usuario_norm_para_match.split())
            set_exemplo_match_sem_ph = set(melhor_match_exemplo_norm_sem_ph.split())
            intersecao = len(set_usuario.intersection(set_exemplo_match_sem_ph))
            uniao = len(set_usuario.union(set_exemplo_match_sem_ph))
            score_jaccard = intersecao / uniao if uniao > 0 else 0.0
            logger.info(f"Template selecionado: '{template_id_final_selecionado}' (Match com: '{melhor_match_exemplo_original_com_ph}', Jaccard Aprox: {score_jaccard:.3f})")
            return template_id_final_selecionado, score_jaccard 
        except ValueError: 
             logger.error(f"Erro interno (selecionar_template): Match '{melhor_match_exemplo_norm_sem_ph}' não encontrado.")
             return None, 0.0
    logger.warning(f"Nenhum template similar para: '{pergunta_usuario_original}' (cutoff=0.60)")
    return None, 0.0

def extract_and_normalize_setor(pergunta_original_texto):
    logger.debug(f"Tentando extrair setor da pergunta: '{pergunta_original_texto}'")
    setor_extraido_bruto_norm_para_chave = None 
    pergunta_norm = normalizar_texto(pergunta_original_texto)
    padroes_regex = [
        r'setor\s+(?:de\s+|do\s+|da\s+)?([a-z0-9\s\-\_]+?)(?:\?|$|\s+com|\s+que|listad[ao]s|na\s+b3|brasileir[ao]s)',
        r'(?:ações|acoes|empresas)\s+do\s+setor\s+([a-z0-9\s\-\_]+?)(?:\?|$)'
    ]
    for pattern in padroes_regex:
        match = re.search(pattern, pergunta_norm)
        if match:
            setor_extraido_bruto_norm_para_chave = match.group(1).strip()
            logger.info(f"Setor bruto (norm) extraído por regex ('{pattern}'): '{setor_extraido_bruto_norm_para_chave}'")
            break 
    if not setor_extraido_bruto_norm_para_chave:
        # Chaves do setor_map_global já foram normalizadas no carregamento
        sorted_map_keys_norm = sorted(setor_map_global.keys(), key=len, reverse=True) 
        for key_norm_mapa_setor in sorted_map_keys_norm:
            if re.search(r'\b' + re.escape(key_norm_mapa_setor) + r'\b', pergunta_norm):
                setor_extraido_bruto_norm_para_chave = key_norm_mapa_setor
                logger.info(f"Setor bruto (norm) encontrado por keyword do setor_map_global: '{setor_extraido_bruto_norm_para_chave}'")
                break
    if setor_extraido_bruto_norm_para_chave:
        # Chaves do setor_map_global já estão normalizadas, então setor_extraido_bruto_norm_para_chave deve ser uma chave
        setor_final_para_ontologia = setor_map_global.get(setor_extraido_bruto_norm_para_chave)
        if setor_final_para_ontologia:
            logger.info(f"Valor do setor para ontologia (do mapa): '{setor_final_para_ontologia}' (Chave de busca no mapa: '{setor_extraido_bruto_norm_para_chave}')")
            return setor_final_para_ontologia # Retorna o valor como está no JSON (ex: "Energia Elétrica")
        logger.warning(f"Termo de setor '{setor_extraido_bruto_norm_para_chave}' extraído mas não é chave válida em setor_map_global.")
    logger.info(f"Nenhum termo de setor pôde ser extraído ou normalizado de: '{pergunta_original_texto}'")
    return None

def mapear_placeholders(ent_ner, data_iso, kw_val_detectadas, tid, p_orig_txt):
    maps = {}
    tipo_ent_dbg = "N/A (Placeholder #ENTIDADE_NOME# não preenchido)"
    if data_iso: maps["#DATA#"] = data_iso; logger.info(f"  Placeholder #DATA# => '{data_iso}'")

    ent_nome_final = None
    if ent_ner.get("ORG"):
        org_ner_txt = ent_ner["ORG"][0]
        chave_map = normalizar_texto(org_ner_txt).replace(" ", "")
        logger.debug(f"Processando ORG NER: '{org_ner_txt}' (chave normalizada para mapa: '{chave_map}')")

        if chave_map in empresa_nome_map: # empresa_nome_map tem chaves normalizadas
            val_map_list = empresa_nome_map[chave_map]
            if isinstance(val_map_list, list) and val_map_list:
                val0 = val_map_list[0] 
                if tid == "Template 2A":
                    if val0.endswith("_LABEL"): 
                        ent_nome_final = chave_map.upper() 
                        tipo_ent_dbg = f"LABEL_EMPRESA (de _LABEL, chave: {chave_map})"
                    elif not re.match(r"^[A-Z]{4}\d{1,2}$", val0.upper()): 
                        ent_nome_final = val0.upper() 
                        tipo_ent_dbg = f"LABEL_EMPRESA (Mapeado direto para '{val0}')"
                    else: 
                        ent_nome_final = chave_map.upper() if not re.match(r"^[A-Z]{4}\d{1,2}$", chave_map.upper()) else val0
                        tipo_ent_dbg = f"LABEL_EMPRESA_FALLBACK (Chave '{chave_map}' usada, pois mapeamento era Ticker '{val0}')"
                        logger.warning(f"T2A: Entidade '{org_ner_txt}' mapeou para Ticker '{val0}'. Usando '{ent_nome_final}' como label.")
                else: # Template 1A, 1B etc.
                    if val0.endswith("_LABEL"):
                        ticker_derivado = None
                        for sfx in ["3", "4", "11", "5", "6"]: 
                            chk_ticker_deriv = chave_map + sfx 
                            if chk_ticker_deriv in empresa_nome_map and isinstance(empresa_nome_map[chk_ticker_deriv], list) and empresa_nome_map[chk_ticker_deriv]: 
                                ticker_derivado = empresa_nome_map[chk_ticker_deriv][0]; break
                        if ticker_derivado:
                            ent_nome_final = ticker_derivado
                            tipo_ent_dbg = f"TICKER (Derivado de Label '{chave_map}' para '{ticker_derivado}')"
                        else:
                            logger.error(f"FALHA derivar Ticker para Label '{chave_map}' (de NER '{org_ner_txt}')")
                            ent_nome_final = org_ner_txt 
                            tipo_ent_dbg = "FALHA_DERIVAR_TICKER_DE_LABEL"
                    elif re.match(r"^[A-Z]{4}\d{1,2}$", val0.upper()): 
                        ent_nome_final = val0.upper()
                        tipo_ent_dbg = f"TICKER (Mapeado direto de chave '{chave_map}')"
                    else: 
                        ent_nome_final = val0; tipo_ent_dbg = f"MAPEADO_NAO_TICKER ('{val0}')"
                        logger.warning(f"Valor '{val0}' para '{chave_map}' não é ticker nem _LABEL para template {tid} que espera ticker.")
            else: ent_nome_final = org_ner_txt; tipo_ent_dbg = "NER_ORG (Falha Formato Valor Mapeamento)"
        elif re.match(r"^[A-Z]{4}\d{1,2}$", org_ner_txt.upper()): 
            if tid != "Template 2A":
                 ent_nome_final = org_ner_txt.upper()
                 tipo_ent_dbg = "TICKER (NER ORG já era Ticker)"
            else: 
                 ent_nome_final = re.sub(r'\d+$', '', org_ner_txt.upper()) 
                 tipo_ent_dbg = f"LABEL_DE_TICKER_NER (NER ORG era Ticker '{org_ner_txt}', usando '{ent_nome_final}' como label para T2A)"
                 logger.info(f"T2A: NER ORG era Ticker '{org_ner_txt}'. Usando '{ent_nome_final}' como possível label.")
        else:
            ent_nome_final = org_ner_txt.upper() if tid == "Template 2A" else org_ner_txt 
            tipo_ent_dbg = "LABEL_EMPRESA (NER ORG não mapeada)" if tid == "Template 2A" else "NER_ORG (Não Mapeado, Não Ticker)"
            if tid != "Template 2A": logger.warning(f"ORG '{org_ner_txt}' não mapeada/ticker para {tid}")
    
    if not ent_nome_final and tid != "Template 2A":
        m_tf = re.search(r'\b([A-Z]{4}\d{1,2})\b', p_orig_txt.upper())
        if m_tf: ent_nome_final = m_tf.group(1); tipo_ent_dbg = "TICKER (Regex Fallback)"

    if ent_nome_final: 
        maps["#ENTIDADE_NOME#"] = ent_nome_final
        logger.info(f"  Placeholder #ENTIDADE_NOME# => '{ent_nome_final}' (Tipo Detecção: {tipo_ent_dbg})")
    else:
         logger.warning(f"Placeholder #ENTIDADE_NOME# não pôde ser mapeado para template {tid}.")

    if kw_val_detectadas: # kw_val_detectadas são chaves normalizadas do sinonimos_map
        val_des = sinonimos_map.get(kw_val_detectadas[0])
        if val_des: maps["#VALOR_DESEJADO#"] = val_des; logger.info(f"  Placeholder #VALOR_DESEJADO# => '{val_des}' (KW: '{kw_val_detectadas[0]}')")

    if tid == "Template 3A": # tid já é sem underscore
        set_norm = extract_and_normalize_setor(p_orig_txt)
        if set_norm: maps["#SETOR#"] = set_norm; logger.info(f"  Placeholder #SETOR# => '{set_norm}'")
    
    logger.debug(f"Mapeamentos placeholders: {maps}")
    return maps, tipo_ent_dbg

def main(pergunta_usuario_java):
    logger.info(f"* INÍCIO Main PLN: '{pergunta_usuario_java}' *")
    if not nlp: exit_with_json_error("PLN Interno: Modelo spaCy não carregado.")

    doc_spacy_obj = nlp(pergunta_usuario_java)
    entidades_ner, data_iso, keywords_valor = extrair_entidades_data(doc_spacy_obj)
    logger.debug(f"  NER:{entidades_ner}, Data:{data_iso}, KW_Valor:{keywords_valor}")

    template_id, similaridade_score = selecionar_template(pergunta_usuario_java, PERGUNTAS_INTERESSE_PATH)
    if not template_id: 
        exit_with_json_error(f"Não foi possível selecionar template para: '{pergunta_usuario_java}'", 
                             template_id_info=template_id) # Passa template_id mesmo que None

    logger.info(f"Template selecionado: '{template_id}' (Similaridade aprox.: {similaridade_score:.3f})")
    
    map_placeholders_final, tipo_ent_debug = mapear_placeholders(
        entidades_ner, data_iso, keywords_valor, template_id, pergunta_usuario_java
    )
    
    # *** CORREÇÃO: As chaves aqui devem ser como o template_id é retornado (sem underscore) ***
    placeholders_essenciais_por_template = {
         "Template 1A": ["#DATA#", "#ENTIDADE_NOME#", "#VALOR_DESEJADO#"],
         "Template 1B": ["#DATA#", "#ENTIDADE_NOME#", "#VALOR_DESEJADO#"],
         "Template 2A": ["#ENTIDADE_NOME#"], 
         "Template 3A": ["#SETOR#"] 
    }
    
    faltando_essenciais_list = []
    if template_id in placeholders_essenciais_por_template:
        for ph_req_essencial in placeholders_essenciais_por_template[template_id]: 
            if ph_req_essencial not in map_placeholders_final or not map_placeholders_final.get(ph_req_essencial):
                faltando_essenciais_list.append(ph_req_essencial)
    else: 
        logger.warning(f"Template ID '{template_id}' não está na config de validação de placeholders. Validação não realizada.")

    if faltando_essenciais_list:
        msg_erro_faltando = f"Informação essencial faltando para template '{template_id}': {', '.join(faltando_essenciais_list)}."
        logger.error(msg_erro_faltando)
        print(json.dumps({
            "template_nome": template_id, "mapeamentos": map_placeholders_final, "erro": msg_erro_faltando, 
            "_debug_info": {"pergunta_original": pergunta_usuario_java, "faltando_essenciais": faltando_essenciais_list, "tipo_entidade_principal_debug": tipo_ent_debug}
        }, ensure_ascii=False))
        sys.exit(0) 
    
    logger.info(f"Validação OK para '{template_id}'.")
    resposta_final_obj = {
        "template_nome": template_id, "mapeamentos": map_placeholders_final,
        "_debug_info": {
             "pergunta_original_recebida": pergunta_usuario_java,
             "ner_spacy_detectadas": entidades_ner, "data_iso_detectada": data_iso,
             "keywords_valor_detectadas_mapa": keywords_valor,
             "template_score_similaridade": round(similaridade_score, 3),
             "tipo_entidade_principal_debug": tipo_ent_debug
         }
    }
    print(json.dumps(resposta_final_obj, ensure_ascii=False))
    logger.info(f"* FIM Main PLN: JSON enviado. Status 0. *")
    sys.exit(0)

if __name__ == "__main__":
    logger.info(f"--- INICIANDO SCRIPT PLN_PROCESSOR.PY (PID: {os.getpid()}), Args: {sys.argv} ---")
    if len(sys.argv) > 1:
        pergunta_completa_usuario = " ".join(sys.argv[1:])
        try: main(pergunta_completa_usuario)
        except SystemExit: raise
        except Exception as e_main: exit_with_json_error(f"Erro não tratado em main: {str(e_main)}")
    else: exit_with_json_error("Nenhuma pergunta fornecida.")