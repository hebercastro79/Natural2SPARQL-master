# -*- coding: utf-8 -*-
import nltk
import spacy
import difflib
import re
import unicodedata
# from nltk.stem.rslp import RSLPStemmer
import logging
import sys
import json
import os
from datetime import datetime, timedelta
import io
from pathlib import Path
import traceback

# --- Configuração Inicial Essencial ---
try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
except Exception as e_enc: print(f"AVISO URGENTE: Falha UTF-8: {e_enc}", file=sys.__stderr__)

# --- DEFINIÇÃO DO LOGGER E FUNÇÃO DE ERRO ---
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_logger = logging.getLogger("PLN_Processor")
_logger.setLevel(logging.DEBUG)
if _logger.hasHandlers(): _logger.handlers.clear()
try: _SCRIPT_DIR_PATH = Path(__file__).parent.resolve()
except NameError: _SCRIPT_DIR_PATH = Path(".").resolve()
_log_file_path = _SCRIPT_DIR_PATH / 'pln_processor.log'
try:
    file_handler = logging.FileHandler(_log_file_path, encoding='utf-8', mode='a')
    file_handler.setFormatter(log_formatter); file_handler.setLevel(logging.DEBUG); _logger.addHandler(file_handler)
except Exception as e: print(f"AVISO URGENTE: Falha log file '{_log_file_path}': {e}", file=sys.__stderr__)
try:
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(log_formatter); stderr_handler.setLevel(logging.INFO); _logger.addHandler(stderr_handler)
except Exception as e: print(f"AVISO: Falha log stderr: {e}", file=sys.stderr)

def fail_with_json_error(error_message, details=None, status_code=1):
    error_payload = {"erro": error_message}
    if details:
        try: details_str = str(details); error_payload["detalhes"] = details_str[:500] + ('...' if len(details_str) > 500 else '')
        except Exception: error_payload["detalhes"] = "Erro serializar detalhes."
    _logger.error(f"Finalizando com erro: {error_message} - Detalhes: {details}", exc_info=True if status_code != 0 else False)
    try: print(json.dumps(error_payload, ensure_ascii=False)); sys.stdout.flush()
    except Exception as json_e: _logger.critical(f"Erro CRÍTICO gerar JSON erro: {json_e}"); print(f'{{"erro": "Erro critico JSON.", "detalhes": "{str(json_e)[:100]}..."}}'); sys.stdout.flush()
    sys.exit(status_code)

# --- Carregamento Modelos ---
try: nlp = spacy.load("pt_core_news_sm"); _logger.info("Modelo spaCy carregado.")
except Exception as e_spacy: fail_with_json_error("Erro ao inicializar spaCy.", e_spacy)

# --- Constantes e Caminhos ---
RESOURCES_DIR = _SCRIPT_DIR_PATH
CAMINHO_DICIONARIO_SINONIMOS = RESOURCES_DIR / 'resultado_similaridade.txt'
CAMINHO_PERGUNTAS_INTERESSE = RESOURCES_DIR / 'perguntas_de_interesse.txt'

# --- CAMINHO CORRETO PARA O JSON ---
JSON_FILENAME = 'empresa_nome_map.json' # <-- NOME CORRETO
CAMINHO_MAPA_EMPRESAS_JSON_STR = JSON_FILENAME # Tenta no CWD primeiro (target/classes)
CAMINHO_MAPA_EMPRESAS_JSON_ABS = (_SCRIPT_DIR_PATH / JSON_FILENAME) # Caminho absoluto
# --- FIM CORREÇÃO ---

_logger.info(f"Diretório base script (inferido): {_SCRIPT_DIR_PATH.resolve()}")
_logger.info(f"Tentando carregar mapa empresas de (relativo): {CAMINHO_MAPA_EMPRESAS_JSON_STR}")

# --- Carregamento do Mapa de Empresas JSON ---
empresa_nome_map = {} # {CHAVE_UPPER -> NOME_CANONICO}
if not os.path.exists(CAMINHO_MAPA_EMPRESAS_JSON_STR):
     _logger.warning(f"Tentativa 1: Arquivo '{CAMINHO_MAPA_EMPRESAS_JSON_STR}' não encontrado no CWD. Tentando fallback absoluto: {CAMINHO_MAPA_EMPRESAS_JSON_ABS}")
     if not os.path.exists(CAMINHO_MAPA_EMPRESAS_JSON_ABS):
          _logger.error(f"Tentativa 2: Fallback absoluto '{CAMINHO_MAPA_EMPRESAS_JSON_ABS}' também FALHOU.")
          fail_with_json_error("Arquivo config empresas ausente.", details=f"Não encontrado: {CAMINHO_MAPA_EMPRESAS_JSON_STR} ou {CAMINHO_MAPA_EMPRESAS_JSON_ABS}")
     else:
         CAMINHO_MAPA_EMPRESAS_JSON_STR = str(CAMINHO_MAPA_EMPRESAS_JSON_ABS)
         _logger.info(f"Fallback absoluto {CAMINHO_MAPA_EMPRESAS_JSON_STR} encontrado!")
try:
    _logger.info(f"Abrindo JSON de: {CAMINHO_MAPA_EMPRESAS_JSON_STR}")
    with open(CAMINHO_MAPA_EMPRESAS_JSON_STR, 'r', encoding='utf-8') as f_json:
        empresa_nome_map_raw = json.load(f_json)
        empresa_nome_map = {str(key).upper(): val for key, val in empresa_nome_map_raw.items()}
        _logger.info(f"Carregados {len(empresa_nome_map)} mapeamentos do mapa de nomes.")
except Exception as e_load_json:
    fail_with_json_error("Erro carregar config empresas.", e_load_json)

pronomes_interrogativos = ['quem', 'o que', 'que', 'qual', 'quais', 'quanto', 'quantos', 'onde', 'como', 'quando', 'por que', 'porquê']

# --- Funções Auxiliares ---

def normalize_key(text):
    if not isinstance(text, str): return None
    text = text.upper().strip()
    try: text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    except Exception as e_norm: print(f" Aviso: Falha acentos '{text[:50]}...': {e_norm}", file=sys.stderr)
    text = re.sub(r'\b(S\.?A\.?|S/?A|CIA\.?|COMPANHIA|LTDA\.?|ON|PN|N[12]|PREF\.?|ORD\.?|NM|ED|EJ|MA)\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r'[^\w]', '', text)
    text = re.sub(r'\s+', '', text)
    return text if text else None

def carregar_arquivo_linhas(caminho: Path):
    caminho_str = str(caminho.resolve())
    _logger.debug(f"Tentando carregar: {caminho_str}")
    try:
        if not caminho.exists(): raise FileNotFoundError(f"Não encontrado: {caminho_str}")
        if not caminho.is_file(): raise IOError(f"Não é arquivo: {caminho_str}")
        for enc in ['utf-8', 'latin-1', 'windows-1252']:
            try: content = caminho.read_text(encoding=enc); _logger.info(f"'{caminho.name}' lido com {enc}"); break
            except UnicodeDecodeError: continue
            except Exception as e: raise IOError(f"Erro leitura {enc}") from e
        else: raise IOError(f"Impossível decodificar '{caminho.name}'")
        if content.startswith('\ufeff'): content = content[1:]
        linhas = [ln.strip() for ln in content.splitlines() if ln.strip()]
        if not linhas: _logger.warning(f"'{caminho.name}' vazio.")
        return linhas
    except Exception as e: fail_with_json_error(f"Erro crítico ler '{caminho.name}'.", e)

def carregar_dicionario_sinonimos(caminho: Path):
    dicionario = {}; linhas = carregar_arquivo_linhas(caminho)
    _logger.info(f"Processando dic. sinônimos '{caminho.name}'...")
    try:
        chave_regex = re.compile(r"^\s*([\w_]+)\s*=\s*\[(.*)\]\s*$"); valor_regex = re.compile(r"\(\s*'((?:[^'\\]|\\.)*)'\s*,\s*([\d\.]+)\s*\)")
        for i, linha in enumerate(linhas):
            if not linha or linha.startswith('#'): continue
            match = chave_regex.match(linha)
            if match:
                chave, val_str = match.group(1).lower().strip(), match.group(2); vals = []
                for s_match in valor_regex.finditer(val_str):
                    sin, v_str = s_match.group(1).replace("\\'", "'").strip(), s_match.group(2)
                    try: v = float(v_str); vals.append((sin, v))
                    except ValueError: _logger.warning(f"Valor inválido '{v_str}' ('{sin}', '{chave}') L{i+1}")
                if vals: dicionario[chave] = vals
            else: _logger.warning(f"Formato inválido dic. L{i+1}: {linha[:100]}...")
    except Exception as e: fail_with_json_error(f"Erro formato dic. '{caminho.name}'.", e)
    if not dicionario: _logger.error(f"Dicionário '{caminho.name}' vazio!");
    _logger.info(f"Dicionário '{caminho.name}' carregado: {len(dicionario)} chaves.")
    return dicionario

def normalizar_texto(texto):
    if not texto: return ""
    try:
        t = str(texto).strip().lower()
        t = "".join(ch for ch in t if unicodedata.category(ch)[0] not in ('C', 'Z') or ch == ' ')
        t = ''.join(c for c in unicodedata.normalize('NFD', t) if unicodedata.category(c) != 'Mn')
        t = re.sub(r'[^\w\s-]', '', t).strip('-').strip()
        t = re.sub(r'\s+', ' ', t)
        return t
    except Exception as e: _logger.warning(f"Erro normalizar '{texto[:50]}...': {e}"); return str(texto).strip().lower()

def extrair_entidades_spacy(texto):
    elementos = {"sujeito": [], "predicado": [], "objeto": []}; entidades_nomeadas = {}
    _logger.debug(f"Extraindo spaCy de: '{texto[:100]}...'");
    if not texto: return elementos, entidades_nomeadas
    try:
        doc = nlp(texto);
        for ent in doc.ents:
            label = ent.label_; text = ent.text.strip().rstrip('?.!,;')
            if text and not text.isdigit(): entidades_nomeadas.setdefault(label, []).append(text)
        for label in entidades_nomeadas: entidades_nomeadas[label] = sorted(list(set(entidades_nomeadas[label])), key=lambda x: texto.find(x))
        for token in doc:
            if token.dep_ in ("nsubj", "nsubj:pass") and token.text.lower() not in pronomes_interrogativos : elementos["sujeito"].append(token.text)
            elif token.dep_ in ("obj", "dobj", "iobj", "pobj", "obl", "attr", "acomp"): elementos["objeto"].append(token.text)
            elif token.pos_ == "VERB" and token.dep_ == "ROOT": elementos["predicado"].append(token.lemma_)
        for key in elementos: elementos[key] = sorted(list(set(elementos[key])), key=lambda x: texto.find(x))
        _logger.info(f"Extração spaCy final. SPO: {elementos}, NER: {entidades_nomeadas}")
    except Exception as e: _logger.error(f"Erro GERAL extração spaCy: {e}", exc_info=True); elementos = {}; entidades_nomeadas = {}
    return elementos, entidades_nomeadas

def extrair_data(texto):
    _logger.debug(f"Extraindo data de: '{texto}'"); padrao_dma=r'\b(\d{1,2})\s*[/.-]\s*(\d{1,2})\s*[/.-]\s*(\d{4})\b'; padrao_amd=r'\b(\d{4})\s*[/.-]\s*(\d{1,2})\s*[/.-]\s*(\d{1,2})\b';
    match = re.search(padrao_dma, texto) or re.search(padrao_amd, texto); is_dma = (match and match.re.pattern == padrao_dma)
    if match:
        try: g=match.groups(); d,m,a=(int(g[0]),int(g[1]),int(g[2])) if is_dma else (int(g[2]),int(g[1]),int(g[0])); fmt=datetime(a,m,d).strftime("%Y-%m-%d"); _logger.info(f"Data '{match.group(0)}'->{fmt}"); return fmt
        except: _logger.warning(f"Erro conversão data '{match.group(0)}'")
    t = texto.lower()
    if "hoje" in t or "hj" in t: _logger.info("'hoje'/'hj'->atual"); return datetime.now().strftime("%Y-%m-%d")
    if "ontem" in t: _logger.info("'ontem'->anterior"); return (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
    _logger.info("Nenhuma data reconhecida."); return None

def encontrar_termo_dicionario(frase, dicionario, limiar=0.70):
    frase_norm = normalizar_texto(frase)
    if not dicionario or not frase_norm: return None
    melhor_chave, maior_similaridade, texto_match = None, limiar - 0.01, None
    for chave, sinonimos in dicionario.items():
        sim_chave = difflib.SequenceMatcher(None, frase_norm, chave).ratio()
        if sim_chave > maior_similaridade: maior_similaridade, melhor_chave, texto_match = sim_chave, chave, chave
        for sinonimo, _ in sinonimos:
            sin_norm = normalizar_texto(sinonimo)
            if sin_norm:
                sim_sin = difflib.SequenceMatcher(None, frase_norm, sin_norm).ratio()
                if sim_sin > maior_similaridade: maior_similaridade, melhor_chave, texto_match = sim_sin, chave, sinonimo
    if melhor_chave: _logger.info(f"Termo para '{frase}': Chave='{melhor_chave}' (Match '{texto_match}', Sim: {maior_similaridade:.2f})"); return melhor_chave
    else: _logger.info(f"Nenhum termo encontrado para '{frase}' >= {limiar:.2f}"); return None

def encontrar_pergunta_similar(pergunta_usuario, templates_linhas, limiar=0.65):
    maior_sim, t_nome, t_linha = 0.0, None, None
    if not templates_linhas: _logger.error("Lista exemplos vazia."); return None, 0, None
    pu_norm = normalizar_texto(pergunta_usuario);
    if not pu_norm: _logger.error("Pergunta usuário normalizada vazia."); return None, 0, None
    _logger.debug(f"Buscando template para '{pu_norm}' (Limiar: {limiar:.2f})")
    for i, linha_t in enumerate(templates_linhas):
        if not linha_t or linha_t.startswith('#') or " - " not in linha_t: continue;
        try:
            nome_t, ex_t = [p.strip() for p in linha_t.split(" - ", 1)]
            if nome_t and ex_t:
                 ex_norm = normalizar_texto(ex_t)
                 if ex_norm:
                     sim = difflib.SequenceMatcher(None, pu_norm, ex_norm).ratio(); _logger.debug(f"    -> '{nome_t}', Sim: {sim:.4f}")
                     if sim > maior_sim: maior_sim, t_nome, t_linha = sim, nome_t, linha_t
        except: _logger.warning(f"Erro processar linha template {i+1}")
    if maior_sim < limiar: _logger.warning(f"Similaridade max ({maior_sim:.2f}) < limiar ({limiar:.2f})."); return None, maior_sim, None
    _logger.info(f"Template similar: Nome='{t_nome}', Sim={maior_sim:.4f}"); return t_nome, maior_sim, t_linha

# --- Função de Mapeamento ATUALIZADA ---
def mapear_para_placeholders(pergunta_usuario_original, elementos, entidades_nomeadas, data, dicionario_sinonimos, nome_map):
    """Mapeia elementos para placeholders, focando em extrair NOME canônico da entidade e VD por keyword."""
    mapeamentos = {}; _logger.debug("Iniciando mapeamento semântico...")
    if data: mapeamentos['#DATA'] = data; _logger.debug(f"  - Map #DATA: {data}")

    # 1. Encontrar o Nome Canônico da Entidade
    nome_canonico_encontrado, tipo_entidade_detectada = None, None
    ner_order = ['ORG', 'GPE', 'MISC']; known_keys_upper = set(nome_map.keys())
    entidades_ner_detectadas = []
    for label in ner_order:
        if label in entidades_nomeadas: entidades_ner_detectadas.extend([(ent, label) for ent in entidades_nomeadas[label]])
    for label, ents in entidades_nomeadas.items():
        if label not in ner_order: entidades_ner_detectadas.extend([(ent, label) for ent in ents])
    _logger.debug(f"  - Entidades NER p/ validação: {entidades_ner_detectadas}")
    best_candidate_name = None; highest_priority_found = 99; best_score = 0.7
    for ent_text, ent_label in entidades_ner_detectadas:
        ent_text_upper = ent_text.upper().strip().replace('.','').replace('?','').replace('!','')
        current_priority = ner_order.index(ent_label) if ent_label in ner_order else 90
        _logger.debug(f"    Validando NER: '{ent_text}' (Label:{ent_label}) -> Upper: '{ent_text_upper}'")
        if ent_text_upper in known_keys_upper:
            if current_priority <= highest_priority_found:
                 score = 1.0; nome_canonico_encontrado = nome_map[ent_text_upper]; best_candidate_name = ent_text
                 tipo_entidade_detectada = f"NER {ent_label} (Match Exato Gazetteer)"; highest_priority_found = current_priority; best_score = score
                 _logger.debug(f"    -> Match Exato Prioritário: Nome='{best_candidate_name}', Mapeado='{nome_canonico_encontrado}'")
        ent_norm = normalize_key(ent_text)
        if ent_norm and ent_norm in known_keys_upper:
             if current_priority <= highest_priority_found:
                 if best_candidate_name is None or current_priority < highest_priority_found or difflib.SequenceMatcher(None, ent_norm, ent_norm).ratio() >= best_score:
                     score = difflib.SequenceMatcher(None, ent_norm, ent_norm).ratio()
                     nome_canonico_encontrado = nome_map[ent_norm]; best_candidate_name = ent_text
                     tipo_entidade_detectada = f"NER {ent_label} (Match Normalizado Gazetteer)"; highest_priority_found = current_priority; best_score = score
                     _logger.debug(f"    -> Match Normalizado Prioritário: Nome='{best_candidate_name}' (Norm:{ent_norm}), Mapeado='{nome_canonico_encontrado}'")

    if nome_canonico_encontrado:
        mapeamentos['#ENTIDADE_NOME#'] = nome_canonico_encontrado.strip(); _logger.info(f"  - Map #ENTIDADE_NOME#: '{mapeamentos['#ENTIDADE_NOME#']}' (Tipo: {tipo_entidade_detectada}, Orig: '{best_candidate_name}')")
    else: _logger.error("  - FALHA CRÍTICA AO MAPEAR #ENTIDADE_NOME.")

    # 2. Mapear #VALOR_DESEJADO (Abordagem por Palavra-chave Simples)
    valor_desejado_chave = None
    pergunta_lower = pergunta_usuario_original.lower()
    _logger.debug(f"  - Buscando keyword para #VALOR_DESEJADO em: '{pergunta_lower}'")
    possible_vd_key = None
    if "preço de abertura" in pergunta_lower or "preco de abertura" in pergunta_lower: possible_vd_key = "preco_abertura"
    elif "preço de fechamento" in pergunta_lower or "preco de fechamento" in pergunta_lower: possible_vd_key = "preco_fechamento"
    elif "preço máximo" in pergunta_lower or "preco maximo" in pergunta_lower: possible_vd_key = "preco_maximo"
    elif "preço mínimo" in pergunta_lower or "preco minimo" in pergunta_lower: possible_vd_key = "preco_minimo"
    elif "preço médio" in pergunta_lower or "preco medio" in pergunta_lower: possible_vd_key = "preco_medio"
    elif "código" in pergunta_lower or "codigo" in pergunta_lower or "ticker" in pergunta_lower or "negociação" in pergunta_lower: possible_vd_key = "codigo"
    elif "preço" in pergunta_lower or "preco" in pergunta_lower or "cotação" in pergunta_lower or "cotacao" in pergunta_lower or "valor" in pergunta_lower: possible_vd_key = "preco_fechamento" # Default
    elif "volume total negociado" in pergunta_lower or "volume negociado" in pergunta_lower: possible_vd_key = "volume_total_negociado"
    elif "volume" in pergunta_lower: possible_vd_key = "volume_total_negociado"
    elif "quantidade" in pergunta_lower and ("negociada" in pergunta_lower or "papeis" in pergunta_lower or "acoes" in pergunta_lower): possible_vd_key = "quantidade_negociada"
    elif "total de negocios" in pergunta_lower or "numero de negocios" in pergunta_lower: possible_vd_key = "total_negocios"
    # Adicione outros elifs para indicadores (use as chaves do resultado_similaridade.txt)
    # elif "p/l" in pergunta_lower or "pl" in pergunta_lower: possible_vd_key = "pl"
    # elif "dividend yield" in pergunta_lower or "dy" in pergunta_lower: possible_vd_key = "dy"

    if possible_vd_key:
        _logger.debug(f"  - Keyword encontrada indica chave: '{possible_vd_key}'")
        if possible_vd_key in dicionario_sinonimos: # Verifica se chave existe no dicionário de sinônimos
             valor_desejado_chave = possible_vd_key
             _logger.info(f"  - Map #VALOR_DESEJADO (Keyword): '{valor_desejado_chave}'")
        else: _logger.error(f"  - Chave '{possible_vd_key}' não existe em resultado_similaridade.txt!")
    else: _logger.warning("  - Nenhuma keyword para #VALOR_DESEJADO encontrada.") # Aviso, pode ser ok para alguns templates

    # Mapeamento final de VD (só adiciona se encontrou e validou)
    if valor_desejado_chave: mapeamentos['#VALOR_DESEJADO'] = valor_desejado_chave
    else: _logger.error("  - FALHA CRÍTICA AO MAPEAR #VALOR_DESEJADO.") # Erro se não encontrou

    # ... (Mapear Tipo Ação e Setor, igual antes) ...

    _logger.info(f"Mapeamentos finais: {mapeamentos}"); return mapeamentos
# --- FIM FUNÇÃO MAPEAMENTO ---

# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    _logger.info(f"--- ================================= ---")
    _logger.info(f"--- Iniciando PLN Script (PID: {os.getpid()}) ---")
    if len(sys.argv) > 1: pergunta_usuario = " ".join(sys.argv[1:]); _logger.info(f"Pergunta: '{pergunta_usuario}'")
    else: fail_with_json_error("Nenhuma pergunta fornecida.")
    _logger.info("Configs carregadas.")
    _logger.info("Processando NLP...");
    try: elementos_nlp, entidades_nomeadas_nlp = extrair_entidades_spacy(pergunta_usuario); data_extraida_nlp = extrair_data(pergunta_usuario); _logger.info("NLP concluído.")
    except Exception as e_nlp: fail_with_json_error("Erro NLP.", e_nlp)
    _logger.info("Buscando template...");
    templates_linhas_interesse = carregar_arquivo_linhas(CAMINHO_PERGUNTAS_INTERESSE)
    template_nome, similaridade, _ = encontrar_pergunta_similar(pergunta_usuario, templates_linhas_interesse, limiar=0.65)
    if not template_nome: fail_with_json_error("Pergunta não compreendida (template).", details=f"Sim max: {similaridade:.2f}")
    _logger.info(f"Template '{template_nome}' selecionado (Sim: {similaridade:.4f}).")
    _logger.info("Mapeando placeholders...");
    dicionario_sinonimos = carregar_dicionario_sinonimos(CAMINHO_DICIONARIO_SINONIMOS)
    try: mapeamentos_semanticos = mapear_para_placeholders(pergunta_usuario, elementos_nlp, entidades_nomeadas_nlp, data_extraida_nlp, dicionario_sinonimos, empresa_nome_map) # Passa mapa nomes
    except Exception as e_map: fail_with_json_error("Erro inesperado mapeamento.", e_map)
    _logger.info("Mapeamento concluído.")
    _logger.info("Validando placeholders...");
    placeholders_requeridos_por_template = {
        "Template 1A": {"#ENTIDADE_NOME#", "#DATA", "#VALOR_DESEJADO"}, "Template 1B": {"#ENTIDADE_NOME#", "#DATA", "#VALOR_DESEJADO"},
        "Template 2A": {"#ENTIDADE_NOME#", "#VALOR_DESEJADO"}, "Template 3A": {"#SETOR", "#VALOR_DESEJADO"},
    };
    placeholders_requeridos = placeholders_requeridos_por_template.get(template_nome, set())
    placeholders_encontrados = set(mapeamentos_semanticos.keys())
    placeholders_faltando = placeholders_requeridos - placeholders_encontrados
    if placeholders_faltando:
         _logger.error(f"VALIDAÇÃO FALHOU '{template_nome}': Faltando OBRIGATÓRIOS: {sorted(list(placeholders_faltando))}")
         fail_with_json_error(f"Não foi possível extrair: {', '.join(sorted(list(placeholders_faltando)))}.", status_code=1)
    else: _logger.info(f"Validação OK '{template_nome}'.")
    _logger.info("Construindo JSON de resposta...");
    debug_info = { "pergunta_original": pergunta_usuario, "elementos_extraidos_nlp": elementos_nlp, "entidades_nomeadas_nlp": entidades_nomeadas_nlp, "data_extraida_nlp": data_extraida_nlp, "similaridade_template": round(similaridade, 4), "template_requeridos": sorted(list(placeholders_requeridos)) }
    resposta_final_stdout = { "template_nome": template_nome, "mapeamentos": mapeamentos_semanticos, "_debug_info": debug_info };
    try: json_output_string = json.dumps(resposta_final_stdout, ensure_ascii=False, indent=None); _logger.debug(f"JSON final: {json_output_string}")
    except Exception as e_json_final: fail_with_json_error("Erro gerar JSON final.", e_json_final)
    _logger.info("Enviando resposta para stdout...");
    try: print(json_output_string); sys.stdout.flush()
    except Exception as e_print: _logger.critical(f"Erro CRÍTICO print stdout: {e_print}", exc_info=True); print(f'{{"erro": "Erro critico print.", "detalhes": "{str(e_print)[:100]}..."}}', file=sys.stderr); sys.exit(1)
    _logger.info(f"--- Processamento PLN Script concluído com sucesso ---"); sys.exit(0)
