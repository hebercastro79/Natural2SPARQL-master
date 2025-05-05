# -*- coding: utf-8 -*-
import spacy
import difflib
import re
import unicodedata
import logging
import sys
import json
import os
from datetime import datetime, timedelta
import io
from pathlib import Path
import traceback

# --- Configuração Inicial Essencial (Codificação) ---
try: sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace'); sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace'); _logger_stream_handler_stderr = sys.stderr
except Exception as e: print(f"AVISO UTF-8: {e}", file=sys.__stderr__); _logger_stream_handler_stderr = sys.__stderr__

# --- DEFINIÇÃO DO LOGGER E FUNÇÃO DE ERRO ---
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_logger = logging.getLogger("PLN_Processor"); _logger.setLevel(logging.DEBUG);
if _logger.hasHandlers(): _logger.handlers.clear()
try: _SCRIPT_DIR_PATH = Path(__file__).parent.resolve()
except NameError: _SCRIPT_DIR_PATH = Path(".").resolve()
_log_file_path = _SCRIPT_DIR_PATH / 'pln_processor.log'
try: file_handler = logging.FileHandler(_log_file_path, encoding='utf-8', mode='a'); file_handler.setFormatter(log_formatter); file_handler.setLevel(logging.DEBUG); _logger.addHandler(file_handler); _logger.info(f"--- LOG INICIADO: {_log_file_path} ---")
except (OSError, IOError) as e: print(f"AVISO log file '{_log_file_path}': {e}", file=sys.__stderr__)
try: stderr_handler = logging.StreamHandler(_logger_stream_handler_stderr); stderr_handler.setFormatter(log_formatter); stderr_handler.setLevel(logging.INFO); _logger.addHandler(stderr_handler)
except Exception as e: print(f"AVISO logger stderr: {e}", file=sys.__stderr__)

def fail_with_json_error(error_message, details=None, status_code=1):
    error_payload = {"erro": str(error_message)};
    if details:
        try: details_str = str(details); error_payload["detalhes"] = details_str[:500] + ('...' if len(details_str) > 500 else '')
        except Exception: error_payload["detalhes"] = "Erro serializar detalhes."
    _logger.error(f"Finalizando c/ erro (status {status_code}): {error_message}", exc_info=(isinstance(details, BaseException)))
    if details and not isinstance(details, BaseException): _logger.error(f"  Detalhes: {error_payload.get('detalhes', '')}")
    try: print(json.dumps(error_payload, ensure_ascii=False)); sys.stdout.flush()
    except Exception as e: _logger.critical(f"Erro CRÍTICO gerar JSON erro: {e}"); print(f'{{"erro": "Erro gerar JSON.", "detalhes": "{str(e)[:100]}..."}}'); sys.stdout.flush()
    sys.exit(status_code)

# --- Carregamento Modelos NLP ---
try: _logger.info("Carregando spaCy..."); nlp = spacy.load("pt_core_news_sm"); _logger.info("spaCy carregado.")
except Exception as e: fail_with_json_error("Erro NLP (spaCy).", e)

# --- Constantes e Caminhos ---
RESOURCES_DIR = _SCRIPT_DIR_PATH
CAMINHO_DICIONARIO_SINONIMOS = RESOURCES_DIR / 'resultado_similaridade.txt'
CAMINHO_PERGUNTAS_INTERESSE = RESOURCES_DIR / 'perguntas_de_interesse.txt'
JSON_MAP_FILENAME = 'empresa_nome_map.json'
CAMINHO_MAPA_EMPRESAS_JSON_ABS = RESOURCES_DIR / JSON_MAP_FILENAME
_logger.info(f"Dir script: {_SCRIPT_DIR_PATH}"); _logger.info(f"Dic sinonimos: {CAMINHO_DICIONARIO_SINONIMOS}"); _logger.info(f"Perguntas: {CAMINHO_PERGUNTAS_INTERESSE}"); _logger.info(f"Mapa JSON: {CAMINHO_MAPA_EMPRESAS_JSON_ABS}")

# --- Carregamento do Mapa de Empresas JSON - CORRIGIDO v2 ---
empresa_nome_map = {}
map_path_to_load = None # Inicializa fora do try
fallback_path = None # Inicializa fora do try
try:
    # Tenta carregar do caminho absoluto esperado (diretório do script)
    if CAMINHO_MAPA_EMPRESAS_JSON_ABS.is_file():
        map_path_to_load = CAMINHO_MAPA_EMPRESAS_JSON_ABS
    else:
        _logger.warning(f"'{CAMINHO_MAPA_EMPRESAS_JSON_ABS}' não encontrado. Tentando './{JSON_MAP_FILENAME}'")
        # Define fallback_path ANTES de usá-lo no if
        fallback_path = Path(f"./{JSON_MAP_FILENAME}").resolve();
        # Verifica se o fallback existe
        if fallback_path.is_file():
            map_path_to_load = fallback_path # Usa o fallback se existir
        else:
            # Se nem o original nem o fallback existem, falha
            fail_with_json_error(f"'{JSON_MAP_FILENAME}' não encontrado.", details=f"Verificado: '{CAMINHO_MAPA_EMPRESAS_JSON_ABS}' e '{fallback_path}'")

    # Se map_path_to_load foi definido (encontrou em um dos locais)
    if map_path_to_load:
        _logger.info(f"Abrindo mapa JSON de: {map_path_to_load}")
        with open(map_path_to_load, 'r', encoding='utf-8') as f_json:
            empresa_nome_map_raw = json.load(f_json);
            empresa_nome_map = {str(key).upper(): val for key, val in empresa_nome_map_raw.items()}
        _logger.info(f"Carregados {len(empresa_nome_map)} mapeamentos JSON.");
        if empresa_nome_map: _logger.debug(f" Exemplo: {next(iter(empresa_nome_map.items()))}")
    # Este else não deveria ser alcançado devido ao fail_with_json_error acima, mas por segurança:
    else:
         # Adiciona log extra se map_path_to_load for None inesperadamente
         _logger.error(f"ERRO INTERNO: map_path_to_load é None após verificações.")
         fail_with_json_error(f"'{JSON_MAP_FILENAME}' não encontrado após verificações.", details=f"Verificado: '{CAMINHO_MAPA_EMPRESAS_JSON_ABS}' e fallback '{fallback_path}'")

except FileNotFoundError: # Captura especificamente se o open() falhar
     fail_with_json_error(f"'{JSON_MAP_FILENAME}' não encontrado no caminho final.", details=f"{map_path_to_load}")
except json.JSONDecodeError as e: fail_with_json_error(f"Erro decodificar JSON ('{map_path_to_load}').", e)
except Exception as e: fail_with_json_error("Erro carregar mapa.", e)
# --- FIM DA CORREÇÃO ---


pronomes_interrogativos = ['quem', 'o que', 'que', 'qual', 'quais', 'quanto', 'quantos', 'onde', 'como', 'quando', 'por que', 'porquê']

# --- Funções Auxiliares ---
def normalize_key(text):
    if not isinstance(text, str): return None
    t_up = text.upper().strip();
    try: t_norm = ''.join(c for c in unicodedata.normalize('NFD', t_up) if unicodedata.category(c) != 'Mn')
    except Exception: _logger.warning(f" Falha acentos '{t_up[:50]}...'"); t_norm = t_up
    t_clean = re.sub(r'\b(S\.?A\.?|S/?A|CIA\.?|COMPANHIA|LTDA\.?|ON|PN|N[12]|PREF\.?|ORD\.?|NM|ED|EJ|MA|HOLDING|PARTICIPACOES|PART)\b', '', t_norm, flags=re.IGNORECASE)
    t_alnum = re.sub(r'[^\w]', '', t_clean); final = re.sub(r'\s+', '', t_alnum).strip(); return final if final else None

def carregar_arquivo_linhas(caminho: Path):
    caminho_str = str(caminho.resolve()); _logger.debug(f"Carregando linhas: {caminho_str}")
    try:
        if not caminho.is_file(): raise FileNotFoundError(f"Não é arquivo: {caminho_str}")
        content = None
        for enc in ['utf-8', 'latin-1', 'windows-1252']:
            try: content = caminho.read_text(encoding=enc); _logger.info(f"'{caminho.name}' lido com {enc}."); break
            except UnicodeDecodeError: _logger.debug(f"Falha {enc} p/ '{caminho.name}'."); continue
            except Exception as e: raise IOError(f"Erro leitura {enc}") from e
        if content is None: raise IOError(f"Não decodificou '{caminho.name}'.")
        if content.startswith('\ufeff'): content = content[1:]
        linhas = [ln.strip() for ln in content.splitlines() if ln.strip()]
        if not linhas: _logger.warning(f"'{caminho.name}' vazio.")
        return linhas
    except FileNotFoundError: fail_with_json_error(f"Erro: Arquivo '{caminho.name}' não encontrado.", details=f"{caminho_str}")
    except Exception as e: fail_with_json_error(f"Erro carregar/ler '{caminho.name}'.", e)

def carregar_dicionario_sinonimos(caminho: Path):
    dicionario = {}; linhas = carregar_arquivo_linhas(caminho)
    _logger.info(f"Processando dicionário '{caminho.name}' ({len(linhas)} linhas)...")
    try:
        r_chave = re.compile(r"^\s*([\w_]+)\s*=\s*\[(.*)\]\s*$"); r_val = re.compile(r"\(\s*'((?:[^'\\]|\\.)*)'\s*,\s*([\d\.]+)\s*\)")
        for i, linha in enumerate(linhas):
            if not linha or linha.startswith('#'): continue
            m_ch = r_chave.match(linha)
            if m_ch:
                chave = m_ch.group(1).lower().strip(); v_str = m_ch.group(2); s_lista = []
                for m_val in r_val.finditer(v_str):
                    sin = m_val.group(1).replace("\\'", "'").strip(); sc_str = m_val.group(2)
                    try: s_lista.append((sin, float(sc_str)))
                    except ValueError: _logger.warning(f"Score inválido '{sc_str}' p/ '{sin}', chave '{chave}' (L{i+1})")
                if s_lista: dicionario[chave] = s_lista
                else: _logger.warning(f"Nenhum sinônimo p/ chave '{chave}' (L{i+1})")
            else: _logger.warning(f"Formato inválido L{i+1} dic: {linha[:100]}...")
    except Exception as e: fail_with_json_error(f"Erro formato dicionário '{caminho.name}'.", e)
    if not dicionario: _logger.error(f"Dicionário '{caminho.name}' vazio!")
    else: _logger.info(f"Dicionário '{caminho.name}' carregado: {len(dicionario)} chaves.")
    return dicionario

def normalizar_texto(texto):
    if not texto: return ""
    try: t = str(texto).strip().lower(); t = "".join(ch for ch in t if unicodedata.category(ch)[0] not in ('C', 'Z') or ch == ' '); t = ''.join(c for c in unicodedata.normalize('NFD', t) if unicodedata.category(c) != 'Mn'); t = re.sub(r'[^\w\s-]', '', t).strip('-').strip(); t = re.sub(r'\s+', ' ', t); return t
    except Exception as e: _logger.warning(f"Erro normalizar '{texto[:50]}...': {e}"); return str(texto).strip().lower()

def extrair_entidades_spacy(texto):
    spo = {"sujeito": [], "predicado": [], "objeto": []}; ner = {}
    _logger.debug(f"spaCy para: '{texto[:100]}...'")
    if not texto: _logger.warning("Texto vazio p/ spaCy."); return spo, ner
    try:
        doc = nlp(texto); _logger.debug(f"  NER (spaCy):")
        for ent in doc.ents:
            lbl=ent.label_; txt=ent.text.strip().rstrip('?.!,;')
            if txt and not txt.isdigit(): _logger.debug(f"    - '{txt}' ({lbl})"); ner.setdefault(lbl, []).append(txt)
            else: _logger.debug(f"    - Ignorando: '{ent.text}' ({lbl})")
        for lbl in ner: ner[lbl] = sorted(list(set(ner[lbl])), key=texto.find)
        _logger.debug(f"  NER (final): {ner}"); _logger.debug(f"  SPO (simplif.):")
        root = None
        for t in doc:
             if t.dep_=="ROOT" and t.pos_=="VERB": root=t; spo["predicado"].append(t.lemma_); _logger.debug(f"    -> Pred: {t.lemma_}")
             if t.dep_ in ("nsubj","nsubj:pass") and t.text.lower() not in pronomes_interrogativos:
                 if t.head==root or (t.head.pos_=="AUX" and t.head.head==root): spo["sujeito"].append(t.text); _logger.debug(f"    -> Suj: {t.text}")
             if t.dep_ in ("obj","dobj","iobj","pobj","obl","attr","acomp") and t.head==root: spo["objeto"].append(t.text); _logger.debug(f"    -> Obj: {t.text}")
        for k in spo: spo[k] = sorted(list(set(spo[k])), key=texto.find)
        _logger.info(f"spaCy OK. SPO: {spo}, NER: {ner}")
    except Exception as e: _logger.error(f"Erro GERAL spaCy: {e}", exc_info=True); spo={"sujeito": [], "predicado": [], "objeto": []}; ner={}
    return spo, ner

# --- FUNÇÃO extrair_data CORRIGIDA (Sintaxe v7 - Final) ---
def extrair_data(texto):
    _logger.debug(f"Extraindo data de: '{texto}'")
    padrao_dma = r'\b(\d{1,2})\s*[/.-]\s*(\d{1,2})\s*[/.-]\s*(\d{4})\b'
    padrao_amd = r'\b(\d{4})\s*[/.-]\s*(\d{1,2})\s*[/.-]\s*(\d{1,2})\b'
    m_dma = re.search(padrao_dma, texto)
    m_amd = re.search(padrao_amd, texto)
    m = None
    is_dma = False
    if m_dma and m_amd: m, is_dma = (m_dma, True) if m_dma.start() <= m_amd.start() else (m_amd, False)
    elif m_dma: m, is_dma = m_dma, True
    elif m_amd: m = m_amd

    if m:
        date_str_matched = m.group(0)
        try:
            g = m.groups()
            d, mo, a = (int(g[0]), int(g[1]), int(g[2])) if is_dma else (int(g[2]), int(g[1]), int(g[0]))

            # Validação básica dos componentes antes de criar datetime
            if not (1 <= mo <= 12):
                 _logger.warning(f"Mês inválido ({mo}) em '{date_str_matched}'")
                 raise ValueError("Mês fora do intervalo")
            if not (1 <= d <= 31): # Validação simples do dia
                 _logger.warning(f"Dia inválido ({d}) em '{date_str_matched}'")
                 raise ValueError("Dia fora do intervalo")
            if not (1990 <= a <= datetime.now().year + 5):
                 _logger.warning(f"Ano inválido ({a}) em '{date_str_matched}'")
                 raise ValueError("Ano fora do intervalo")

            # Tenta criar o objeto datetime (validação final, ex: dia 31/02)
            dt_obj = datetime(a, mo, d)
            # Formata a data
            dt_fmt = dt_obj.strftime("%Y-%m-%d")
            _logger.info(f"Data explícita encontrada: '{date_str_matched}' -> {dt_fmt}")
            return dt_fmt

        except ValueError as ve:
            # Erro na validação dos componentes ou na criação do datetime
            # O log já foi feito dentro do try para validações específicas
            if "Componentes" not in str(ve) and "intervalo" not in str(ve): # Evita log duplicado
                 _logger.warning(f"Data inválida (ValueError) em '{date_str_matched}': {ve}")
            return None
        except Exception as e_conv:
            # Outro erro inesperado
            _logger.warning(f"Erro inesperado ao converter data '{date_str_matched}': {e_conv}")
            return None
    else:
        # Verifica keywords se regex falhou
        t_low = texto.lower()
        if "hoje" in t_low or "hj" in t_low: dt_hoje = datetime.now().strftime("%Y-%m-%d"); _logger.info(f"Keyword 'hoje'. Data: {dt_hoje}"); return dt_hoje
        if "ontem" in t_low: dt_ontem = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"); _logger.info(f"Keyword 'ontem'. Data: {dt_ontem}"); return dt_ontem

    _logger.info("Nenhuma data encontrada no texto.")
    return None
# --- FIM DA CORREÇÃO ---

def encontrar_termo_dicionario(frase, dicionario, limiar=0.70):
    f_norm=normalizar_texto(frase);
    if not dicionario or not f_norm: _logger.debug(f"Dic/Frase vazia p/ '{frase}'."); return None
    k_best=None; s_max=limiar-0.01; txt_match=None; _logger.debug(f"Buscando termo p/ '{frase}' ('{f_norm}') no dic (lim {limiar:.2f})...")
    for ch, s_list in dicionario.items():
        s_ch=difflib.SequenceMatcher(None,f_norm,ch).ratio(); _logger.debug(f" '{f_norm}' vs '{ch}': {s_ch:.4f}")
        if s_ch > s_max: s_max=s_ch; k_best=ch; txt_match=ch; _logger.debug(f"   -> Melhor (chave): '{ch}', {s_ch:.4f}")
        for sin,_ in s_list:
            s_n=normalizar_texto(sin);
            if s_n: s_s=difflib.SequenceMatcher(None,f_norm,s_n).ratio(); _logger.debug(f" '{f_norm}' vs '{sin}' ('{s_n}') de '{ch}': {s_s:.4f}")
            if s_s > s_max: s_max=s_s; k_best=ch; txt_match=sin; _logger.debug(f"   -> Melhor (sin): Chave='{ch}', Sin='{sin}', {s_s:.4f}")
    if k_best: _logger.info(f"Termo p/ '{frase}': Chave='{k_best}' (Match '{txt_match}', Sim: {s_max:.2f})"); return k_best
    else: _logger.info(f"Nenhum termo >= {limiar:.2f} p/ '{frase}'."); return None

# --- FUNÇÃO encontrar_pergunta_similar CORRIGIDA (Sintaxe v4 - Final) ---
def encontrar_pergunta_similar(pergunta_usuario, templates_linhas, limiar=0.65):
    sim_max=0.0; t_nome=None; t_linha=None
    if not templates_linhas: _logger.error("Lista templates vazia."); return None,0,None
    pu_norm=normalizar_texto(pergunta_usuario);
    if not pu_norm: _logger.error("Pergunta normalizada vazia."); return None,0,None
    _logger.debug(f"Buscando template p/ '{pu_norm}' (Lim {limiar:.2f}) em {len(templates_linhas)} linhas...")
    for i, linha_t in enumerate(templates_linhas):
        if not linha_t or linha_t.startswith('#') or " - " not in linha_t:
            _logger.debug(f" Ignorando L{i+1}: {linha_t[:50]}...")
            continue
        # Bloco try agora engloba o processamento da linha
        try:
            n_t, ex_t = [p.strip() for p in linha_t.split(" - ", 1)]; # Linha 228/236 original causava erro aqui
            # As verificações e lógica agora estão DENTRO do try
            if n_t and ex_t:
                 ex_n=normalizar_texto(ex_t);
                 if ex_n:
                     sim=difflib.SequenceMatcher(None,pu_norm,ex_n).ratio()
                     _logger.debug(f" -> '{n_t}' ('{ex_n}'): Sim={sim:.4f}");
                     if sim > sim_max:
                         sim_max=sim; t_nome=n_t; t_linha=linha_t;
                         _logger.debug(f"   --> Melhor: '{n_t}', {sim:.4f}")
                 # else: _logger.debug(f" Exemplo L{i+1} ('{ex_n}') não melhorou.")
            else:
                 _logger.warning(f" Formato inválido L{i+1}: '{linha_t}'")
        # except agora está corretamente posicionado após o try
        except Exception as e:
            _logger.warning(f"Erro processar linha {i+1}: {e}", exc_info=False)

    # Resto da função permanece igual
    if sim_max < limiar: _logger.warning(f"Sim max ({sim_max:.2f}) < limiar ({limiar:.2f})."); return None,sim_max,None
    _logger.info(f"Template selecionado: '{t_nome}', Sim={sim_max:.4f}"); _logger.debug(f" Linha: '{t_linha}'"); return t_nome,sim_max,t_linha
# --- FIM DA CORREÇÃO ---

def mapear_para_placeholders(pergunta_usuario_original, elementos, entidades_nomeadas, data, dicionario_sinonimos, nome_map):
    map = {}; _logger.debug("Mapeando placeholders...")
    if data: map['#DATA#'] = data; _logger.debug(f"  - Map #DATA#: '{data}'")
    else: _logger.debug("  - #DATA# não mapeado.")
    nome_canon = None; ner_orig = None; tipo_detec = "Nenhum"
    keys_up = set(nome_map.keys()); ner_prio = ['ORG','PRODUCT','WORK_OF_ART','GPE','LOC','PERSON','MISC']
    ner_flat = [];
    for lbl in ner_prio:
        if lbl in entidades_nomeadas: ner_flat.extend([(ent, lbl) for ent in entidades_nomeadas[lbl]])
    for lbl, ents in entidades_nomeadas.items():
        if lbl not in ner_prio: ner_flat.extend([(ent, lbl) for ent in ents])
    _logger.debug(f"  - Entidades NER p/ busca: {ner_flat}")
    for ent_txt, ent_lbl in ner_flat:
        ent_up = ent_txt.upper().strip(); ent_n = normalize_key(ent_txt)
        _logger.debug(f"    Validando NER: '{ent_txt}' ({ent_lbl}) -> Up: '{ent_up}', Norm: '{ent_n}'")
        if ent_up in keys_up: nome_canon = nome_map[ent_up]; ner_orig = ent_txt; tipo_detec = f"NER {ent_lbl} (Match Exato)"; _logger.debug(f"    --> MATCH EXATO! Canônico='{nome_canon}'"); break
        if ent_n and ent_n != ent_up and ent_n in keys_up:
            if nome_canon is None: nome_canon = nome_map[ent_n]; ner_orig = ent_txt; tipo_detec = f"NER {ent_lbl} (Match Norm)"; _logger.debug(f"    --> MATCH NORM (primeiro)! Canônico='{nome_canon}'")
    if nome_canon: map['#ENTIDADE_NOME#'] = " ".join(nome_canon.split()); _logger.info(f"  - Map #ENTIDADE_NOME#: '{map['#ENTIDADE_NOME#']}' (Origem: '{ner_orig}', Tipo: {tipo_detec})")
    elif ner_orig: map['#ENTIDADE_NOME#'] = " ".join(ner_orig.split()); _logger.warning(f"  - AVISO: #ENTIDADE_NOME# usando NER bruto '{map['#ENTIDADE_NOME#']}'.")
    else: _logger.error("  - FALHA CRÍTICA: #ENTIDADE_NOME# não identificado.")
    vd_chave = None; perg_low = pergunta_usuario_original.lower(); _logger.debug(f"  - Buscando keyword #VALOR_DESEJADO# em: '{perg_low}'")
    kw_map = [ (["preço de abertura", "preco de abertura", "abertura"], "preco_abertura"), (["preço de fechamento", "preco de fechamento", "fechamento"], "preco_fechamento"), (["preço máximo", "preco maximo", "máximo", "maximo"], "preco_maximo"), (["preço mínimo", "preco minimo", "mínimo", "minimo"], "preco_minimo"), (["preço médio", "preco medio", "médio", "medio"], "preco_medio"), (["código", "codigo", "ticker", "negociação", "negociacao"], "codigo"), (["volume"], "volume"), ]; fb_kw_map = [ (["preço", "preco", "cotação", "cotacao", "valor"], "preco_fechamento") ]
    for kws, dk in kw_map:
        for kw in kws:
            if kw in perg_low: vd_chave = dk; _logger.debug(f"  - Keyword ('{kw}') -> chave: '{vd_chave}'"); break
        if vd_chave: break
    if not vd_chave:
        _logger.debug("  - Nenhuma keyword específica, tentando fallbacks...")
        for kws, dk in fb_kw_map:
            for kw in kws:
                 if kw in perg_low: vd_chave = dk; _logger.debug(f"  - Keyword fallback ('{kw}') -> chave: '{vd_chave}'"); break
            if vd_chave: break
    if vd_chave:
        if dicionario_sinonimos and vd_chave in dicionario_sinonimos: map['#VALOR_DESEJADO#'] = vd_chave; _logger.info(f"  - Map #VALOR_DESEJADO#: '{vd_chave}' (Validado)")
        elif not dicionario_sinonimos: _logger.error(f"  - ERRO: Dic. sinônimos não carregado. Chave '{vd_chave}' não validada.")
        else: _logger.error(f"  - ERRO: Chave '{vd_chave}' NÃO existe em '{CAMINHO_DICIONARIO_SINONIMOS.name}'!")
    else: _logger.warning("  - Nenhuma keyword para #VALOR_DESEJADO# encontrada/validada.")
    _logger.info(f"Mapeamentos finais: {map}"); return map

# --- Bloco Principal de Execução CORRIGIDO (Sintaxe FINAL) ---
if __name__ == "__main__":
    _logger.info(f"--- ================================= ---"); _logger.info(f"--- INICIANDO PLN_PROCESSOR.PY (PID: {os.getpid()}) ---"); _logger.info(f"--- Args: {sys.argv} ---"); _logger.info(f"--- CWD: {Path('.').resolve()} ---"); _logger.info(f"--- Script Dir: {_SCRIPT_DIR_PATH} ---")
    if len(sys.argv) > 1: pergunta_usuario = " ".join(sys.argv[1:]); _logger.info(f"Pergunta: '{pergunta_usuario}'")
    else: fail_with_json_error("Nenhuma pergunta fornecida.")

    # 1. NLP
    _logger.info("Passo 1: NLP...")
    try:
        elementos_nlp, entidades_nomeadas_nlp = extrair_entidades_spacy(pergunta_usuario)
        data_extraida_nlp = extrair_data(pergunta_usuario)
        _logger.info("NLP OK.")
    except Exception as e:
        fail_with_json_error("Erro NLP.", e)

    # 2. Template
    _logger.info("Passo 2: Template...")
    try:
        templates_linhas = carregar_arquivo_linhas(CAMINHO_PERGUNTAS_INTERESSE)
        t_nome, sim, _ = encontrar_pergunta_similar(pergunta_usuario, templates_linhas, limiar=0.65)
        if not t_nome:
            fail_with_json_error("Pergunta não compreendida (template).", details=f"Sim max: {sim:.2f}")
        _logger.info(f"Template: '{t_nome}' (Sim: {sim:.4f}).")
    except Exception as e:
        fail_with_json_error("Erro template.", e)

    # 3. Mapeamento
    _logger.info("Passo 3: Mapeamento...")
    try:
        dic_sin = carregar_dicionario_sinonimos(CAMINHO_DICIONARIO_SINONIMOS)
        map_sem = mapear_para_placeholders(pergunta_usuario, elementos_nlp, entidades_nomeadas_nlp, data_extraida_nlp, dic_sin, empresa_nome_map)
        _logger.info("Mapeamento OK.")
    except Exception as e:
        fail_with_json_error("Erro mapeamento.", e)

    # 4. Validação - CORRIGIDA v3
    _logger.info("Passo 4: Validando placeholders...")
    req_map = {"Template 1A": {"#DATA#", "#ENTIDADE_NOME#", "#VALOR_DESEJADO"}, "Template 1B": {"#DATA#", "#ENTIDADE_NOME#", "#VALOR_DESEJADO"}, "Template 2A": {"#ENTIDADE_NOME#", "#VALOR_DESEJADO"}}
    req_set = req_map.get(t_nome, set()); enc_list = list(map_sem.keys()); falt_list = [];
    _logger.debug(f"  >> DEBUG VAL: Tpl={t_nome}, Req={req_set}, Enc={enc_list}")
    for req in req_set:
        if req not in enc_list: _logger.debug(f"    --> Faltando: '{req}'"); falt_list.append(req)
        else: _logger.debug(f"    --> Encontrado: '{req}'")
    _logger.debug(f"  >> DEBUG VAL: Faltando (final) = {falt_list}")
    if falt_list: erro_msg = f"Não foi possível extrair: {', '.join(sorted(falt_list))}."; _logger.error(f"VALIDAÇÃO FALHOU '{t_nome}': Faltando: {sorted(falt_list)}. Req: {req_set}. Enc: {enc_list}"); fail_with_json_error(erro_msg, details=f"Tpl:{t_nome}, Req:{req_set}, Enc:{enc_list}", status_code=1)
    else: _logger.info(f"Validação placeholders OK para '{t_nome}'.")

    # 5. Resposta JSON
    _logger.info("Passo 5: Construindo JSON resposta..."); debug_info = { "q_orig": pergunta_usuario, "q_norm": normalizar_texto(pergunta_usuario), "spo": elementos_nlp, "ner": entidades_nomeadas_nlp, "data": data_extraida_nlp, "tpl_sim": round(sim, 4), "req": sorted(list(req_set)) }
    resp_final = { "template_nome": t_nome, "mapeamentos": map_sem, "_debug_info": debug_info };
    try: json_out = json.dumps(resp_final, ensure_ascii=False, indent=None); _logger.debug(f"JSON final: {json_out}")
    except Exception as e: fail_with_json_error("Erro gerar JSON final.", e)

    # 6. Envio
    _logger.info("Passo 6: Enviando resposta p/ stdout...");
    try: print(json_out); sys.stdout.flush()
    except Exception as e: _logger.critical(f"Erro CRÍTICO print stdout: {e}", exc_info=True); print(f'{{"erro": "Erro enviar JSON.", "detalhes": "{str(e)[:100]}..."}}', file=sys.__stderr__); sys.exit(1)

    _logger.info(f"--- Processamento PLN Script concluído com sucesso ---"); sys.exit(0)
# --- FIM DO ARQUIVO ---