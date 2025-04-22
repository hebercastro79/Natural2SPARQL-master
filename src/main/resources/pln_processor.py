import nltk
import spacy
import difflib
import re
import unicodedata
from nltk.stem.rslp import RSLPStemmer
import logging
import sys
import json
import os
from datetime import datetime, timedelta
import io
from pathlib import Path # Usar pathlib para caminhos mais robustos

# --- Configuração Inicial Essencial ---
# Tenta garantir que stdout/stderr usem UTF-8 e substituam erros.
try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
except Exception as e_enc:
    print(f"AVISO URGENTE: Não foi possível forçar UTF-8 em stdout/stderr. Erro: {e_enc}", file=sys.__stderr__)

# --- Configuração de Logging (Arquivo e Stderr) ---
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_logger = logging.getLogger("PLN_Processor")
_logger.setLevel(logging.DEBUG)

if _logger.hasHandlers():
    _logger.handlers.clear()

_SCRIPT_DIR_PATH = Path(__file__).parent.resolve()
_log_file_path = _SCRIPT_DIR_PATH / 'pln_processor.log'

try:
    file_handler = logging.FileHandler(_log_file_path, encoding='utf-8', mode='w')
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.DEBUG)
    _logger.addHandler(file_handler)
except Exception as e:
    print(f"AVISO URGENTE: Não foi possível criar/abrir o arquivo de log '{_log_file_path}'. Erro: {e}", file=sys.__stderr__)

try:
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(log_formatter)
    stderr_handler.setLevel(logging.INFO)
    _logger.addHandler(stderr_handler)
except Exception as e:
    print(f"AVISO: Não foi possível configurar logging para stderr. Erro: {e}", file=sys.stderr)

# -----------------------------------------------------

# Função para imprimir JSON de erro e sair (usa stdout)
def fail_with_json_error(error_message, details=None, status_code=1):
    """Loga o erro detalhado e imprime um JSON de erro para stdout antes de sair."""
    error_payload = {"erro": error_message}
    if details:
        try:
            details_str = str(details)
            if len(details_str) > 500:
                 details_str = details_str[:497] + "..."
            error_payload["detalhes"] = details_str
        except Exception:
            error_payload["detalhes"] = "Erro ao serializar detalhes do erro."
    _logger.error(f"Finalizando com erro: {error_message} - Detalhes: {details}", exc_info=True)
    try:
        print(json.dumps(error_payload, ensure_ascii=False))
    except Exception as json_e:
        _logger.critical(f"Erro CRÍTICO ao gerar JSON de erro final: {json_e}")
        print(f'{{"erro": "Erro critico ao gerar JSON de erro.", "detalhes": "{str(json_e)[:100]}..."}}')
    sys.exit(status_code)

# --- Carregamento de Modelos e Dados NLTK/SpaCy ---
try:
    # --- CORREÇÃO NLTK ---
    nltk_downloader = nltk.downloader.Downloader() # Cria a instância
    nltk_data_path = nltk_downloader.default_download_dir() # CHAMA o método com ()

    _logger.info(f"Verificando/Baixando dados NLTK em: {nltk_data_path}") # Agora loga o caminho correto

    # Tentar criar o diretório se não existir
    if not os.path.exists(nltk_data_path): # Agora usa a string do caminho
        try:
            os.makedirs(nltk_data_path, exist_ok=True)
            _logger.info(f"Diretório NLTK data criado: {nltk_data_path}")
        except OSError as mkdir_err:
             _logger.warning(f"Não foi possível criar diretório NLTK data: {mkdir_err}. Download pode falhar.")

    # Verifica e baixa punkt
    punkt_path = os.path.join(nltk_data_path, 'tokenizers', 'punkt')
    if not os.path.exists(punkt_path):
        _logger.info("Tentando baixar NLTK 'punkt'...")
        nltk_downloader.download('punkt', download_dir=nltk_data_path, quiet=False, raise_on_error=True) # Usa a instância
        _logger.info("'punkt' baixado.")
    else:
        _logger.debug("'punkt' já existe.")

    # Verifica e baixa rslp
    rslp_path = os.path.join(nltk_data_path, 'stemmers', 'rslp')
    if not os.path.exists(rslp_path):
         _logger.info("Tentando baixar NLTK 'rslp'...")
         nltk_downloader.download('rslp', download_dir=nltk_data_path, quiet=False, raise_on_error=True) # Usa a instância
         _logger.info("'rslp' baixado.")
    else:
         _logger.debug("'rslp' já existe.")

    _logger.info("Pacotes NLTK verificados/baixados (ou já existentes).")
    # --- FIM DA CORREÇÃO NLTK ---

except FileNotFoundError as fnf_nltk:
     _logger.error(f"Erro de caminho NLTK: {fnf_nltk}. Tentando com NLTK_DATA...")
     nltk_data_env = os.environ.get('NLTK_DATA')
     if nltk_data_env and os.path.exists(nltk_data_env):
          _logger.info(f"Tentando usar NLTK_DATA={nltk_data_env}")
          nltk.data.path.append(nltk_data_env)
          try:
              nltk.word_tokenize("teste") # Força o uso para ver se funciona
              _logger.info("NLTK parece funcionar com NLTK_DATA.")
          except Exception as nltk_env_err:
               fail_with_json_error("Erro ao usar NLTK mesmo com NLTK_DATA.", nltk_env_err)
     else:
          fail_with_json_error(f"Caminho NLTK padrão falhou ({nltk_data_path}) e NLTK_DATA não definido/válido.")
except Exception as e_nltk_inner:
    fail_with_json_error("Erro ao inicializar NLTK (verifique download/permissões/conexão).", e_nltk_inner)


# Carregamento SpaCy
try:
    nlp = spacy.load("pt_core_news_sm")
    _logger.info("Modelo spaCy 'pt_core_news_sm' carregado.")
except Exception as e_spacy:
    if isinstance(e_spacy, OSError):
         fail_with_json_error("Erro ao carregar modelo spaCy 'pt_core_news_sm'. Modelo não encontrado ou corrompido?", f"Verifique a instalação. Detalhe: {e_spacy}")
    else:
         fail_with_json_error("Erro ao inicializar spaCy.", e_spacy)
# --------------------------------------------

# --- Constantes e Caminhos (Usando Relativos) ---
# Assume que este script está em 'src/main/resources'
RESOURCES_DIR = _SCRIPT_DIR_PATH

CAMINHO_DICIONARIO_SINONIMOS = RESOURCES_DIR / 'resultado_similaridade.txt'
CAMINHO_PERGUNTAS_INTERESSE = RESOURCES_DIR / 'perguntas_de_interesse.txt'
CAMINHO_DICIONARIO_VERBOS = RESOURCES_DIR / 'dicionario_verbos.txt' # Se usado

_logger.info(f"Diretório base do script: {_SCRIPT_DIR_PATH}")
_logger.info(f"Diretório de recursos assumido: {RESOURCES_DIR}")
_logger.info(f"Tentando carregar dicionário de: {CAMINHO_DICIONARIO_SINONIMOS}")
_logger.info(f"Tentando carregar perguntas de: {CAMINHO_PERGUNTAS_INTERESSE}")

if not CAMINHO_PERGUNTAS_INTERESSE.exists():
    _logger.error(f"VERIFICAÇÃO INICIAL FALHOU: Arquivo de perguntas NÃO encontrado em: {CAMINHO_PERGUNTAS_INTERESSE}")
if not CAMINHO_DICIONARIO_SINONIMOS.exists():
    _logger.error(f"VERIFICAÇÃO INICIAL FALHOU: Arquivo de sinônimos NÃO encontrado em: {CAMINHO_DICIONARIO_SINONIMOS}")
# --------------------------------------------------------------------

# --- Lista de pronomes interrogativos ---
pronomes_interrogativos = [
    'quem', 'o que', 'que', 'qual', 'quais', 'quanto',
    'quantos', 'onde', 'como', 'quando', 'por que', 'porquê'
]

# --- Funções Auxiliares ---

def carregar_arquivo_linhas(caminho: Path):
    """Carrega linhas de um arquivo Path, com detecção de encoding e tratamento de erros."""
    caminho_str = str(caminho.resolve())
    _logger.debug(f"Tentando carregar arquivo: {caminho_str}")
    try:
        if not caminho.exists(): raise FileNotFoundError(f"Arquivo não encontrado: {caminho_str}")
        if not caminho.is_file(): raise IOError(f"Não é um arquivo válido: {caminho_str}")
        encodings_to_try = ['utf-8', 'latin-1', 'windows-1252']; content = None; detected_encoding = None; last_exception = None
        for enc in encodings_to_try:
            try:
                content = caminho.read_text(encoding=enc)
                _logger.info(f"Arquivo '{caminho.name}' lido com encoding: {enc}"); detected_encoding = enc; last_exception = None; break
            except UnicodeDecodeError as ude: _logger.debug(f"Falha ao ler '{caminho.name}' com {enc}: {ude}"); last_exception = ude; continue
            except Exception as inner_e: _logger.warning(f"Erro ao tentar ler '{caminho.name}' com {enc}: {inner_e}"); last_exception = inner_e; continue
        if content is None: raise IOError(f"Não foi possível ler '{caminho.name}'") from last_exception
        if detected_encoding == 'utf-8' and content.startswith('\ufeff'): content = content[1:]; _logger.debug(f"Removido BOM UTF-8 de '{caminho.name}'.")
        linhas = [linha.strip() for linha in content.splitlines() if linha.strip()]
        if not linhas: _logger.warning(f"Arquivo '{caminho.name}' vazio ou sem conteúdo útil.")
        else: _logger.debug(f"Carregadas {len(linhas)} linhas de '{caminho.name}'.")
        return linhas
    except FileNotFoundError as fnf_e: fail_with_json_error(f"Arquivo essencial '{caminho.name}' não encontrado.", details=f"Verificado: {caminho_str}")
    except Exception as e: fail_with_json_error(f"Erro crítico ao ler '{caminho.name}'.", e)

def carregar_dicionario_sinonimos(caminho: Path):
    """Carrega dicionário de sinônimos do arquivo Path: chave=[(sinonimo, valor)]."""
    dicionario = {}; linhas = carregar_arquivo_linhas(caminho); caminho_str = str(caminho.resolve())
    _logger.info(f"Processando dicionário de sinônimos de '{caminho.name}'...")
    try:
        linha_num = 0; chave_regex = re.compile(r"^\s*([\w_]+)\s*=\s*\[(.*)\]\s*$"); valor_regex = re.compile(r"\(\s*'((?:[^'\\]|\\.)*)'\s*,\s*([\d\.]+)\s*\)")
        for linha in linhas:
            linha_num += 1;
            if not linha or linha.startswith('#'): continue
            match = chave_regex.match(linha)
            if match:
                chave = match.group(1).lower().strip(); valores_str = match.group(2); valores = []
                for sin_match in valor_regex.finditer(valores_str):
                    sinonimo = sin_match.group(1).replace("\\'", "'").replace("\\\\", "\\").strip(); valor_str = sin_match.group(2)
                    try:
                        valor = float(valor_str)
                        if 0.0 <= valor <= 1.0: valores.append((sinonimo, valor))
                        else: _logger.warning(f"Valor fora [0,1]: {valor} ('{sinonimo}', '{chave}') em '{caminho.name}', L{linha_num}")
                    except ValueError: _logger.warning(f"Valor float inválido: '{valor_str}' ('{sinonimo}', '{chave}') em '{caminho.name}', L{linha_num}")
                if valores:
                    if chave in dicionario: _logger.warning(f"Chave duplicada: '{chave}' em '{caminho.name}', L{linha_num}.")
                    dicionario[chave] = valores; _logger.debug(f"  - Chave '{chave}': {len(valores)} sinônimos.")
                else: _logger.warning(f"Nenhum par válido para '{chave}' em '{caminho.name}', L{linha_num}.")
            else: _logger.warning(f"Formato inválido: '{caminho.name}', L{linha_num}: {linha[:100]}...")
    except Exception as e: fail_with_json_error(f"Erro no formato do dicionário '{caminho.name}'.", e)
    if not dicionario: _logger.error(f"Dicionário '{caminho.name}' vazio!"); # fail_with_json_error(f"Dicionário '{caminho.name}' vazio.")
    _logger.info(f"Dicionário '{caminho.name}' carregado: {len(dicionario)} chaves.")
    return dicionario

def normalizar_texto(texto):
    """Normaliza texto: minúsculas, remove acentos, remove caracteres de controle, pontuação básica."""
    if not texto: return ""
    try:
        texto_norm = str(texto).strip().lower()
        texto_norm = "".join(ch for ch in texto_norm if unicodedata.category(ch)[0] not in ('C', 'Z') or ch == ' ')
        texto_norm = ''.join(c for c in unicodedata.normalize('NFD', texto_norm) if unicodedata.category(c) != 'Mn')
        texto_norm = re.sub(r'[^\w\s-]', '', texto_norm).strip('-')
        texto_norm = re.sub(r'\s+', ' ', texto_norm).strip()
        return texto_norm
    except Exception as e:
        _logger.warning(f"Erro ao normalizar '{texto[:50]}...': {e}"); return str(texto).strip().lower()

def extrair_entidades_spacy(texto):
    """Extrai SPO e NER usando spaCy."""
    elementos = {"sujeito": [], "predicado": [], "objeto": []}; entidades_nomeadas = {}
    _logger.debug(f"Extraindo spaCy de: '{texto[:100]}...'");
    if not texto: return elementos, entidades_nomeadas
    try:
        doc = nlp(texto); processed_tokens_indices = set()
        _logger.debug("  - Análise de dependências SPO...")
        for token in doc:
            if token.i in processed_tokens_indices or token.text.lower() in pronomes_interrogativos: continue
            if token.dep_ in ("nsubj", "nsubj:pass"):
                subtree_tokens = list(token.subtree); sub_phrase = "".join(t.text_with_ws for t in subtree_tokens).strip()
                if sub_phrase and token.text.lower() not in pronomes_interrogativos: elementos["sujeito"].append(sub_phrase); processed_tokens_indices.update(t.i for t in subtree_tokens); _logger.debug(f"    - Suj ({token.dep_}): '{sub_phrase}'")
            elif token.dep_ in ("obj", "dobj", "iobj", "obl"):
                 subtree_tokens = list(token.subtree); obj_phrase = "".join(t.text_with_ws for t in subtree_tokens).strip()
                 if obj_phrase:
                      obj_phrase_cleaned = re.sub(r'\s+(em|no dia)\s+\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$', '', obj_phrase, flags=re.IGNORECASE).strip()
                      if obj_phrase_cleaned: elementos["objeto"].append(obj_phrase_cleaned); processed_tokens_indices.update(t.i for t in subtree_tokens); _logger.debug(f"    - Obj/Obl ({token.dep_}): '{obj_phrase_cleaned}'")
            elif token.pos_ == "VERB" and token.dep_ in ("ROOT", "conj", "xcomp", "ccomp"):
                verbo_lemma = token.lemma_;
                if verbo_lemma not in elementos["predicado"]: elementos["predicado"].append(verbo_lemma); _logger.debug(f"    - Pred ({token.dep_}): '{verbo_lemma}'")
        for key in elementos: unique_elements_dict = {el: texto.find(el) for el in elementos[key] if el}; elementos[key] = sorted(unique_elements_dict, key=unique_elements_dict.get)
        _logger.debug("  - Extração NER...")
        for ent in doc.ents:
            label = ent.label_; text = ent.text.strip().rstrip('.')
            if text and not text.isdigit(): entidades_nomeadas.setdefault(label, []).append(text); _logger.debug(f"    - NER: '{text}' ({label})")
        for label in entidades_nomeadas: unique_ents_dict = {ent: texto.find(ent) for ent in entidades_nomeadas[label]}; entidades_nomeadas[label] = sorted(unique_ents_dict, key=unique_ents_dict.get)
        _logger.info(f"Extração spaCy final. SPO: {elementos}, NER: {entidades_nomeadas}")
    except Exception as e: _logger.error(f"Erro GERAL extração spaCy: {e}", exc_info=True); elementos = {"sujeito": [], "predicado": [], "objeto": []}; entidades_nomeadas = {}
    return elementos, entidades_nomeadas

def extrair_data(texto):
    """Extrai data (vários formatos, hoje, ontem) e normaliza para AAAA-MM-DD."""
    _logger.debug(f"Extraindo data de: '{texto}'"); padrao_dma = r'\b(\d{1,2})\s*[/.-]\s*(\d{1,2})\s*[/.-]\s*(\d{4})\b'; padrao_amd = r'\b(\d{4})\s*[/.-]\s*(\d{1,2})\s*[/.-]\s*(\d{1,2})\b';
    match_dma = re.search(padrao_dma, texto); match_amd = re.search(padrao_amd, texto); data_normalizada = None
    match_prioritario = match_dma if match_dma else match_amd; is_dma = (match_prioritario == match_dma)
    if match_prioritario:
        try:
            groups = match_prioritario.groups();
            dia, mes, ano = (int(groups[0]), int(groups[1]), int(groups[2])) if is_dma else (int(groups[2]), int(groups[1]), int(groups[0]))
            if 1 <= dia <= 31 and 1 <= mes <= 12 and 1900 < ano < 2100:
                data_obj = datetime(ano, mes, dia); data_normalizada = data_obj.strftime("%Y-%m-%d"); _logger.info(f"Data '{match_prioritario.group(0)}' -> {data_normalizada}"); return data_normalizada
            else: _logger.warning(f"Data inválida: d={dia}, m={mes}, a={ano} ('{match_prioritario.group(0)}')")
        except (ValueError, IndexError) as ve: _logger.warning(f"Erro conversão data '{match_prioritario.group(0)}': {ve}")
        except Exception as e_dt: _logger.warning(f"Erro inesperado data '{match_prioritario.group(0)}': {e_dt}")
    texto_lower = texto.lower();
    if "hoje" in texto_lower: _logger.info("'hoje' -> data atual"); return datetime.now().strftime("%Y-%m-%d")
    if "ontem" in texto_lower: _logger.info("'ontem' -> dia anterior"); return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    _logger.info("Nenhuma data reconhecível."); return None

# --- CORREÇÃO UnboundLocalError ---
def encontrar_termo_dicionario(frase, dicionario, limiar=0.70):
    """Encontra a melhor chave no dicionário de sinônimos para uma frase, usando texto normalizado e similaridade."""
    _logger.debug(f"Recebido para buscar termo: '{frase}'")

    # Define frase_norm logo no início, mesmo que seja None ou "" temporariamente
    frase_norm = None # Inicializa como None

    if not dicionario or not frase:
        _logger.debug(f"Dicionário ou frase de entrada vazia.")
        return None

    # Normaliza a frase de entrada AGORA
    frase_norm = normalizar_texto(frase)
    if not frase_norm:
        _logger.debug(f"Frase normalizada resultou vazia.")
        return None

    # Se chegou aqui, frase_norm DEFINITIVAMENTE tem um valor válido (string não vazia)

    melhor_chave = None
    maior_similaridade = limiar - 0.001 # Garante que a primeira > limiar seja pega
    texto_que_deu_match = None

    _logger.debug(f"Buscando termo no dicionário para frase normalizada: '{frase_norm}' (Limiar: {limiar:.2f})")

    # Itera sobre as chaves do dicionário
    for chave, sinonimos in dicionario.items():
        # Comparação com a chave principal (chave já está normalizada no carregamento)
        # USO SEGURO DE frase_norm A PARTIR DAQUI
        sim_chave = difflib.SequenceMatcher(None, frase_norm, chave).ratio()
        _logger.log(5, f"  Comparando '{frase_norm}' com CHAVE '{chave}': {sim_chave:.4f}")

        if sim_chave > maior_similaridade:
            maior_similaridade = sim_chave
            melhor_chave = chave
            texto_que_deu_match = chave
            _logger.debug(f"    -> Novo melhor (da chave): Chave='{melhor_chave}' Similaridade={maior_similaridade:.4f}")

        # Comparação com os sinônimos
        for sinonimo, _ in sinonimos:
            sinonimo_norm = normalizar_texto(sinonimo)
            if not sinonimo_norm: continue
            # USO SEGURO DE frase_norm A PARTIR DAQUI
            sim_sin = difflib.SequenceMatcher(None, frase_norm, sinonimo_norm).ratio()
            _logger.log(5, f"    Comparando '{frase_norm}' com SINONIMO '{sinonimo_norm}' (da chave '{chave}'): {sim_sin:.4f}")

            if sim_sin > maior_similaridade:
                maior_similaridade = sim_sin
                melhor_chave = chave
                texto_que_deu_match = sinonimo_norm
                _logger.debug(f"    -> Novo melhor (de sinônimo '{sinonimo_norm}'): Chave='{melhor_chave}' Similaridade={maior_similaridade:.4f}")

    # Verifica o limiar final
    if maior_similaridade >= limiar and melhor_chave is not None:
        _logger.info(f"Melhor termo encontrado para '{frase}': Chave='{melhor_chave}' (Match '{texto_que_deu_match}', Sim: {maior_similaridade:.2f})")
        return melhor_chave
    else:
        _logger.info(f"Nenhum termo encontrado para '{frase}' >= {limiar:.2f} (Max: {maior_similaridade:.2f})")
        return None
# --- FIM DA CORREÇÃO ---


def encontrar_pergunta_similar(pergunta_usuario, templates_linhas, limiar=0.70):
    """Encontra a linha mais similar e extrai o NOME do template."""
    maior_similaridade = 0.0; template_nome_final = None; linha_template_correspondente = None
    if not templates_linhas: _logger.error("Lista de exemplos de perguntas vazia."); return None, 0, None
    pergunta_usuario_norm = normalizar_texto(pergunta_usuario);
    if not pergunta_usuario_norm: _logger.error("Pergunta do usuário normalizada vazia."); return None, 0, None
    _logger.debug(f"Buscando template para '{pergunta_usuario_norm}' (Limiar: {limiar:.2f})")
    for i, linha_template_original in enumerate(templates_linhas):
        try:
            if not linha_template_original or linha_template_original.startswith('#'): continue; _logger.debug(f"  Analisando L{i+1}: '{linha_template_original[:100]}...'")
            if " - " in linha_template_original:
                partes = linha_template_original.split(" - ", 1); nome_template_atual, pergunta_exemplo = partes[0].strip(), partes[1].strip();
                if not nome_template_atual or not pergunta_exemplo: _logger.warning(f"    Formato inválido L{i+1}."); continue;
                template_norm = normalizar_texto(pergunta_exemplo);
                if not template_norm: _logger.warning(f"    Exemplo normalizado vazio L{i+1}."); continue;
                similaridade = difflib.SequenceMatcher(None, pergunta_usuario_norm, template_norm).ratio(); _logger.debug(f"    -> '{nome_template_atual}', Sim: {similaridade:.4f}")
                if similaridade > maior_similaridade: _logger.debug(f"      >> NOVA MAIOR SIM! <<"); maior_similaridade, template_nome_final, linha_template_correspondente = similaridade, nome_template_atual, linha_template_original
            else: _logger.warning(f"    Formato inválido L{i+1} (sem ' - ').")
        except Exception as e: _logger.exception(f"Erro ao processar linha exemplo {i+1}")
    if maior_similaridade < limiar: _logger.warning(f"Similaridade max ({maior_similaridade:.2f}) < limiar ({limiar:.2f})."); return None, maior_similaridade, None
    _logger.info(f"Template similar: Nome='{template_nome_final}', Sim={maior_similaridade:.4f}"); return template_nome_final, maior_similaridade, linha_template_correspondente

def mapear_para_placeholders(pergunta_usuario_original, elementos, entidades_nomeadas, data, dicionario_sinonimos):
    """Mapeia elementos extraídos para placeholders semânticos."""
    mapeamentos = {}; _logger.debug("Iniciando mapeamento semântico...")
    if data: mapeamentos['#DATA'] = data; _logger.debug(f"  - Map #DATA: {data}")
    entidade_principal_str, tipo_entidade_detectada = None, None; ner_order = ['ORG', 'GPE']; ticker_regex = r'^[A-Z]{4}\d{1,2}$'
    for label in ner_order:
        if not entidade_principal_str and label in entidades_nomeadas:
            for ent_text in entidades_nomeadas[label]:
                ent_upper = ent_text.upper().strip();
                if re.fullmatch(ticker_regex, ent_upper): entidade_principal_str, tipo_entidade_detectada = ent_upper, f"NER {label} (Ticker)"; _logger.debug(f"  - #ENT via {tipo_entidade_detectada}: '{entidade_principal_str}'"); break
            if entidade_principal_str: break
    if not entidade_principal_str and 'ORG' in entidades_nomeadas and entidades_nomeadas['ORG']: entidade_principal_str = entidades_nomeadas['ORG'][0].upper().strip(); tipo_entidade_detectada = "NER ORG (Genérico)"; _logger.debug(f"  - #ENT via {tipo_entidade_detectada}: '{entidade_principal_str}'")
    if not entidade_principal_str:
        _logger.warning("  - Sem entidade NER clara. Fallback SPO..."); textos_spo = elementos.get("sujeito", []) + elementos.get("objeto", [])
        for texto_spo in textos_spo:
             palavras = re.findall(r'\b[A-Z]{4}\d{1,2}\b', texto_spo.upper());
             if palavras: entidade_principal_str = palavras[0]; tipo_entidade_detectada = "SPO (Ticker Fallback)"; _logger.debug(f"  - #ENT via {tipo_entidade_detectada}: '{entidade_principal_str}'"); break
             if entidade_principal_str: break
    if entidade_principal_str: mapeamentos['#ENTIDADE'] = entidade_principal_str.replace('.', '').strip(); _logger.info(f"  - Map #ENTIDADE: '{mapeamentos['#ENTIDADE']}' ({tipo_entidade_detectada})")
    else: _logger.error("  - FALHA CRÍTICA AO MAPEAR #ENTIDADE.")
    valor_desejado_chave = None; texto_candidato_valor = ""; partes_relevantes_para_valor = []; entidade_lower = mapeamentos.get('#ENTIDADE', '').lower() if '#ENTIDADE' in mapeamentos else None
    for key_spo in ["sujeito", "objeto"]:
        for item_spo in elementos.get(key_spo, []):
             texto_item = item_spo.lower();
             if entidade_lower: texto_item = texto_item.replace(entidade_lower, "")
             if data:
                 texto_item = texto_item.replace(data, "")
                 try: data_obj = datetime.strptime(data, "%Y-%m-%d"); texto_item = texto_item.replace(data_obj.strftime("%d/%m/%Y"), ""); texto_item = texto_item.replace(data_obj.strftime("%d-%m-%Y"), "")
                 except ValueError: pass
             texto_item = re.sub(r"^\s*(o|a|os|as|um|uma|uns|umas|da|de|do|das|dos|na|no|nas|nos|em|para|pelo|pela|pelos|pelas)\b", "", texto_item, flags=re.IGNORECASE).strip()
             texto_item = re.sub(r"\b(da|de|do|das|dos|na|no|nas|nos|em)\s*$", "", texto_item, flags=re.IGNORECASE).strip()
             texto_item = re.sub(r'\s+', ' ', texto_item).strip()
             if texto_item and len(texto_item) > 2: partes_relevantes_para_valor.append(texto_item)
    if partes_relevantes_para_valor:
        texto_candidato_valor = max(partes_relevantes_para_valor, key=len); _logger.debug(f"  - Texto candidato #VALOR_DESEJADO: '{texto_candidato_valor}'")
        valor_desejado_chave = encontrar_termo_dicionario(texto_candidato_valor, dicionario_sinonimos, limiar=0.75) # Chama a função corrigida
    else: _logger.debug("  - Nenhum candidato (SPO limpo) para #VALOR_DESEJADO.")
    if valor_desejado_chave: mapeamentos['#VALOR_DESEJADO'] = valor_desejado_chave; _logger.info(f"  - Map #VALOR_DESEJADO: '{valor_desejado_chave}' (de '{texto_candidato_valor}')")
    else: _logger.error(f"  - FALHA CRÍTICA AO MAPEAR #VALOR_DESEJADO para '{texto_candidato_valor}'.")
    tipo_acao_encontrado = None; texto_completo_norm = normalizar_texto(pergunta_usuario_original); _logger.debug(f"  - Texto norm para tipo ação: '{texto_completo_norm}'")
    if re.search(r'\b(ordinarias?|on)\b', texto_completo_norm, re.IGNORECASE): tipo_acao_encontrado = "ORDINARIA"; _logger.debug("  - Map #TIPO_ACAO: ORDINARIA")
    elif re.search(r'\b(preferenciais|preferencial|pn[a-z]?)\b', texto_completo_norm, re.IGNORECASE): tipo_acao_encontrado = "PREFERENCIAL"; _logger.debug("  - Map #TIPO_ACAO: PREFERENCIAL")
    else: _logger.debug("  - Nenhum #TIPO_ACAO explícito.")
    if tipo_acao_encontrado: mapeamentos['#TIPO_ACAO'] = tipo_acao_encontrado
    setor_encontrado = None; match_setor = re.search(r'\bsetor\s+(?:de|do|da)?\s*([\w\s\-]+)', pergunta_usuario_original, re.IGNORECASE)
    if match_setor:
         nome_setor = match_setor.group(1).strip(); nome_setor = re.sub(r'\s+(da|de|do|das|dos)$', '', nome_setor, flags=re.IGNORECASE).strip()
         if '#ENTIDADE' in mapeamentos: nome_setor = nome_setor.replace(mapeamentos['#ENTIDADE'].lower(), '').strip()
         if nome_setor and len(nome_setor) > 2: setor_encontrado = nome_setor; _logger.info(f"  - Map #SETOR: '{setor_encontrado}'")
         else: _logger.debug("  - 'setor' encontrado, mas sem nome válido.")
    else: _logger.debug("  - Nenhum #SETOR explícito.")
    if setor_encontrado: mapeamentos['#SETOR'] = setor_encontrado
    _logger.info(f"Mapeamentos semânticos finais: {mapeamentos}"); return mapeamentos

# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    _logger.info(f"--- ================================= ---")
    _logger.info(f"--- Iniciando processamento PLN Script (PID: {os.getpid()}) ---")
    _logger.info(f"--- Argumentos Recebidos: {sys.argv} ---")
    _logger.info(f"--- Diretório Atual (CWD): {os.getcwd()} ---")
    _logger.info(f"--- Python Executable: {sys.executable} ---")

    if len(sys.argv) > 1: pergunta_usuario = " ".join(sys.argv[1:]); _logger.info(f"Pergunta recebida: '{pergunta_usuario}'")
    else: fail_with_json_error("Nenhuma pergunta fornecida.")

    _logger.info("Carregando configs..."); templates_linhas_interesse = carregar_arquivo_linhas(CAMINHO_PERGUNTAS_INTERESSE); dicionario_sinonimos = carregar_dicionario_sinonimos(CAMINHO_DICIONARIO_SINONIMOS); _logger.info("Configs carregadas.")

    _logger.info("Processando NLP...");
    try: elementos_nlp, entidades_nomeadas_nlp = extrair_entidades_spacy(pergunta_usuario); data_extraida_nlp = extrair_data(pergunta_usuario); _logger.info("NLP concluído.")
    except Exception as e_nlp: fail_with_json_error("Erro no processamento NLP.", e_nlp)

    _logger.info("Buscando template..."); template_nome, similaridade, linha_original_template = encontrar_pergunta_similar(pergunta_usuario, templates_linhas_interesse, limiar=0.65)
    if not template_nome: fail_with_json_error("Pergunta não compreendida.", details=f"Similaridade max: {similaridade:.2f}")
    _logger.info(f"Template '{template_nome}' selecionado (Sim: {similaridade:.4f}).")

    _logger.info("Mapeando placeholders semânticos...");
    try: mapeamentos_semanticos = mapear_para_placeholders(pergunta_usuario, elementos_nlp, entidades_nomeadas_nlp, data_extraida_nlp, dicionario_sinonimos); _logger.info("Mapeamento concluído.")
    except Exception as e_map: fail_with_json_error("Erro no mapeamento semântico.", e_map) # Erro acontece aqui se UnboundLocal ocorrer

    _logger.info("Validando placeholders..."); placeholders_requeridos_por_template = {
        "Template 1A": {"#ENTIDADE", "#DATA", "#VALOR_DESEJADO"}, "Template 1B": {"#ENTIDADE", "#DATA", "#VALOR_DESEJADO"},
        "Template 2A": {"#ENTIDADE", "#VALOR_DESEJADO"}, "Template 3A": {"#SETOR", "#VALOR_DESEJADO"},
        "Template 4A": {"#SETOR", "#DATA", "#VALOR_DESEJADO"},   "Template 4B": {"#ENTIDADE", "#DATA", "#VALOR_DESEJADO"},
        "Template 5A": {"#ENTIDADE", "#DATA", "#VALOR_DESEJADO"}, "Template 5B": {"#ENTIDADE", "#DATA", "#VALOR_DESEJADO"},
        "Template 5C": {"#ENTIDADE", "#DATA", "#VALOR_DESEJADO"},
    }; placeholders_requeridos = placeholders_requeridos_por_template.get(template_nome, set()); placeholders_encontrados = set(mapeamentos_semanticos.keys()); placeholders_faltando = placeholders_requeridos - placeholders_encontrados
    if placeholders_faltando:
         _logger.warning(f"VALIDAÇÃO: '{template_nome}' - Faltando: {sorted(list(placeholders_faltando))}")
         if "#VALOR_DESEJADO" in placeholders_faltando: _logger.error("### VALIDAÇÃO FALHOU: #VALOR_DESEJADO não mapeado! ###")
         if "#ENTIDADE" in placeholders_faltando: _logger.error("### VALIDAÇÃO FALHOU: #ENTIDADE não mapeada! ###")
    else: _logger.info(f"Validação OK para '{template_nome}'.")

    _logger.info("Construindo JSON de resposta..."); resposta_final_stdout = {
        "template_nome": template_nome, "mapeamentos": mapeamentos_semanticos,
        "_debug_info": { "pergunta_original": pergunta_usuario, "elementos_extraidos_nlp": elementos_nlp, "entidades_nomeadas_nlp": entidades_nomeadas_nlp, "data_extraida_nlp": data_extraida_nlp, "similaridade_template": round(similaridade, 4) if similaridade else 0.0, "linha_template_correspondente": linha_original_template, "placeholders_requeridos_verificados": sorted(list(placeholders_requeridos)), "placeholders_faltando_verificados": sorted(list(placeholders_faltando)) }
    }; json_output_string = None
    try: json_output_string = json.dumps(resposta_final_stdout, ensure_ascii=False, indent=None); _logger.debug(f"JSON final para stdout: {json_output_string}")
    except Exception as e_json_final: fail_with_json_error("Erro ao gerar JSON final.", e_json_final)

    _logger.info("Enviando resposta para stdout...");
    try: print(json_output_string); sys.stdout.flush()
    except Exception as e_print: _logger.critical(f"Erro CRÍTICO ao imprimir para stdout: {e_print}", exc_info=True); print(f"ERRO PRINT STDOUT. JSON (stderr):\n{json_output_string}", file=sys.stderr); sys.exit(1)

    _logger.info(f"--- Processamento PLN Script concluído com sucesso ---"); sys.exit(0)
