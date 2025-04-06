import nltk
import spacy
import difflib
import re
import unicodedata
from nltk.stem.rslp import RSLPStemmer # Embora não usado ativamente no código fornecido, mantido se NLTK for usado futuramente.
import logging
import sys
import json
import os
from datetime import datetime, timedelta
import io # Para forçar encoding

# --- Configuração Inicial Essencial ---
# Tenta garantir que stdout/stderr usem UTF-8 e substituam erros.
# Faça isso ANTES de configurar o logging para que os próprios logs saiam corretamente.
try:
    # Usar buffer para evitar problemas com terminais que não suportam reconfiguração direta
    # errors='replace' evita crash se algum caractere não puder ser codificado
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    # Nota: O logging usará essas streams reconfiguradas.
except Exception as e_enc:
    # Se falhar, imprime no stderr original e continua (melhor esforço)
    print(f"AVISO URGENTE: Não foi possível forçar UTF-8 em stdout/stderr. Erro: {e_enc}", file=sys.__stderr__)

# --- Configuração de Logging (Arquivo e Stderr) ---
# Formato do Log
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_logger = logging.getLogger("PLN_Processor") # Usar um logger nomeado
_logger.setLevel(logging.DEBUG) # Nível raiz captura tudo

# Limpa handlers existentes para evitar duplicação se o script for importado
if _logger.hasHandlers():
    _logger.handlers.clear()

_log_file_path = 'pln_processor.log'
# Manipulador para arquivo 'pln_processor.log'
try:
    # 'w' para sobrescrever a cada execução, utf-8 para caracteres corretos
    file_handler = logging.FileHandler(_log_file_path, encoding='utf-8', mode='w')
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.DEBUG) # Captura DEBUG e acima no arquivo
    _logger.addHandler(file_handler)
except Exception as e:
    # Se não conseguir criar o log, imprime no stderr original e continua
    print(f"AVISO URGENTE: Não foi possível criar/abrir o arquivo de log '{_log_file_path}'. Erro: {e}", file=sys.stderr)

# Manipulador para stderr (console do Python, NÃO o stdout lido pelo Java)
try:
    # Mostra apenas INFO e acima no console para não poluir muito, mas mais que WARNING
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(log_formatter)
    stderr_handler.setLevel(logging.INFO) # INFO, WARNING, ERROR, CRITICAL no console
    _logger.addHandler(stderr_handler)
except Exception as e:
    print(f"AVISO: Não foi possível configurar logging para stderr. Erro: {e}", file=sys.stderr)

# -----------------------------------------------------

# Função para imprimir JSON de erro e sair (usa stdout)
def fail_with_json_error(error_message, details=None, status_code=1):
    error_payload = {"erro": error_message}
    if details:
        error_payload["detalhes"] = str(details)
    # Imprime o JSON de erro em stdout para o Java capturar
    print(json.dumps(error_payload, ensure_ascii=False))
    # Loga o erro detalhado em stderr/arquivo
    _logger.error(f"Finalizando com erro: {error_message} - Detalhes: {details}")
    sys.exit(status_code)

# --- Carregamento de Modelos e Dados ---
try:
    nltk.download('punkt', quiet=True, raise_on_error=True)
    nltk.download('rslp', quiet=True, raise_on_error=True)
    _logger.info("Pacotes NLTK verificados/baixados.")
except Exception as e:
    fail_with_json_error("Erro ao inicializar NLTK.", e)

try:
    nlp = spacy.load("pt_core_news_sm")
    _logger.info("Modelo spaCy 'pt_core_news_sm' carregado.")
except Exception as e:
    fail_with_json_error("Erro ao inicializar spaCy (modelo 'pt_core_news_sm' não encontrado?).", e)
# --------------------------------------------

# --- Constantes e Caminhos ---
# Usar caminhos relativos ao script ou absolutos verificados
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CAMINHO_DICIONARIO_SINONIMOS = os.path.join(_SCRIPT_DIR, r'C:\Users\MENICIO JR\Desktop\Natural2SPARQL-master\src\main\resources', 'resultado_similaridade.txt')
CAMINHO_PERGUNTAS_INTERESSE = os.path.join(_SCRIPT_DIR, r'C:\Users\MENICIO JR\Desktop\Natural2SPARQL-master\src\main\resources', 'perguntas_de_interesse.txt')
CAMINHO_DICIONARIO_VERBOS = os.path.join(_SCRIPT_DIR, r'C:\Users\MENICIO JR\Desktop\Natural2SPARQL-master\src\main\resources', 'dicionario_verbos.txt')

# Ajuste os caminhos se necessário para serem absolutos ou relativos corretos
# Exemplo Caminho Absoluto Fixo (menos flexível):
# CAMINHO_BASE = r'C:\Users\MENICIO JR\Desktop\Natural2SPARQL-master\src\main\resources'
# CAMINHO_DICIONARIO_SINONIMOS = os.path.join(CAMINHO_BASE, 'resultado_similaridade.txt')
# CAMINHO_PERGUNTAS_INTERESSE = os.path.join(CAMINHO_BASE, 'perguntas_de_interesse.txt')
# CAMINHO_DICIONARIO_VERBOS = os.path.join(CAMINHO_BASE, 'dicionario_verbos.txt')

_logger.debug(f"Caminho Dicionário Sinônimos: {CAMINHO_DICIONARIO_SINONIMOS}")
_logger.debug(f"Caminho Perguntas Interesse: {CAMINHO_PERGUNTAS_INTERESSE}")
# --------------------------------------------------------------------

pronomes_interrogativos = ['quem', 'o que', 'que', 'qual', 'quais', 'quanto', 'quantos', 'onde', 'como', 'quando', 'por que', 'porquê']

# --- Funções Auxiliares ---

def carregar_arquivo_linhas(caminho):
    """Carrega linhas de um arquivo, com detecção de encoding e tratamento de erros."""
    _logger.debug(f"Tentando carregar arquivo: {caminho}")
    try:
        if not os.path.exists(caminho):
            raise FileNotFoundError(f"Arquivo não encontrado em: {caminho}")

        encodings_to_try = ['utf-8', 'latin-1', 'windows-1252']
        content = None
        detected_encoding = None
        for enc in encodings_to_try:
            try:
                with open(caminho, 'r', encoding=enc) as f:
                    content = f.read()
                _logger.info(f"Arquivo {caminho} lido com encoding: {enc}")
                detected_encoding = enc
                break
            except UnicodeDecodeError:
                _logger.debug(f"Falha ao ler {caminho} com encoding {enc}.")
                continue
            except Exception as inner_e:
                 _logger.warning(f"Erro inesperado ao tentar ler {caminho} com {enc}: {inner_e}")
                 continue

        if content is None:
            raise IOError(f"Não foi possível ler {caminho} com os encodings testados: {encodings_to_try}")

        # Remove BOM (Byte Order Mark) se existir (comum em UTF-8 do Windows)
        if detected_encoding == 'utf-8' and content.startswith('\ufeff'):
            content = content[1:]
            _logger.debug("Removido BOM UTF-8 do início do arquivo.")

        linhas = [linha.strip() for linha in content.splitlines() if linha.strip()]
        if not linhas:
            _logger.warning(f"Arquivo {caminho} vazio ou sem conteúdo útil após strip.")
        _logger.debug(f"Carregadas {len(linhas)} linhas de {caminho}")
        return linhas
    except FileNotFoundError as fnf_e:
        # Erro fatal, reporta para Java e sai
        fail_with_json_error(f"Configuração '{os.path.basename(caminho)}' não encontrada.", fnf_e)
    except Exception as e:
        # Outro erro fatal na leitura, reporta para Java e sai
        fail_with_json_error(f"Erro ao ler configuração: {os.path.basename(caminho)}.", e)


def carregar_dicionario_sinonimos(caminho):
    """Carrega dicionário: chave=[(sinonimo, valor)]."""
    dicionario = {}
    linhas = carregar_arquivo_linhas(caminho) # Já trata erro fatal se arquivo não existe
    try:
        linha_num = 0
        # Regex permite letras, números e underscore na chave
        # Modificado para permitir espaços em branco opcionais e tratar floats com '.'
        chave_regex = re.compile(r"^\s*([\w_]+)\s*=\s*\[(.*)\]\s*$")
        valor_regex = re.compile(r"\(\s*'([^']+)'\s*,\s*([\d\.]+)\s*\)")

        for linha in linhas:
            linha_num += 1
            # Ignora linhas de comentário ou vazias
            if not linha or linha.startswith('#'):
                continue

            match = chave_regex.match(linha)
            if match:
                chave = match.group(1).lower() # Normaliza chave para minúsculas
                valores_str = match.group(2)
                valores = []
                # Encontra todos os pares ('sinonimo', valor) dentro dos colchetes
                for sin_match in valor_regex.finditer(valores_str):
                    sinonimo = sin_match.group(1).strip()
                    valor_str = sin_match.group(2)
                    try:
                        valor = float(valor_str)
                        if 0.0 <= valor <= 1.0: # Validar se o valor está no range esperado
                           valores.append((sinonimo, valor))
                        else:
                           _logger.warning(f"Valor de similaridade fora do range [0,1] ignorado: {valor} para '{sinonimo}' (chave '{chave}') em {caminho}, linha {linha_num}")
                    except ValueError:
                        _logger.warning(f"Valor float inválido ignorado: '{valor_str}' para '{sinonimo}' (chave '{chave}') em {caminho}, linha {linha_num}")

                if valores:
                    if chave in dicionario:
                        _logger.warning(f"Chave duplicada encontrada: '{chave}' em {caminho}, linha {linha_num}. Valores anteriores serão sobrescritos.")
                    dicionario[chave] = valores
                    _logger.debug(f"Carregada chave '{chave}' com {len(valores)} sinônimos.")
                else:
                    # Log se a lista de valores estiver vazia após o parse, mesmo que a chave exista
                    _logger.warning(f"Nenhum par (sinônimo, valor) válido encontrado para chave '{chave}' em {caminho}, linha {linha_num}. Linha original: {linha[:80]}...")
            else:
                 _logger.warning(f"Formato inválido ignorado no dicionário {os.path.basename(caminho)}, linha {linha_num}: {linha[:100]}...")

    except Exception as e:
        # Erro fatal no parse do dicionário
        fail_with_json_error(f"Erro no formato interno do dicionário '{os.path.basename(caminho)}'.", e)

    if not dicionario:
       _logger.error(f"Dicionário '{os.path.basename(caminho)}' carregado está vazio!")
       # Considerar se isso é um erro fatal? Depende da importância do dicionário.
       # fail_with_json_error(f"Dicionário '{os.path.basename(caminho)}' está vazio.")

    _logger.info(f"Dicionário '{os.path.basename(caminho)}' carregado: {len(dicionario)} chaves.")
    return dicionario

def normalizar_texto(texto):
    """Normaliza texto: minúsculas, remove acentos, trata caracteres específicos."""
    if not texto:
        return ""
    try:
        # Garante que é string e remove espaços extras
        texto_norm = str(texto).strip().lower()

        # Remove caracteres de controle e formatação invisíveis (exceto espaço)
        texto_norm = "".join(ch for ch in texto_norm if unicodedata.category(ch)[0]!="C" or ch == ' ')

        # Remove acentos (decomposição NFD e filtração de Mn)
        texto_norm = ''.join(c for c in unicodedata.normalize('NFD', texto_norm)
                             if unicodedata.category(c) != 'Mn')

        # Substituições específicas (opcional, cuidado para não criar ambiguidades)
        # replacements = {'?':' '} # Remover pontos de interrogação?
        # for problematic, replacement in replacements.items():
        #      texto_norm = texto_norm.replace(problematic, replacement)

        # Remove pontuação excessiva, mas mantém hífens/underscores internos
        # Mantém letras, números, espaço, hífen e underscore
        texto_norm = re.sub(r'[^\w\s-]', '', texto_norm)
        # Remove espaços duplicados
        texto_norm = re.sub(r'\s+', ' ', texto_norm).strip()

        return texto_norm
    except Exception as e:
        _logger.warning(f"Erro ao normalizar texto: '{texto[:50]}...'. Usando original. Erro: {e}")
        return str(texto).strip().lower() # Retorna uma versão minimamente processada


def extrair_entidades_spacy(texto):
    """Extrai SPO e NER usando spaCy."""
    elementos = {"sujeito": [], "predicado": [], "objeto": []}
    entidades_nomeadas = {}
    texto_original_para_debug = texto[:100] + "..." if len(texto) > 100 else texto
    _logger.debug(f"Iniciando extração spaCy para: '{texto_original_para_debug}'")

    if not texto:
        _logger.warning("Texto vazio para extração spaCy.")
        return elementos, entidades_nomeadas

    try:
        doc = nlp(texto)
        processed_tokens_indices = set() # Armazena índices dos tokens já usados

        # --- Extração SPO Baseada em Dependências ---
        for token in doc:
            token_idx = token.i
            token_text_lower = token.text.lower()

            # Ignora tokens já processados
            if token_idx in processed_tokens_indices:
                continue

            # Ignora pronomes interrogativos como elementos principais
            if token_text_lower in pronomes_interrogativos:
                 continue

            # Sujeito (nominal 'nsubj' ou passivo 'nsubj:pass')
            if token.dep_ in ("nsubj", "nsubj:pass"):
                # Pega a subárvore completa do sujeito
                subtree_tokens = list(token.subtree)
                sub_phrase = "".join(t.text_with_ws for t in subtree_tokens).strip()
                if sub_phrase:
                    elementos["sujeito"].append(sub_phrase)
                    processed_tokens_indices.update(t.i for t in subtree_tokens)
                    _logger.debug(f"  - Sujeito ({token.dep_}): '{sub_phrase}' (Token: {token.text})")

            # Objeto (direto 'dobj', indireto 'iobj') ou Complemento Oblíquo ('obl')
            elif token.dep_ in ("obj", "dobj", "iobj", "obl"):
                 # Pega a subárvore completa do objeto/oblíquo
                 subtree_tokens = list(token.subtree)
                 obj_phrase = "".join(t.text_with_ws for t in subtree_tokens).strip()
                 if obj_phrase:
                      # Tenta remover construções de data comuns do final (ex: "em 08/05/2023")
                      obj_phrase_cleaned = re.sub(r'\s+(em|no dia)\s+\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$', '', obj_phrase, flags=re.IGNORECASE).strip()
                      if obj_phrase_cleaned:
                           elementos["objeto"].append(obj_phrase_cleaned)
                           processed_tokens_indices.update(t.i for t in subtree_tokens)
                           _logger.debug(f"  - Objeto/Obl ({token.dep_}): '{obj_phrase_cleaned}' (Token: {token.text})")

            # Predicado (Verbo Principal - ROOT ou ligado a outro verbo)
            # Adiciona o lemma do verbo
            elif token.pos_ == "VERB" and token.dep_ in ("ROOT", "conj", "xcomp", "ccomp"):
                if token.lemma_ not in elementos["predicado"]:
                    elementos["predicado"].append(token.lemma_)
                    _logger.debug(f"  - Predicado ({token.dep_}): Lemma '{token.lemma_}' (Token: {token.text})")

        # Limpeza final e deduplicação das listas SPO
        for key in elementos:
             unique_elements = sorted(list(set(el for el in elementos[key] if el)), key=lambda x: texto.find(x))
             elementos[key] = unique_elements

        # --- Extração de Entidades Nomeadas (NER) ---
        for ent in doc.ents:
            label = ent.label_
            # Limpa o texto da entidade: remove espaços extras e pontos finais
            text = ent.text.strip()
            if text.endswith('.'): text = text[:-1].strip()

            # Adiciona se não for vazio e não for apenas número
            if text and not text.isdigit():
                 entidades_nomeadas.setdefault(label, []).append(text)
                 _logger.debug(f"  - NER: '{text}' (Label: {label})")

        # Deduplica lista de entidades nomeadas
        for label in entidades_nomeadas:
            entidades_nomeadas[label] = sorted(list(set(entidades_nomeadas[label])), key=lambda x: texto.find(x))

        _logger.info(f"Extração spaCy concluída. SPO: {elementos}, NER: {entidades_nomeadas}")

    except Exception as e:
        _logger.error(f"Erro durante extração spaCy para texto '{texto_original_para_debug}': {e}", exc_info=True)
        # Retorna vazio, mas não quebra o script
        elementos = {"sujeito": [], "predicado": [], "objeto": []}
        entidades_nomeadas = {}

    return elementos, entidades_nomeadas


def extrair_data(texto):
    """Extrai data (dd/mm/yyyy, dd-mm-yyyy, hoje, ontem) e normaliza para AAAA-MM-DD."""
    _logger.debug(f"Tentando extrair data de: '{texto}'")
    # Padroes mais flexiveis para dia/mes/ano
    padroes = [
        r'\b(\d{1,2})\s*[/.-]\s*(\d{1,2})\s*[/.-]\s*(\d{4})\b', # dd/mm/yyyy ou dd-mm-yyyy ou dd.mm.yyyy
        r'\b(\d{4})\s*[/.-]\s*(\d{1,2})\s*[/.-]\s*(\d{1,2})\b'  # yyyy/mm/dd ou yyyy-mm-dd ou yyyy.mm.dd
    ]
    texto_lower = texto.lower()
    data_normalizada = None

    for i, padrao in enumerate(padroes):
        match = re.search(padrao, texto)
        if match:
            try:
                if i == 0: # Formato dd/mm/yyyy
                    dia, mes, ano = int(match.group(1)), int(match.group(2)), int(match.group(3))
                else: # Formato yyyy/mm/dd
                    ano, mes, dia = int(match.group(1)), int(match.group(2)), int(match.group(3))

                # Validação básica dos valores
                if 1 <= dia <= 31 and 1 <= mes <= 12 and ano > 1900: # Ano > 1900 é um palpite razoável
                    data_obj = datetime(ano, mes, dia)
                    data_normalizada = data_obj.strftime("%Y-%m-%d")
                    _logger.info(f"Data explícita '{match.group(0)}' normalizada para: {data_normalizada}")
                    return data_normalizada # Retorna a primeira data válida encontrada
                else:
                    _logger.warning(f"Componentes de data inválidos encontrados: dia={dia}, mes={mes}, ano={ano} (Texto: '{match.group(0)}')")
            except ValueError as ve:
                _logger.warning(f"Erro ao converter data encontrada '{match.group(0)}': {ve}")
            except Exception as e_dt:
                 _logger.warning(f"Erro inesperado ao processar data encontrada '{match.group(0)}': {e_dt}")

    # Se não encontrou pelos padrões, tenta palavras-chave
    if "hoje" in texto_lower:
        data_normalizada = datetime.now().strftime("%Y-%m-%d")
        _logger.info("Data 'hoje' normalizada.")
        return data_normalizada
    if "ontem" in texto_lower:
        data_normalizada = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        _logger.info("Data 'ontem' normalizada.")
        return data_normalizada

    # Se chegou aqui, nenhuma data válida foi encontrada
    _logger.info("Nenhuma data válida encontrada no texto.")
    return None


def encontrar_termo_dicionario(frase, dicionario, limiar=0.70):
    """Encontra a melhor chave no dicionário de sinônimos para uma frase, usando texto normalizado."""
    if not dicionario or not frase:
        _logger.debug(f"Dicionário ou frase vazia ao buscar termo: '{frase}'")
        return None

    frase_norm = normalizar_texto(frase)
    if not frase_norm:
        _logger.debug(f"Frase normalizada vazia para busca de termo: '{frase}'")
        return None

    melhor_chave = None
    # Inicia com limiar - epsilon para garantir que >= limiar funcione
    maior_similaridade = limiar - 0.001
    texto_chave_match = None # Guarda o texto exato que deu o melhor match

    _logger.debug(f"Buscando termo para frase normalizada: '{frase_norm}' (Limiar: {limiar:.2f})")

    # Itera sobre as chaves do dicionário (já normalizadas para minúsculas no carregamento)
    for chave, sinonimos in dicionario.items():
        # --- Comparação com chave principal ---
        sim_chave = difflib.SequenceMatcher(None, frase_norm, chave).ratio() # Chave já está normalizada
        _logger.log(5, f"  Comparando '{frase_norm}' com CHAVE '{chave}': {sim_chave:.4f}") # Nível 5 para Trace extremo
        if sim_chave > maior_similaridade:
            maior_similaridade = sim_chave
            melhor_chave = chave
            texto_chave_match = chave # O match foi com a própria chave
            _logger.debug(f"  -> Novo melhor (chave): '{melhor_chave}' similaridade {maior_similaridade:.4f}")
        # Caso de empate: prefere a chave mais curta? Ou a que teve maior similaridade *exata*?
        # A lógica atual já prioriza > maior_similaridade, então só atualiza se for estritamente melhor.

        # --- Comparação com sinônimos ---
        for sinonimo, _ in sinonimos: # Ignora o valor de similaridade do arquivo aqui
            sinonimo_norm = normalizar_texto(sinonimo)
            if not sinonimo_norm: continue # Pula sinônimos que viram vazios após normalização

            sim_sin = difflib.SequenceMatcher(None, frase_norm, sinonimo_norm).ratio()
            _logger.log(5, f"    Comparando '{frase_norm}' com SINONIMO '{sinonimo_norm}' (chave '{chave}'): {sim_sin:.4f}")
            if sim_sin > maior_similaridade:
                maior_similaridade = sim_sin
                melhor_chave = chave # Associa à chave principal
                texto_chave_match = sinonimo_norm # O match foi com este sinônimo normalizado
                _logger.debug(f"  -> Novo melhor (sinônimo '{sinonimo_norm}'): chave '{melhor_chave}' similaridade {maior_similaridade:.4f}")

    # Verifica se a maior similaridade encontrada realmente atinge o limiar
    if maior_similaridade >= limiar and melhor_chave is not None:
        _logger.info(f"Melhor termo encontrado para '{frase}': Chave='{melhor_chave}' (Match com '{texto_chave_match}', Similaridade: {maior_similaridade:.2f})")
        return melhor_chave # Retorna a CHAVE do dicionário
    else:
        _logger.info(f"Nenhum termo encontrado para '{frase}' com similaridade >= {limiar:.2f} (Max: {maior_similaridade:.2f})")
        return None


# --- Função encontrar_pergunta_similar (Retorna 3 valores) ---
def encontrar_pergunta_similar(pergunta_usuario, templates_linhas, limiar=0.70):
    """
    Encontra a linha mais similar no arquivo de templates e extrai o NOME do template.
    Retorna (nome_template, similaridade, linha_completa_original) ou (None, 0, None).
    Assume formato: "Nome do Template - Pergunta exemplo..."
    """
    maior_similaridade = 0.0
    template_nome_final = None
    linha_template_correspondente = None

    if not templates_linhas:
        _logger.error("Lista de templates de perguntas vazia ao tentar encontrar similar.")
        return None, 0, None # Retorna tupla consistente

    pergunta_usuario_norm = normalizar_texto(pergunta_usuario)
    _logger.debug(f"Buscando template para pergunta normalizada: '{pergunta_usuario_norm}' (Limiar: {limiar:.2f})")
    _logger.debug(f"Comparando com {len(templates_linhas)} templates.")

    for i, linha_template_original in enumerate(templates_linhas):
        try:
            # Ignora linhas de comentário ou vazias
            if not linha_template_original or linha_template_original.startswith('#'):
                continue

            _logger.debug(f"  Analisando Linha {i+1}: '{linha_template_original}'")
            if " - " in linha_template_original:
                # Divide APENAS no primeiro " - "
                partes = linha_template_original.split(" - ", 1)
                nome_template_atual = partes[0].strip()
                pergunta_exemplo = partes[1].strip()

                # Verifica se o nome do template parece válido (evita acidentes)
                if not nome_template_atual or not pergunta_exemplo:
                    _logger.warning(f"    Formato inválido (nome ou pergunta vazia após split): '{linha_template_original}'. Ignorando.")
                    continue

                template_norm = normalizar_texto(pergunta_exemplo)
                similaridade = difflib.SequenceMatcher(None, pergunta_usuario_norm, template_norm).ratio()
                _logger.debug(f"    -> Nome: '{nome_template_atual}', Exemplo norm: '{template_norm}', Similaridade: {similaridade:.4f}")

                # Atualiza se a similaridade for ESTRITAMENTE maior
                if similaridade > maior_similaridade:
                    _logger.debug(f"    -> NOVA MAIOR SIMILARIDADE encontrada!")
                    maior_similaridade = similaridade
                    template_nome_final = nome_template_atual
                    linha_template_correspondente = linha_template_original

            else:
                 _logger.warning(f"    Formato inválido (sem ' - ' delimitador): '{linha_template_original}'. Ignorando.")
                 continue

        except Exception as e:
            # Loga o erro mas continua tentando os outros templates
            _logger.exception(f"Erro ao processar linha template {i+1}: '{linha_template_original}'")
            continue # Pula para a próxima linha

    # Verifica o limiar APÓS testar todos os templates
    if maior_similaridade < limiar:
         _logger.warning(f"Similaridade máxima encontrada ({maior_similaridade:.2f}) está abaixo do limiar ({limiar:.2f}). Nenhum template selecionado.")
         template_nome_final = None
         linha_template_correspondente = None
         # Mantém maior_similaridade para debug info, mas retorna None para nome e linha

    if template_nome_final:
         _logger.info(f"Template similar encontrado: Nome='{template_nome_final}', Similaridade={maior_similaridade:.4f} (Linha: '{linha_template_correspondente}')")
    else:
         _logger.info(f"Nenhum template similar encontrado acima do limiar {limiar:.2f}.")

    # Retorna sempre a tupla com 3 elementos
    return template_nome_final, maior_similaridade, linha_template_correspondente
# --- FIM encontrar_pergunta_similar ---


def mapear_para_placeholders(pergunta_usuario_original, elementos, entidades_nomeadas, data, dicionario_sinonimos):
    """Mapeia elementos extraídos para placeholders semânticos (#DATA, #ENTIDADE, #VALOR_DESEJADO, etc.)."""
    mapeamentos = {}
    _logger.debug("Iniciando mapeamento semântico para placeholders...")

    # 1. Mapear #DATA
    if data:
        mapeamentos['#DATA'] = data
        _logger.debug(f"  - Mapeado #DATA: {data}")
    else:
        _logger.debug("  - Nenhuma #DATA para mapear.")

    # 2. Mapear #ENTIDADE (Empresa/Ticker)
    entidade_principal_str = None
    tipo_entidade_detectada = None

    # Prioridade 1: NER 'ORG' que parece ticker (ex: CBAV3)
    if 'ORG' in entidades_nomeadas:
        for ent_org in entidades_nomeadas['ORG']:
            ent_upper = ent_org.upper().strip()
            # Regex simples para ticker (4 letras + 1 ou 2 digitos)
            if re.fullmatch(r'^[A-Z]{4}\d{1,2}$', ent_upper):
                entidade_principal_str = ent_upper
                tipo_entidade_detectada = "ORG (Ticker)"
                _logger.debug(f"  - #ENTIDADE via NER ORG (Ticker): '{entidade_principal_str}'")
                break # Usa o primeiro ticker encontrado em ORG

    # Prioridade 2: NER 'GPE' que parece ticker (menos provável, mas possível)
    if not entidade_principal_str and 'GPE' in entidades_nomeadas:
         for ent_gpe in entidades_nomeadas['GPE']:
            ent_upper = ent_gpe.upper().strip()
            if re.fullmatch(r'^[A-Z]{4}\d{1,2}$', ent_upper):
                entidade_principal_str = ent_upper
                tipo_entidade_detectada = "GPE (Ticker?)"
                _logger.debug(f"  - #ENTIDADE via NER GPE (Ticker?): '{entidade_principal_str}'")
                break

    # Prioridade 3: NER 'ORG' genérico (pega o primeiro)
    if not entidade_principal_str and 'ORG' in entidades_nomeadas and entidades_nomeadas['ORG']:
        # Pega a primeira entidade ORG encontrada
        entidade_principal_str = entidades_nomeadas['ORG'][0].upper().strip()
        # Pode ser necessário mapear nomes para tickers aqui (Ex: "Vale" -> "VALE3")
        # Por enquanto, só normaliza.
        tipo_entidade_detectada = "ORG (Genérico)"
        _logger.debug(f"  - #ENTIDADE via NER ORG (Genérico): '{entidade_principal_str}'")


    # Prioridade 4: Fallback para Sujeito/Objeto (menos confiável)
    if not entidade_principal_str:
        _logger.warning("  - Nenhuma entidade clara (Ticker/ORG) encontrada via NER. Tentando fallback SPO...")
        # Tentar encontrar algo que pareça ticker no sujeito ou objeto
        textos_spo = elementos.get("sujeito", []) + elementos.get("objeto", [])
        for texto in textos_spo:
             # Procura por palavras que parecem tickers dentro das frases SPO
             for palavra in texto.split():
                 palavra_upper = palavra.upper().strip(".,?!")
                 if re.fullmatch(r'^[A-Z]{4}\d{1,2}$', palavra_upper):
                     entidade_principal_str = palavra_upper
                     tipo_entidade_detectada = "SPO (Ticker Fallback)"
                     _logger.debug(f"  - #ENTIDADE via Fallback SPO (Ticker): '{entidade_principal_str}'")
                     break
             if entidade_principal_str: break

    # Adiciona ao mapeamento se encontrado
    if entidade_principal_str:
        # Normalização final (remover pontos, etc.)
        entidade_final = entidade_principal_str.replace('.', '').strip()
        mapeamentos['#ENTIDADE'] = entidade_final
        _logger.info(f"  - Mapeado #ENTIDADE: '{entidade_final}' (Detectado como: {tipo_entidade_detectada})")
    else:
        # Não ter entidade pode ser um problema dependendo do template
        _logger.warning("  - FALHA ao mapear #ENTIDADE. Isso pode causar erro na geração da query.")


    # 3. Mapear #VALOR_DESEJADO
    valor_desejado_chave = None
    texto_para_busca = ""

    # Combina sujeito e objeto para ter mais contexto
    # Remove a entidade principal e a data para isolar o valor desejado
    partes_relevantes = []
    for key in ["sujeito", "objeto"]:
        for item in elementos.get(key, []):
             texto_item = item.lower()
             if entidade_principal_str:
                 texto_item = texto_item.replace(entidade_principal_str.lower(), "")
             # Remove a data em vários formatos (normalizado e talvez original)
             if data:
                 texto_item = texto_item.replace(data, "") # Remove YYYY-MM-DD
                 try: # Tenta remover dd/mm/yyyy também
                      data_obj = datetime.strptime(data, "%Y-%m-%d")
                      texto_item = texto_item.replace(data_obj.strftime("%d/%m/%Y"), "")
                      texto_item = texto_item.replace(data_obj.strftime("%d-%m-%Y"), "")
                 except ValueError: pass # Ignora se a data não for no formato esperado

             # Remove preposições/artigos comuns e espaços extras
             texto_item = re.sub(r"^\s*(o|a|os|as|da|de|do|em|na|no|para|pelo|pela)\b", "", texto_item).strip()
             texto_item = re.sub(r"\b(da|de|do|em|na|no)\s*$", "", texto_item).strip()
             texto_item = re.sub(r'\s+', ' ', texto_item).strip() # Normaliza espaços

             if texto_item and len(texto_item) > 2: # Evita termos muito curtos
                 partes_relevantes.append(texto_item)

    # Usa a parte mais longa encontrada como candidata principal
    if partes_relevantes:
        texto_para_busca = max(partes_relevantes, key=len)
        _logger.debug(f"  - Texto candidato para #VALOR_DESEJADO: '{texto_para_busca}' (de {partes_relevantes})")
        valor_desejado_chave = encontrar_termo_dicionario(texto_para_busca, dicionario_sinonimos, limiar=0.75) # Limiar um pouco mais alto para valor?
    else:
         _logger.debug("  - Nenhum texto candidato (sujeito/objeto limpos) encontrado para #VALOR_DESEJADO.")

    # Adiciona ao mapeamento se encontrado
    if valor_desejado_chave:
        mapeamentos['#VALOR_DESEJADO'] = valor_desejado_chave
        _logger.info(f"  - Mapeado #VALOR_DESEJADO: '{valor_desejado_chave}' (para texto '{texto_para_busca}')")
    else:
        # Não ter valor desejado pode ser problemático
        _logger.warning(f"  - FALHA ao mapear #VALOR_DESEJADO para texto candidato '{texto_para_busca}'. Verificar dicionário e extração SPO.")
        # Java precisará lidar com a ausência desta chave

    # 4. Mapear #TIPO_ACAO (Ordinária/Preferencial)
    texto_completo_norm = normalizar_texto(pergunta_usuario_original)
    _logger.debug(f"  - Texto normalizado para buscar tipo de ação: '{texto_completo_norm}'")
    # Usar regex para ser mais robusto (ON, Ordinaria, PN, PNA, PNB, Preferencial)
    if re.search(r'\b(ordinaria| on)\b', texto_completo_norm):
        mapeamentos['#TIPO_ACAO'] = "ORDINARIA"
        _logger.debug("  - Mapeado #TIPO_ACAO: ORDINARIA")
    elif re.search(r'\b(preferencial| pn[a-z]?)\b', texto_completo_norm):
        mapeamentos['#TIPO_ACAO'] = "PREFERENCIAL"
        _logger.debug("  - Mapeado #TIPO_ACAO: PREFERENCIAL")
    else:
         _logger.debug("  - Nenhum #TIPO_ACAO explícito encontrado.")

    # 5. Mapear #SETOR (Se aplicável, pode precisar de NER=MISC ou lógica específica)
    # Exemplo simples: procurar por palavras como "setor" seguido de algo
    match_setor = re.search(r'\bsetor\s+(de|do|da)?\s*([\w\s]+)', pergunta_usuario_original, re.IGNORECASE)
    if match_setor:
         # Pega o texto após "setor ..." e normaliza
         nome_setor = match_setor.group(2).strip()
         # Remove preposições finais se houver
         nome_setor = re.sub(r'\s+(da|de|do)$', '', nome_setor, flags=re.IGNORECASE).strip()
         if nome_setor:
              mapeamentos['#SETOR'] = nome_setor
              _logger.info(f"  - Mapeado #SETOR: '{nome_setor}'")
         else:
              _logger.debug("  - Encontrada palavra 'setor', mas sem nome claro após.")
    else:
         _logger.debug("  - Nenhum #SETOR explícito encontrado via regex.")


    _logger.info(f"Mapeamentos semânticos finais: {mapeamentos}")
    return mapeamentos


# --- Bloco Principal ---
if __name__ == "__main__":
    _logger.info(f"--- ================================= ---")
    _logger.info(f"--- Iniciando processamento PLN (PID: {os.getpid()}) ---")
    _logger.info(f"--- Argumentos: {sys.argv} ---")

    # 1. Obter Pergunta
    if len(sys.argv) > 1:
        # Junta todos os argumentos após o nome do script, caso a pergunta venha com espaços
        pergunta_usuario = " ".join(sys.argv[1:])
        _logger.info(f"Pergunta recebida (argv): '{pergunta_usuario}'")
    else:
        _logger.error("Nenhuma pergunta fornecida como argumento de linha de comando.")
        fail_with_json_error("Nenhuma pergunta fornecida.")

    # 2. Carregar Dados (Arquivos de Configuração)
    # O carregamento já chama fail_with_json_error se arquivos essenciais não forem encontrados/lidos
    templates_linhas_interesse = carregar_arquivo_linhas(CAMINHO_PERGUNTAS_INTERESSE)
    dicionario_sinonimos = carregar_dicionario_sinonimos(CAMINHO_DICIONARIO_SINONIMOS)
    # dicionario_verbos = carregar_arquivo_linhas(CAMINHO_DICIONARIO_VERBOS) # Descomentar se for usar

    # 3. Processamento NLP (spaCy)
    try:
        elementos_nlp, entidades_nomeadas_nlp = extrair_entidades_spacy(pergunta_usuario)
        data_extraida_nlp = extrair_data(pergunta_usuario)
    except Exception as e_nlp:
        # Erro durante o processamento principal do NLP
        fail_with_json_error("Erro durante o processamento da linguagem natural.", e_nlp)

    # 4. Encontrar Template Similar
    # O limiar pode ser ajustado aqui ou vindo de configuração
    template_nome, similaridade, linha_original_template = encontrar_pergunta_similar(
        pergunta_usuario,
        templates_linhas_interesse,
        limiar=0.65 # Limiar de similaridade para aceitar um template
    )

    # Se nenhum template foi encontrado acima do limiar, é um erro para o Java
    if not template_nome:
        fail_with_json_error(
            "Pergunta não compreendida (nenhum template similar encontrado).",
            details=f"Similaridade máxima: {similaridade:.2f}"
        )

    # 5. Mapear Entidades para Placeholders Semânticos
    try:
        mapeamentos_semanticos = mapear_para_placeholders(
            pergunta_usuario,
            elementos_nlp,
            entidades_nomeadas_nlp,
            data_extraida_nlp,
            dicionario_sinonimos
        )
    except Exception as e_map:
        fail_with_json_error("Erro durante o mapeamento semântico.", e_map)


    # 6. Validação Mínima dos Mapeamentos (Opcional, mas recomendado)
    # Verifica se os placeholders essenciais para o template selecionado foram mapeados.
    # (Esta parte é mais complexa, pois depende de saber quais placeholders cada template *requer*)
    # Exemplo simples: Se o template for 1A ou 1B, precisa de #ENTIDADE e #DATA?
    placeholders_requeridos = set()
    if template_nome in ["Template 1A", "Template 1B", "Template 5A", "Template 5B", "Template 5C", "Template 4B"]:
         placeholders_requeridos.add("#ENTIDADE")
         placeholders_requeridos.add("#DATA")
         placeholders_requeridos.add("#VALOR_DESEJADO") # Crucial para saber o que selecionar
    elif template_nome == "Template 2A":
         placeholders_requeridos.add("#ENTIDADE")
         placeholders_requeridos.add("#VALOR_DESEJADO") # Geralmente 'codigo'
    elif template_nome == "Template 3A":
         placeholders_requeridos.add("#SETOR")
         placeholders_requeridos.add("#VALOR_DESEJADO") # Geralmente 'codigo'
    elif template_nome == "Template 4A":
         placeholders_requeridos.add("#SETOR")
         placeholders_requeridos.add("#DATA")
         placeholders_requeridos.add("#VALOR_DESEJADO") # Geralmente 'volume'

    placeholders_faltando = placeholders_requeridos - set(mapeamentos_semanticos.keys())

    if placeholders_faltando:
         # Loga como warning, mas deixa o Java decidir se é fatal
         _logger.warning(f"Mapeamento pode estar incompleto para template '{template_nome}'. Placeholders requeridos faltando: {placeholders_faltando}")
         # Poderia retornar um erro aqui se quisesse ser mais estrito:
         # fail_with_json_error(f"Informações insuficientes para template '{template_nome}'. Faltando: {placeholders_faltando}")
         # Por enquanto, vamos permitir que o Java receba o mapeamento parcial.
         # **IMPORTANTE:** A falta de #VALOR_DESEJADO ainda é um problema sério!
         if "#VALOR_DESEJADO" in placeholders_faltando:
              _logger.error("### #VALOR_DESEJADO não foi mapeado! A consulta provavelmente falhará em determinar o resultado. ###")
              # Considerar falhar aqui?
              # fail_with_json_error("Não foi possível identificar o que você deseja saber (valor desejado).")


    # 7. Construir Resposta JSON Final para STDOUT
    resposta_final_stdout = {
        "template_nome": template_nome,
        "mapeamentos": mapeamentos_semanticos,
        # Informações de debug podem ser úteis no Java
        "_debug_info": {
             "pergunta_original": pergunta_usuario,
             "elementos_extraidos_nlp": elementos_nlp,
             "entidades_nomeadas_nlp": entidades_nomeadas_nlp,
             "data_extraida_nlp": data_extraida_nlp,
             "similaridade_template": round(similaridade, 4),
             "linha_template_correspondente": linha_original_template,
             "placeholders_requeridos_verificados" : list(placeholders_requeridos),
             "placeholders_faltando_verificados": list(placeholders_faltando)
        }
    }

    # Loga o JSON que será enviado ANTES de imprimir
    # Usa indent=None para uma única linha, mais fácil para Java ler se não precisar ser pretty
    json_output_string = json.dumps(resposta_final_stdout, ensure_ascii=False, indent=None)
    _logger.debug(f"JSON final para stdout: {json_output_string}")

    # --- IMPRESSÃO FINAL PARA STDOUT ---
    # Imprime o JSON final, e APENAS ele, para stdout
    print(json_output_string)
    # -----------------------------------

    _logger.info("--- Processamento PLN concluído com sucesso ---")
    sys.exit(0) # Indica sucesso para o Java
