# -*- coding: utf-8 -*-
import pandas as pd
import os
import re
import unicodedata
from collections import defaultdict
import json
import sys
import traceback

# --- Configuração ---
# SCRIPT EM: src/main/resources
# EXCELS EM: ../../../datasets/ (Relativo a src/main/resources)
# JSON SALVO EM: src/main/resources

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    script_dir = os.path.abspath(".")
print(f"Diretório de execução do script (src/main/resources): {script_dir}")

# Ajuste o caminho para a pasta datasets se ela não estiver 3 níveis acima
project_root_dir = os.path.abspath(os.path.join(script_dir, "..", "..", ".."))
datasets_dir = os.path.join(project_root_dir, "datasets")
print(f"Diretório esperado para datasets: {datasets_dir}")


# Caminhos completos para os arquivos Excel dentro da pasta 'datasets'
EXCEL_FILES = [
    os.path.join(datasets_dir, "dados_novos_atual.xlsx"),
    os.path.join(datasets_dir, "dados_novos_anterior.xlsx"),
    # os.path.join(datasets_dir, "Informacoes_Empresas.xlsx"),
]

# Colunas
COMPANY_NAME_COL = 6 # G
TICKER_COL = 4      # E
SHEET_NAME = 0

# Arquivo JSON de saída - NOME PADRONIZADO
JSON_OUTPUT_FILENAME = "empresa_nome_map.json" # <-- NOME CORRETO
JSON_OUTPUT_PATH = os.path.join(script_dir, JSON_OUTPUT_FILENAME)

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

def get_canonical_name(names_set):
    if not names_set: return None
    return min(sorted(list(names_set)), key=len)

def is_valid_ticker(ticker):
    return isinstance(ticker, str) and re.match(r'^[A-Z]{4}\d{1,2}$', ticker.strip().upper())

# --- Processamento Principal ---
name_to_names = defaultdict(set)
ticker_to_canonical_name = {}
all_tickers = set()
files_processed = 0
has_errors = False

print(f"--- Iniciando processamento Excel para Mapa de Nomes ---")
for file_path in EXCEL_FILES:
    print(f"\nTentando processar: {file_path}")
    if not os.path.exists(file_path): print(f"AVISO: Não encontrado: {file_path}. Pulando.", file=sys.stderr); has_errors = True; continue
    try:
        df = pd.read_excel(file_path, sheet_name=SHEET_NAME, header=0, dtype=str)
        print(f"  Lido '{os.path.basename(file_path)}'. {len(df)} linhas."); files_processed += 1
        for index, row in df.iterrows():
            line_num = index + 2
            try:
                company_name_original, ticker = None, None
                if isinstance(COMPANY_NAME_COL, int):
                    if COMPANY_NAME_COL < len(row.index): company_name_original = row.iloc[COMPANY_NAME_COL]
                if isinstance(TICKER_COL, int):
                    if TICKER_COL < len(row.index): ticker = row.iloc[TICKER_COL]

                if isinstance(ticker, str): ticker = ticker.strip().upper()
                else: continue
                if not is_valid_ticker(ticker): continue
                if not isinstance(company_name_original, str) or not company_name_original.strip(): company_name_original = ticker

                company_name_clean = company_name_original.strip()
                normalized_name = normalize_key(company_name_clean)

                if normalized_name:
                    name_to_names[normalized_name].add(company_name_clean)
                all_tickers.add(ticker)

            except Exception as e_row: print(f"  AVISO: Erro L{line_num}: {e_row}", file=sys.stderr); has_errors = True
    except Exception as e_file: print(f"ERRO: Falha geral '{os.path.basename(file_path)}': {e_file}", file=sys.stderr); has_errors = True
if files_processed == 0: print("\nERRO FATAL: Nenhum Excel encontrado/processado.", file=sys.stderr); sys.exit(1)
if not name_to_names: print("\nERRO FATAL: Nenhum nome de empresa coletado.", file=sys.stderr); sys.exit(1)
print(f"\n--- Processamento Excel concluído. ---")

# --- Construção do Mapa Final ---
final_map_to_canonical = {}
print(f"--- Construindo mapa final de nomes para JSON ---")

print("  - Mapeando Nome Normalizado -> Nome Canônico...")
for norm_name, names_set in name_to_names.items():
    canonical_name = get_canonical_name(names_set)
    if canonical_name:
        final_map_to_canonical[norm_name.upper()] = canonical_name

print("  - Mapeando Ticker -> Nome Canônico...")
ticker_to_canonical_temp = {} # Guarda temporário para evitar sobrescrita
for norm_name, names_set in name_to_names.items():
     canonical = get_canonical_name(names_set)
     if canonical:
         associated_tickers = set()
         for file_name_inner in EXCEL_FILES:
              file_path_inner = os.path.join(datasets_dir, file_name_inner) # Busca em datasets
              if not os.path.exists(file_path_inner): continue
              try:
                 df_temp = pd.read_excel(file_path_inner, sheet_name=SHEET_NAME, header=0, dtype=str, usecols=[COMPANY_NAME_COL, TICKER_COL])
                 for _, row_temp in df_temp.iterrows():
                     comp_orig = row_temp.iloc[0]
                     tick = row_temp.iloc[1]
                     if isinstance(tick, str): tick = tick.strip().upper()
                     else: continue
                     if isinstance(comp_orig, str) and comp_orig.strip() == canonical and is_valid_ticker(tick):
                          associated_tickers.add(tick)
              except Exception as e_read_inner:
                   print(f"  Aviso: Erro ao reler {file_name_inner} para tickers: {e_read_inner}", file=sys.stderr)
         for tick in associated_tickers:
              ticker_to_canonical_temp[tick] = canonical # Guarda no temporário

# Adiciona mapeamentos ticker->nome ao mapa final, sem sobrescrever nomes normalizados
for tick, canon_name in ticker_to_canonical_temp.items():
    if tick not in final_map_to_canonical:
         final_map_to_canonical[tick] = canon_name

print("  - Aplicando mapeamentos manuais / nomes comuns...");
manual_overrides = {
    # Chave: Como usuário digita -> Valor: Nome Canônico (EXATO como usado no filtro SPARQL)
    "CSN": "CSN MINERACAO",        # << AJUSTE este valor se o nome na planilha for diferente
    "CSNMINERACAO": "CSN MINERACAO",
    "GERDAU": "GERDAU",            # << AJUSTE (Metalurgica Gerdau?)
    "METALURGICA GERDAU": "GERDAU", # << AJUSTE
    "VALE": "VALE",
    "ITAU": "ITAUUNIBANCO",       # << AJUSTE (Itau Unibanco Holding S.A.?)
    "ITAU UNIBANCO": "ITAUUNIBANCO",
    "ITAUUNIBANCO HOLDING": "ITAUUNIBANCO",
    "PETROBRAS": "PETROBRAS",
    "PETROLEOBRASILEIRO": "PETROBRAS",
    "TAURUS ARMAS": "TAURUS ARMAS", # << AJUSTE para nome canônico real
    "TAURUSARMAS": "TAURUS ARMAS",
    "TAURUS": "TAURUS ARMAS",
    "TENDA": "TENDA",          # << AJUSTE se for CONSTRUTORA TENDA S.A.
    "CONSTRUTORA TENDA": "TENDA",
    "TAESA": "TAESA",           # << AJUSTE se for TAESA
    "TRANS PAULISTA": "TAESA", # << AJUSTE
}
for key, target_canonical_name in manual_overrides.items():
    key_upper = key.upper().strip()
    normalized_key_override = normalize_key(key)
    # Sempre aplica o override manual para garantir prioridade
    if key_upper: final_map_to_canonical[key_upper] = target_canonical_name; print(f"    Override/Nome Comum: '{key_upper}' -> '{target_canonical_name}'")
    if normalized_key_override and normalized_key_override != key_upper: final_map_to_canonical[normalized_key_override] = target_canonical_name; print(f"    Override (Norm): '{normalized_key_override}' -> '{target_canonical_name}'")

# --- Geração do Arquivo JSON ---
print(f"\n--- Preparando para salvar JSON ({len(final_map_to_canonical)} entradas) ---")
print(f"--- Caminho de destino: {JSON_OUTPUT_PATH} ---")
try:
    parent_dir = os.path.dirname(JSON_OUTPUT_PATH)
    if not os.path.isdir(parent_dir): print(f"ERRO: Diretório pai '{parent_dir}' não existe.", file=sys.stderr); sys.exit(1)
    else: print(f"--- Diretório pai '{parent_dir}' verificado.")
    print(f"--- Tentando ABRIR e ESCREVER em: {JSON_OUTPUT_PATH} ---")
    with open(JSON_OUTPUT_PATH, "w", encoding="utf-8") as f:
        print(f"--- Arquivo aberto. Escrevendo JSON... ---")
        json.dump(final_map_to_canonical, f, ensure_ascii=False, indent=4, sort_keys=True)
        print(f"--- json.dump() concluído. Fechando... ---")
    print(f"--- Verificando pós escrita... ---")
    if os.path.exists(JSON_OUTPUT_PATH): print(f"--- SUCESSO: JSON '{JSON_OUTPUT_FILENAME}' salvo em: {script_dir} ---")
    else: print(f"ERRO PÓS-ESCRITA: JSON NÃO encontrado em '{JSON_OUTPUT_PATH}'.", file=sys.stderr); has_errors = True
except OSError as ose: print(f"ERRO (OSError): Falha escrever '{JSON_OUTPUT_PATH}'. Permissão?", file=sys.stderr); print(f"  {ose.errno}, {ose.strerror}", file=sys.stderr); sys.exit(1)
except Exception as e_write_json: print(f"ERRO (Geral): Falha salvar JSON '{JSON_OUTPUT_PATH}'.", file=sys.stderr); traceback.print_exc(file=sys.stderr); sys.exit(1)

# --- Finalização ---
print("\n--- Script Python concluído. ---")
if has_errors: print("AVISO: Ocorreram erros/avisos durante processamento.", file=sys.stderr)
if os.path.exists(JSON_OUTPUT_PATH): print(f"O arquivo '{JSON_OUTPUT_FILENAME}' está em: {script_dir}"); print("Execute 'mvn clean package' e reinicie o Java.")
else: print(f"ATENÇÃO: '{JSON_OUTPUT_FILENAME}' NÃO FOI ENCONTRADO.", file=sys.stderr); has_errors = True
if has_errors: sys.exit(1)
else: sys.exit(0)
