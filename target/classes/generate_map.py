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
try:
    # Tenta obter o diretório do script. Funciona se executado como arquivo.
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Fallback se __file__ não está definido (ex: executado interativamente)
    script_dir = os.path.abspath(".")
print(f"Diretório de execução do script: {script_dir}")

# Assume que a pasta 'datasets' está 3 níveis acima de onde o script está (src/main/resources)
# Ajuste o número de ".." se a estrutura do projeto for diferente
project_root_dir = os.path.abspath(os.path.join(script_dir, "..", "..", ".."))
datasets_dir = os.path.join(project_root_dir, "datasets")
print(f"Diretório esperado para datasets: {datasets_dir}")

# Arquivos Excel a serem lidos da pasta 'datasets'
# Certifique-se que estes arquivos existem no caminho 'datasets_dir'
EXCEL_FILES = [
    os.path.join(datasets_dir, "dados_novos_atual.xlsx"),
    os.path.join(datasets_dir, "dados_novos_anterior.xlsx"),
    # Descomente a linha abaixo se você tiver e quiser usar este arquivo também
    # os.path.join(datasets_dir, "Informacoes_Empresas.xlsx"),
]

# Configuração das colunas (índices baseados em 0)
# Verifique se estes índices correspondem às colunas corretas nos seus arquivos Excel
# Coluna E = Índice 4
TICKER_COL_IDX = 4
# Coluna G = Índice 6
COMPANY_NAME_COL_IDX = 6
# Assume que os dados estão na primeira planilha (índice 0)
SHEET_NAME = 0

# Nome e caminho completo para o arquivo JSON de saída
JSON_OUTPUT_FILENAME = "empresa_nome_map.json"
JSON_OUTPUT_PATH = os.path.join(script_dir, JSON_OUTPUT_FILENAME) # Salva na mesma pasta do script

# --- Funções Auxiliares ---

def normalize_text_for_map(text):
    """Normaliza texto para usar como chave de mapa: maiúsculas, sem acentos, remove pontuação e termos comuns."""
    if not isinstance(text, str): return None
    text_upper = text.upper().strip()
    try:
        # Remove acentos (NFD normaliza, Mn filtra caracteres de combinação)
        text_norm = ''.join(c for c in unicodedata.normalize('NFD', text_upper) if unicodedata.category(c) != 'Mn')
    except Exception as e_norm:
        print(f" Aviso: Falha ao remover acentos de '{text_upper[:50]}...': {e_norm}", file=sys.stderr)
        text_norm = text_upper # Usa o texto original em maiúsculas se a normalização falhar

    # Remove termos comuns (S.A., CIA, ON, PN, etc.) usando regex case-insensitive
    # Ajuste esta lista conforme necessário
    common_terms_pattern = r'\b(S\.?A\.?|S/?A|CIA\.?|COMPANHIA|HOLDING|PARTICIPACOES|PART|FUNDO|INVESTIMENTO|INVEST|FI|FII|ON|PN|ED|EJ|N[12]|PREF|ORD|UNT|SA|S A)\b'
    text_clean = re.sub(common_terms_pattern, '', text_norm, flags=re.IGNORECASE)

    # Remove caracteres que não sejam letras ou números
    text_clean = re.sub(r'[^\w]', '', text_clean) # \w inclui letras, números e underscore

    # Remove espaços extras (embora a remoção de não-\w já deva ter feito isso)
    text_clean = re.sub(r'\s+', '', text_clean).strip()

    # Retorna a chave normalizada ou None se estiver vazia
    return text_clean if text_clean else None

def is_valid_ticker(ticker):
    """Verifica se a string parece um ticker B3 válido (4 letras + 1 ou 2 números)."""
    if not isinstance(ticker, str): return False
    # ^[A-Z]{4} - Começa com exatamente 4 letras maiúsculas
    # \d{1,2}  - Seguido por 1 ou 2 dígitos
    # $        - Fim da string
    return re.match(r'^[A-Z]{4}\d{1,2}$', ticker.strip().upper()) is not None

# --- Processamento Principal ---

# Dicionário para armazenar dados temporários: ticker -> {set de nomes, set de tickers}
ticker_to_data = defaultdict(lambda: {"names": set(), "tickers": set()})
processed_rows = 0
errors_reading = 0
files_processed_count = 0 # Contador de arquivos Excel realmente processados

print("--- Fase 1: Lendo Arquivos Excel e Coletando Dados ---")
for file_path in EXCEL_FILES:
    # Verifica se o arquivo existe antes de tentar abrir
    if not os.path.exists(file_path):
        print(f"AVISO: Arquivo não encontrado: {file_path}. Pulando.", file=sys.stderr)
        continue # Pula para o próximo arquivo da lista

    print(f"Processando arquivo: {os.path.basename(file_path)}...")
    try:
        # Lê o Excel especificando as colunas e o tipo de dado como string
        df = pd.read_excel(file_path, sheet_name=SHEET_NAME, header=0,
                           usecols=[TICKER_COL_IDX, COMPANY_NAME_COL_IDX], dtype=str)
        files_processed_count += 1 # Incrementa contador de arquivos lidos

        # Itera sobre as linhas do DataFrame
        for index, row in df.iterrows():
            row_num = index + 2 # Número da linha no Excel (para logs)
            try:
                # Acessa as colunas pelo índice relativo às colunas lidas (0 e 1)
                ticker_raw = row.iloc[0] # Primeira coluna lida (TICKER_COL_IDX)
                name_raw = row.iloc[1]   # Segunda coluna lida (COMPANY_NAME_COL_IDX)

                # Validação e Limpeza do Ticker
                if not isinstance(ticker_raw, str):
                    #logger.trace(f"  L{row_num}: Ticker não é string ({type(ticker_raw)}). Pulando.")
                    continue
                ticker = ticker_raw.strip().upper()
                if not is_valid_ticker(ticker):
                    #logger.trace(f"  L{row_num}: Ticker inválido '{ticker}'. Pulando.")
                    continue

                # Tratamento do Nome da Empresa
                # Usa o ticker como nome se o nome estiver vazio, for NaN ou não for string
                if isinstance(name_raw, str) and name_raw.strip():
                    company_name = name_raw.strip()
                else:
                    company_name = ticker # Fallback para o próprio ticker

                # Adiciona os dados coletados ao dicionário temporário
                ticker_to_data[ticker]["names"].add(company_name)
                ticker_to_data[ticker]["tickers"].add(ticker) # Garante que o ticker está no seu próprio set
                processed_rows += 1

            except Exception as e_row:
                # Erro ao processar uma linha específica
                print(f"  AVISO: Erro ao processar linha {row_num} de {os.path.basename(file_path)}: {e_row}", file=sys.stderr)
                errors_reading +=1

    except FileNotFoundError:
         # Esse erro não deveria ocorrer por causa do os.path.exists, mas por segurança
         print(f"ERRO: Arquivo '{os.path.basename(file_path)}' desapareceu após verificação?", file=sys.stderr)
         errors_reading += 1
    except ValueError as ve:
         # Erro comum se os índices das colunas estiverem errados ou fora do range
         print(f"ERRO: Falha ao ler colunas do arquivo '{os.path.basename(file_path)}'. Verifique os índices TICKER_COL_IDX ({TICKER_COL_IDX}) e COMPANY_NAME_COL_IDX ({COMPANY_NAME_COL_IDX}). Detalhes: {ve}", file=sys.stderr)
         errors_reading += 1
    except Exception as e_file:
        # Outro erro geral ao ler o arquivo
        print(f"ERRO: Falha geral ao ler ou processar o arquivo '{os.path.basename(file_path)}': {e_file}", file=sys.stderr)
        errors_reading += 1
        # Pode decidir parar o script se um arquivo falhar completamente
        # sys.exit(1)

# Verifica se algum dado foi processado
if files_processed_count == 0:
     print("\nERRO FATAL: Nenhum dos arquivos Excel especificados foi encontrado no diretório 'datasets'. Verifique os caminhos.", file=sys.stderr)
     sys.exit(1)
if processed_rows == 0:
    print("\nERRO FATAL: Nenhum dado válido (ticker + nome) foi extraído dos arquivos Excel processados. Verifique o conteúdo e formato das planilhas e os índices das colunas.", file=sys.stderr)
    sys.exit(1)

print(f"--- Leitura dos Excels concluída. ---")
print(f"  {processed_rows} linhas válidas processadas.")
print(f"  {len(ticker_to_data)} tickers únicos encontrados.")
if errors_reading > 0:
    print(f"AVISO: Ocorreram {errors_reading} erros durante a leitura/processamento das linhas. Verifique os logs acima.", file=sys.stderr)

# --- Fase 2: Construir o Mapa Final ---
final_map = {} # O dicionário que será salvo como JSON
ticker_primary_map = {} # Mapa auxiliar: ticker -> ticker_primario_escolhido

print("\n--- Fase 2: Construindo o Mapa JSON Final ---")

# 2a: Determinar o ticker primário para cada grupo de tickers associados
print("  Determinando ticker primário para cada grupo (Regra: PN > UNIT > ON > Outros)...")
for ticker, data in ticker_to_data.items():
    # Define a regra de prioridade (menor número = maior prioridade)
    def get_priority(t):
        if t.endswith('4'): return 1  # PN
        if t.endswith('11'): return 2 # UNIT
        if t.endswith('3'): return 3  # ON
        if t.endswith('5'): return 4  # PNA (Exemplo)
        if t.endswith('6'): return 5  # PNB (Exemplo)
        return 99 # Outros

    # Encontra o ticker com a menor prioridade (maior preferência)
    primary_ticker = min(data["tickers"], key=get_priority)
    ticker_primary_map[ticker] = primary_ticker
    if len(data["tickers"]) > 1:
         print(f"    Grupo {ticker}: Tickers={data['tickers']}, Primário Escolhido='{primary_ticker}'")

# 2b: Construir o mapa inicial com base nos dados coletados
print("  Construindo mapeamentos automáticos Nome/Ticker -> Ticker Primário...")
for ticker, data in ticker_to_data.items():
    primary_ticker = ticker_primary_map[ticker] # O ticker primário para este grupo

    # Mapeia o ticker original (e todos no grupo) para o primário
    for t_in_group in data["tickers"]:
        final_map[t_in_group.upper()] = primary_ticker # Chave é o ticker em maiúsculas

    # Mapeia todas as variações de nome encontradas para o ticker primário
    for name in data["names"]:
        # Mapeia o nome exato (convertido para maiúsculas)
        exact_name_key = name.upper().strip()
        if exact_name_key and exact_name_key not in final_map: # Adiciona só se não for um ticker já mapeado
             final_map[exact_name_key] = primary_ticker

        # Mapeia o nome normalizado (sem acentos, pontuação, etc.)
        normalized_name_key = normalize_text_for_map(name)
        if normalized_name_key and normalized_name_key not in final_map:
            final_map[normalized_name_key] = primary_ticker


# 2c: Aplicar Overrides Manuais (SOBRESCREVEM TUDO para as chaves especificadas)
print("  Aplicando overrides manuais (SOBRESCREVENDO mapeamentos anteriores)...")
# !!! VERIFIQUE E AJUSTE OS VALORES (TICKERS PRIMÁRIOS) CONFORME SUA NECESSIDADE !!!
manual_overrides = {
    # Chave (como usuário pode digitar) -> Valor (TICKER PRIMÁRIO CORRETO)
    "CSN": "CMIN3",
    "CSN MINERACAO": "CMIN3",
    "COMPANHIA SIDERURGICA NACIONAL": "CSNA3",
    "CSNA3": "CSNA3",
    "CMIN3": "CMIN3",
    "GERDAU": "GGBR4",
    "METALURGICA GERDAU": "GGBR4",
    "GGBR3": "GGBR4", # Aponta ON para PN primário
    "GGBR4": "GGBR4", # Aponta PN para si mesmo
    "GERDAUMET": "GOAU4", # Primário para Metalúrgica Gerdau
    "GOAU3": "GOAU4",
    "GOAU4": "GOAU4",
    "VALE": "VALE3",
    "VALE ON": "VALE3",
    "VALE3": "VALE3",
    "ITAU": "ITUB4",
    "ITAU UNIBANCO": "ITUB4",
    "ITAU UNIBANCO HOLDING": "ITUB4",
    "ITUB3": "ITUB4",
    "ITUB4": "ITUB4",
    "PETROBRAS": "PETR4",
    "PETROBRAS PN": "PETR4",
    "PETR3": "PETR4",
    "PETR4": "PETR4",
    "TAURUS ARMAS": "TASA4",
    "TAURUSARMAS": "TASA4",
    "TAURUS": "TASA4",
    "TASA3": "TASA4",
    "TASA4": "TASA4",
    "TENDA": "TEND3",
    "CONSTRUTORA TENDA": "TEND3",
    "TEND3": "TEND3",
    "TAESA": "TAEE11",
    "TRANS PAULISTA": "TAEE11",
    "TAEE3": "TAEE11",
    "TAEE4": "TAEE11",
    "TAEE11": "TAEE11",
    "CBA": "CBAV3",
    "CBAV3": "CBAV3"
    # Adicione mais overrides se necessário
}

override_applied_count = 0
for key, primary_ticker_target in manual_overrides.items():
    # Chave exata em maiúsculas
    exact_key = key.upper().strip()
    # Chave normalizada
    normalized_key = normalize_text_for_map(key)

    applied_override = False
    # Aplica/Sobrescreve para a chave exata
    if final_map.get(exact_key) != primary_ticker_target:
        final_map[exact_key] = primary_ticker_target
        print(f"    Override: '{exact_key}' -> '{primary_ticker_target}' (Sobrescrito/Adicionado)")
        applied_override = True

    # Aplica/Sobrescreve para a chave normalizada (se diferente da exata)
    if normalized_key and normalized_key != exact_key:
         if final_map.get(normalized_key) != primary_ticker_target:
              final_map[normalized_key] = primary_ticker_target
              print(f"    Override (Norm): '{normalized_key}' -> '{primary_ticker_target}' (Sobrescrito/Adicionado)")
              applied_override = True
    
    if applied_override: override_applied_count += 1

print(f"  {override_applied_count} overrides manuais aplicados/confirmados.")


# --- Fase 3: Salvar o Mapa como Arquivo JSON ---
print(f"\n--- Fase 3: Salvando o Mapa JSON ---")
print(f"  Total de entradas no mapa final: {len(final_map)}")
print(f"  Salvando em: {JSON_OUTPUT_PATH}")

try:
    # Garante que o diretório de destino exista
    os.makedirs(os.path.dirname(JSON_OUTPUT_PATH), exist_ok=True)

    # Abre o arquivo para escrita em UTF-8
    with open(JSON_OUTPUT_PATH, "w", encoding="utf-8") as f:
        # Escreve o dicionário 'final_map' no arquivo JSON
        # indent=4 para formatação legível
        # sort_keys=True para garantir ordem consistente (facilita diffs)
        # ensure_ascii=False para permitir caracteres acentuados diretamente
        json.dump(final_map, f, ensure_ascii=False, indent=4, sort_keys=True)

    print(f"--- SUCESSO: Arquivo JSON '{JSON_OUTPUT_FILENAME}' salvo com sucesso! ---")

    # Verificação simples pós-escrita
    if os.path.exists(JSON_OUTPUT_PATH) and os.path.getsize(JSON_OUTPUT_PATH) > 2: # > 2 para garantir que não é só '{}'
        print("--- Verificação pós-escrita: Arquivo existe e contém dados.")
    else:
        print("ERRO: Verificação pós-escrita falhou. O arquivo pode não ter sido criado ou está vazio.", file=sys.stderr)
        errors_reading += 1 # Reutiliza flag para indicar erro geral

except Exception as e_write:
    print(f"ERRO CRÍTICO: Falha ao salvar o arquivo JSON em '{JSON_OUTPUT_PATH}'.", file=sys.stderr)
    # Imprime o traceback completo para depuração
    print(traceback.format_exc(), file=sys.stderr)
    errors_reading += 1

# --- Finalização ---
print("\n--- Geração do Mapa concluída. ---")
if errors_reading > 0:
    print("AVISO: Ocorreram erros ou avisos durante o processo. Verifique os logs acima.", file=sys.stderr)
    sys.exit(1) # Sai com código de erro se houve problemas
else:
    print(f"O arquivo '{JSON_OUTPUT_FILENAME}' foi gerado/atualizado em: {script_dir}")
    print("Execute 'mvn clean package' (se aplicável) e reinicie a aplicação Java para usar o novo mapa.")
    sys.exit(0) # Sai com sucesso