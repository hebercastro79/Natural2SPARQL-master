package com.example.Programa_heber.service;

import com.example.Programa_heber.data.DataRepository;
import com.example.Programa_heber.data.DataRepositoryEmpresas;
import com.example.Programa_heber.data.DataRepositoryNovos;
import com.example.Programa_heber.ontology.StockMarketOntology;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.vocabulary.XSD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

@RestController
@Service
public class QuestionProcessor {

    private static final Logger logger = LoggerFactory.getLogger(QuestionProcessor.class);

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.DEBUG); // Manter DEBUG para logs detalhados
    }

    private final DataRepository dataRepository;
    private final DataRepositoryNovos dataRepositoryNovos;
    private final DataRepositoryEmpresas dataRepositoryEmpresas;
    private final StockMarketOntology ontology;

    private static final String BASE_URI = "https://dcm.ffclrp.usp.br/lssb/stock-market-ontology#";
    private static final DateTimeFormatter DATE_FORMATTER_INPUT_SLASH = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    private static final DateTimeFormatter ISO_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE; // YYYY-MM-DD

    private static final Map<String, String> EMPRESA_URI_MAP = new HashMap<>();
    static {

        EMPRESA_URI_MAP.put("CSN", "CMIN3");
        EMPRESA_URI_MAP.put("CSNMINERACAO", "CMIN3");
        EMPRESA_URI_MAP.put("CSNA3", "CSNA3");
        EMPRESA_URI_MAP.put("GERDAU", "GGBR3GGBR4");
        EMPRESA_URI_MAP.put("GGBR4", "GGBR3GGBR4");
        EMPRESA_URI_MAP.put("VALE", "VALE3");
        EMPRESA_URI_MAP.put("VALE3", "VALE3");
        EMPRESA_URI_MAP.put("CBAV", "CBAV3");
        EMPRESA_URI_MAP.put("CBAV3", "CBAV3");
        EMPRESA_URI_MAP.put("ITAU", "ITUB3ITUB4");
        EMPRESA_URI_MAP.put("ITAUUNIBANCO", "ITUB3ITUB4");
        EMPRESA_URI_MAP.put("ITUB4", "ITUB3ITUB4");

    }

    private final Map<String, String> templates;
    private final Map<String, String> placeholders;

    @Autowired
    public QuestionProcessor(DataRepository dataRepository, DataRepositoryNovos dataRepositoryNovos,
                             DataRepositoryEmpresas dataRepositoryEmpresas, StockMarketOntology ontology) {
        logger.warn(">>>> CONSTRUTOR QuestionProcessor INSTANCE HASH: {}", this.hashCode());
        this.dataRepository = dataRepository;
        this.dataRepositoryNovos = dataRepositoryNovos;
        this.dataRepositoryEmpresas = dataRepositoryEmpresas;
        this.ontology = ontology;
        try {
            logger.info(">>> Carregando templates...");
            this.templates = loadTemplates();
            logger.info(">>> Templates OK.");
            logger.info(">>> Carregando placeholders...");
            this.placeholders = loadPlaceholders();
            logger.info(">>> Placeholders OK.");
            // Logger de verificação (mantido)
            logger.debug("MAP 'templates' pós-init -> Size:{}, Key 'Template 1A'?:{}, Keys:{}",
                    (this.templates != null ? this.templates.size() : "NULO"),
                    (this.templates != null ? this.templates.containsKey("Template 1A") : "N/A"), // Verifica a chave EXATA
                    (this.templates != null ? this.templates.keySet() : "NULO"));
            logger.info("QP inicializado: {} TPLs, {} PHs.",
                    (this.templates != null ? this.templates.size() : 0),
                    (this.placeholders != null ? this.placeholders.size() : 0));
        } catch (Exception e) {
            logger.error("!!! FALHA CRÍTICA CONSTRUTOR QP !!!", e);
            throw new RuntimeException("Falha ao carregar configuração (templates/placeholders).", e);
        }
    }

    @PostMapping("/processarPergunta")
    public ResponseEntity<Map<String, Object>> processQuestion(@RequestBody Map<String, String> request) {
        String pergunta = request.get("pergunta");
        if (isEmpty(pergunta)) {
            return createErrorResponse("Pergunta vazia.", HttpStatus.BAD_REQUEST);
        }
        logger.info("Recebida pergunta: '{}'", pergunta);
        logger.trace(">>>> PROCESSANDO REQ HASH: {}", this.hashCode()); // Trace
        try {
            // --- Chamada Python ---
            String pythonExecutable = "python"; // Ou o caminho completo se não estiver no PATH

            String scriptPath = "C:/Users/MENICIO JR/Desktop/Natural2SPARQL-master/src/main/resources/pln_processor.py";
            logger.trace("Exec Py: {} {} \"{}\"", pythonExecutable, scriptPath, pergunta); // Trace
            ProcessBuilder pb = new ProcessBuilder(pythonExecutable, scriptPath, pergunta);
            //pb.redirectErrorStream(true); // Junta stdout e stderr
            Process process = pb.start();
            String outputString;
            // Ler a saída do processo Python
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                outputString = reader.lines().collect(Collectors.joining(System.lineSeparator())).trim();
            }
            logger.trace("String bruta Py:\n{}", outputString); // Trace
            int exitCode = process.waitFor(); // Espera o processo terminar

            // Parse da saída JSON
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> resultPython;
            try {
                // Verificação básica se parece JSON antes de tentar parsear
                if (!outputString.startsWith("{") || !outputString.endsWith("}")) {
                    logger.error("Saída do Python não parece ser JSON. EC:{}. Saída:\n{}", exitCode, outputString);
                    throw new IOException("Formato de saída do script Python inválido (não é JSON).");
                }
                resultPython = mapper.readValue(outputString, new TypeReference<Map<String, Object>>() {});
                logger.trace("Map Jackson pós-parse: {}", resultPython); // Trace
            } catch (IOException e) {
                logger.error("Falha ao parsear JSON do Python. EC:{}. Saída:\n{}", exitCode, outputString, e);
                return createErrorResponse("Erro na comunicação com o serviço de PLN (formato inválido).", HttpStatus.INTERNAL_SERVER_ERROR);
            }

            // Verifica erros explícitos do Python ou código de saída não zero
            if (resultPython.containsKey("erro")) {
                logger.error("Erro explícito retornado pelo script Python: {}", resultPython.get("erro"));

                return createErrorResponse("Erro no processamento da linguagem natural: " + resultPython.get("erro"), HttpStatus.INTERNAL_SERVER_ERROR);
            }
            if (exitCode != 0) {
                logger.error("Script Python terminou com código de erro {} sem erro explícito no JSON. JSON: {}", exitCode, resultPython);
                return createErrorResponse("Erro inesperado no serviço de PLN.", HttpStatus.INTERNAL_SERVER_ERROR);
            }

            // Extrai mapeamentos e nome do template
            Object mapeamentosObj = resultPython.get("mapeamentos");
            String nomeTemplateCurto = (String) resultPython.get("template_nome");
            Map<String, Object> mapeamentos;
            if (mapeamentosObj instanceof Map) {
                // Cast seguro
                mapeamentos = (Map<String, Object>) mapeamentosObj;
            } else {
                logger.error("Estrutura JSON inválida: 'mapeamentos' não é um mapa. JSON: {}", resultPython);
                return createErrorResponse("Erro ao interpretar a resposta do PLN (estrutura inválida).", HttpStatus.INTERNAL_SERVER_ERROR);
            }

            logger.debug("Extr TPL Curto:'{}'", nomeTemplateCurto);
            logger.debug("Extr maps:{}", mapeamentos);
            if (isEmpty(nomeTemplateCurto) || mapeamentos == null || mapeamentos.isEmpty()) { // Verifica se mapa não é nulo ou vazio
                logger.error("Faltando nome do template ou mapeamentos na resposta do PLN. JSON: {}", resultPython);
                return createErrorResponse("Erro ao interpretar a pergunta (faltando informações).", HttpStatus.INTERNAL_SERVER_ERROR);
            }

            // --- Geração SPARQL ---
            String consultaSparql = getSparqlQuery(nomeTemplateCurto, mapeamentos);
            if (consultaSparql == null) {
                return createErrorResponse("Não foi possível montar a consulta para a pergunta.", HttpStatus.INTERNAL_SERVER_ERROR);
            }
            logger.info("SPARQL Gerada:\n{}", consultaSparql);

            // --- Execução e Formatação ---
            Query query;
            try {
                query = QueryFactory.create(consultaSparql);
                logger.debug("Query SPARQL criada com sucesso.");
            } catch (QueryParseException e) {
                logger.error("Erro de Parse na consulta SPARQL gerada.", e);
                logger.error("Query com erro:\n{}", consultaSparql);
                return createErrorResponse("Erro interno ao preparar a consulta (sintaxe inválida?).", HttpStatus.INTERNAL_SERVER_ERROR);
            } catch (Exception e) {
                logger.error("Erro inesperado ao criar objeto Query.", e);
                logger.error("Query com erro:\n{}", consultaSparql);
                return createErrorResponse("Erro interno ao preparar a consulta.", HttpStatus.INTERNAL_SERVER_ERROR);
            }

            String targetVariable = determineTargetVariable(query, mapeamentos);
            if (targetVariable == null) {
                logger.error("Não foi possível determinar a variável alvo da consulta. Vars no SELECT: {}", query.getProjectVars());
                return createErrorResponse("Erro interno ao processar a consulta (variável alvo não encontrada).", HttpStatus.INTERNAL_SERVER_ERROR);
            }
            logger.debug("Variável alvo determinada: '{}'", targetVariable);

            logger.info(">>> EXECUTANDO QUERY c/ target:'{}' <<<", targetVariable);
            List<String> results = ontology.queryAndExtractList(query, targetVariable);
            logger.info(">>> QUERY EXECUTADA. Resultados obtidos: {}. <<<", (results != null ? results.size() : "NULO"));

            Map<String, Object> finalResult = formatResults(results);
            logger.info("Resposta final formatada: {}", finalResult.get("resposta"));
            return new ResponseEntity<>(finalResult, HttpStatus.OK);

        } catch (IOException | InterruptedException e) {
            logger.error("Erro de IO ou Interrupção durante execução do script Python.", e);
            Thread.currentThread().interrupt(); // Restaura o status de interrupção
            return createErrorResponse("Erro na comunicação com o serviço de PLN.", HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            logger.error("Erro INESPERADO GLOBAL ao processar pergunta:'{}'", pergunta, e);
            return createErrorResponse("Ocorreu um erro inesperado no servidor.", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    // --- Métodos Auxiliares ---

    private ResponseEntity<Map<String, Object>> createErrorResponse(String message, HttpStatus status) {
        Map<String, Object> error = new HashMap<>();
        error.put("erro", message);
        return new ResponseEntity<>(error, status);
    }

    // --- determineTargetVariable (sem alterações da versão anterior) ---
    private String determineTargetVariable(Query query, Map<String, Object> mapeamentos) {
        String valorDesejadoKey = (String) mapeamentos.get("#VALOR_DESEJADO");
        logger.debug("Determinando var alvo.#VD='{}'", valorDesejadoKey);

        if (valorDesejadoKey != null) {
            String phAnsEspecifico = "?ANS_" + valorDesejadoKey;
            if (placeholders.containsKey(phAnsEspecifico)) {
                String varSparql = placeholders.get(phAnsEspecifico);
                if (varSparql != null && varSparql.startsWith("?")) {
                    String varName = varSparql.substring(1);
                    if (query.getProjectVars().stream().anyMatch(v -> v.getName().equalsIgnoreCase(varName))) {
                        logger.debug("Var alvo '{}' via ?ANS_{}", varName, valorDesejadoKey); return varName;
                    }
                    logger.warn("PH '{}'->'{}', mas var '{}' !SELECT.", phAnsEspecifico, varSparql, varName);
                } else { logger.warn("PH '{}' !mapeia var SPARQL ('{}').", phAnsEspecifico, varSparql); }
            } else { logger.trace("PH ?ANS_{} não encontrado.", valorDesejadoKey); } // Trace
        } else { logger.trace("#VALOR_DESEJADO não presente."); } // Trace


        String phAnsGenerico = "?ANS";
        if (placeholders.containsKey(phAnsGenerico)) {
            String ansVarSparql = placeholders.get(phAnsGenerico);
            if (ansVarSparql != null) {
                if (ansVarSparql.startsWith("?")) {
                    String ansVarName = ansVarSparql.substring(1);
                    if (query.getProjectVars().stream().anyMatch(v -> v.getName().equalsIgnoreCase(ansVarName))) {
                        logger.debug("Var alvo '{}' via placeholder ?ANS.", ansVarName); return ansVarName;
                    }
                    logger.trace("?ANS->'{}', mas var !SELECT.", ansVarSparql); // Trace
                } else if (ansVarSparql.equals("#VALOR_DESEJADO") && valorDesejadoKey != null) {
                    String phAnsEspecificoRetry = "?ANS_" + valorDesejadoKey;
                    if (placeholders.containsKey(phAnsEspecificoRetry)) {
                        String varSparqlRetry = placeholders.get(phAnsEspecificoRetry);
                        if (varSparqlRetry != null && varSparqlRetry.startsWith("?")) {
                            String varNameRetry = varSparqlRetry.substring(1);
                            if (query.getProjectVars().stream().anyMatch(v -> v.getName().equalsIgnoreCase(varNameRetry))) {
                                logger.debug("Var alvo '{}' via ?ANS->#VD->?ANS_{}", varNameRetry, valorDesejadoKey); return varNameRetry;
                            }
                        }
                    }
                } else { logger.warn("?ANS ('{}') !mapeia var ou #VALOR_DESEJADO.", ansVarSparql); }
            }
        } else { logger.trace("PH ?ANS não definido."); } // Trace


        List<org.apache.jena.sparql.core.Var> projectVars = query.getProjectVars();
        if (projectVars != null && !projectVars.isEmpty()) {
            String firstVarName = projectVars.get(0).getName();
            logger.warn("Var alvo !determinada. Usando primeira do SELECT:'{}'", firstVarName); return firstVarName;
        }
        logger.error("Nenhuma variável no SELECT! Não foi possível determinar var alvo."); return null;
    }


    private Map<String, Object> formatResults(List<String> results) {
        Map<String, Object> finalResult = new HashMap<>(); String respostaFormatada;
        if (results == null || results.isEmpty()) { respostaFormatada = "Não foram encontrados resultados para sua pergunta."; }
        else if (results.size() == 1) { respostaFormatada = results.get(0) != null ? results.get(0) : "Resultado encontrado, mas valor é nulo."; } // Trata null
        else { StringBuilder sb = new StringBuilder("Resultados encontrados:\n"); results.forEach(item -> sb.append("- ").append(item != null ? item : "(valor nulo)").append("\n")); respostaFormatada = sb.toString().trim(); }
        finalResult.put("resposta", respostaFormatada); return finalResult;
    }

    // --- loadTemplates e loadPlaceholders (sem alterações da versão anterior) ---
    private Map<String, String> loadTemplates() throws IOException {
        Map<String, String> loadedTemplates = new HashMap<>(); String resourcePath = "/templates.txt";
        InputStream inputStream = getClass().getResourceAsStream(resourcePath);
        if (inputStream == null) { logger.error("!!! FATAL: '{}' NÃO ENCONTRADO !!!", resourcePath); throw new IOException("Recurso não encontrado: " + resourcePath); }
        else { try { URL u = getClass().getResource(resourcePath); logger.debug(">>>> Carregando templates.txt URL: {}", (u!=null?u.toExternalForm():"NULA")); } catch(Exception ex) { logger.error(">> Erro URL TPL {}: {}", resourcePath, ex.getMessage()); } }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line; String currentKey = null; StringBuilder currentContent = new StringBuilder(); int lineNum = 0;
            while ((line = reader.readLine()) != null) {
                lineNum++; String trimmedLine = line.trim();
                // Usa regex mais flexível para chave (permite números e hífen no nome curto)
                if (trimmedLine.matches("^Template\\s+[\\w\\-]+\\s*-\\s*.*") && trimmedLine.contains(" - ")) {
                    if (currentKey != null && currentContent.length() > 0) { loadedTemplates.put(currentKey, currentContent.toString().trim()); }
                    currentKey = trimmedLine.split(" - ", 2)[0].trim(); // Pega só a parte antes do primeiro " - "
                    logger.trace("(loadTemplates) L{}: Nova chave:'{}'", lineNum, currentKey); currentContent = new StringBuilder();
                } else if (currentKey != null && !trimmedLine.isEmpty() && !trimmedLine.startsWith("#")) { currentContent.append(line).append(System.lineSeparator()); }
            }
            if (currentKey != null && currentContent.length() > 0) { loadedTemplates.put(currentKey, currentContent.toString().trim()); }
        } catch (IOException e) { logger.error("Erro DENTRO leitura '{}'.", resourcePath, e); throw e; }
        if (loadedTemplates.isEmpty()) { logger.error("!!! CRÍTICO: 0 templates carregados de '{}' !!!", resourcePath); }
        logger.info("=== Templates Carregados: {}. Chaves: {}", loadedTemplates.size(), loadedTemplates.keySet()); return loadedTemplates;
    }

    private Map<String, String> loadPlaceholders() throws IOException {
        Map<String, String> loadedPlaceholders = new HashMap<>(); String resourcePath = "/placeholders.txt";
        InputStream inputStream = getClass().getResourceAsStream(resourcePath);
        if (inputStream == null) { logger.error("!!! FATAL: '{}' NÃO ENCONTRADO !!!", resourcePath); throw new IOException("Recurso não encontrado: " + resourcePath); }
        else { try { URL u = getClass().getResource(resourcePath); logger.debug(">>>> Carregando placeholders.txt URL: {}", (u!=null?u.toExternalForm():"NULA")); } catch(Exception ex) { logger.error(">> Erro URL PH {}: {}", resourcePath, ex.getMessage()); } }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line; int lineNum = 0;
            while ((line = reader.readLine()) != null) {
                lineNum++; String trimmedLine = line.trim();
                if (!trimmedLine.isEmpty() && !trimmedLine.startsWith("#") && trimmedLine.contains(" - ")) {
                    String[] parts = trimmedLine.split(" - ", 2);
                    if (parts.length == 2 && !isEmpty(parts[0]) && !isEmpty(parts[1])) {
                        loadedPlaceholders.put(parts[0].trim(), parts[1].trim()); logger.trace("PH: '{}' -> '{}'", parts[0].trim(), parts[1].trim());
                    } else { logger.warn("Formato Inv. placeholders.txt L{}: {}", lineNum, trimmedLine); }
                }
            }
        } catch (IOException e) { logger.error("Erro DENTRO leitura '{}'.", resourcePath, e); throw e; }
        if (loadedPlaceholders.isEmpty()) { logger.warn("Nenhum placeholder carregado de '{}'", resourcePath); }
        logger.info("=== Placeholders Carregados: {}. Chaves: {}", loadedPlaceholders.size(), loadedPlaceholders.keySet()); return loadedPlaceholders;
    }



    private String getSparqlQuery(String nomeTemplateCurto, Map<String, Object> mapeamentos) {
        logger.debug(">>> Buscando template. Chave CURTA: '{}'", nomeTemplateCurto);
        logger.trace(">>> Mapa 'templates': {}", this.templates.keySet()); // Trace
        String template = null; String chaveLongaEncontrada = null;

        if (this.templates.containsKey(nomeTemplateCurto)) {
            template = this.templates.get(nomeTemplateCurto);
            chaveLongaEncontrada = nomeTemplateCurto;
            logger.info(">>> SUCESSO: Template encontrado diretamente pela chave CURTA '{}'", nomeTemplateCurto);
        } else {

            String prefixoBusca = nomeTemplateCurto + " - ";
            for (Map.Entry<String, String> entry : this.templates.entrySet()) {
                if (entry.getKey().trim().startsWith(prefixoBusca)) {
                    template = entry.getValue();
                    chaveLongaEncontrada = entry.getKey();
                    logger.warn(">>> Template encontrado via PREFIXO! Curta '{}' -> Longa '{}'. Verifique se a chave no map está correta.", nomeTemplateCurto, chaveLongaEncontrada);
                    break;
                }
            }
        }

        if (template == null) {
            logger.error("!!! ERRO FATAL getSparqlQuery: Template NÃO encontrado para chave CURTA '{}'. Keys disponíveis: {}", nomeTemplateCurto, this.templates.keySet());
            return null;
        }
        logger.trace(">>> Usando template bruto (key:'{}')", chaveLongaEncontrada); // Trace

        String consultaIntermediaria = template;

        // --- FASE 1 (Substituição Genérica - P/O/S e variáveis) ---
        logger.trace(">>> Fase 1 Substituição (Genéricos)"); // Trace
        for(Map.Entry<String, String> ph : placeholders.entrySet()){
            String placeholderKey = ph.getKey(); // Ex: "?P0", "<...#P0>", "?ANS_PRECO", etc.
            String substitutionValue = ph.getValue(); // Ex: "stock:temValorMobiliarioNegociado", "?preco", etc.
            String placeholderInTemplate = null;
            String valueToSubstitute = null;

            if(placeholderKey.startsWith("?")){ // É uma variável (ex: ?ANS_PRECO -> ?preco)
                placeholderInTemplate = placeholderKey;
                valueToSubstitute = substitutionValue; // Mantém como está (?preco)
                logger.trace("F1: Var placeholder '{}' -> '{}'", placeholderKey, valueToSubstitute);
            } else if(placeholderKey.matches("^[PpOo]\\d+$")){ // É um Predicado/Objeto genérico (ex: P0 -> stock:tem...)
                placeholderInTemplate = "<...#" + placeholderKey + ">"; // Forma no template
                if(substitutionValue.startsWith("#")){ // Mapeia para algo semântico (ex: #DATA) - adiar para FASE 2
                    logger.trace("F1: Placeholder '{}' mapeia para '{}'. Adiar para Fase 2.", placeholderInTemplate, substitutionValue);
                } else { // Mapeia para uma URI de predicado/classe
                    String resourceName = substitutionValue;
                    String uri = ontology.getPredicateURI(resourceName); // Tenta como predicado
                    if (uri != null) {
                        valueToSubstitute = "<" + uri + ">";
                        logger.trace("F1: Subst Gen P/O '{}' -> Predicado '{}'", placeholderInTemplate, valueToSubstitute);
                    } else {
                        // Se não for predicado conhecido, assume que é classe ou recurso e constrói URI
                        valueToSubstitute = "<" + BASE_URI + resourceName + ">";
                        logger.trace("F1: Subst Gen P/O '{}' -> URI Classe/Rec Assumida '{}'", placeholderInTemplate, valueToSubstitute);
                    }
                }
            } else {
                // Outros tipos de placeholder (ignorar na Fase 1?)
                logger.trace("F1: Placeholder não tratado '{}'", placeholderKey);
                continue;
            }

            // Realiza a substituição se placeholder e valor existem
            if (valueToSubstitute != null && placeholderInTemplate != null && consultaIntermediaria.contains(placeholderInTemplate)) {
                // Substitui TODAS as ocorrências
                consultaIntermediaria = consultaIntermediaria.replace(placeholderInTemplate, valueToSubstitute);
            } else if (valueToSubstitute == null && placeholderInTemplate != null && consultaIntermediaria.contains(placeholderInTemplate)) {
                logger.warn("F1: Placeholder '{}' presente no template mas sem valor de substituição definido.", placeholderInTemplate);
            } else if (valueToSubstitute != null && placeholderInTemplate != null && !consultaIntermediaria.contains(placeholderInTemplate)) {
                // Loga se o placeholder definido não foi encontrado no template atual (pode ser normal)
                logger.trace("F1: Placeholder genérico '{}' não encontrado no template atual.", placeholderInTemplate);
            }
        }

        // --- FASE 2 (Substituição Semântica - #ENTIDADE, #DATA, etc.) ---
        logger.trace(">>> Fase 2 Substituição (Semânticos Diretos)"); // Trace
        for (Map.Entry<String,Object> entry : mapeamentos.entrySet()){
            String semanticPlaceholder = entry.getKey(); // Ex: "#DATA", "#ENTIDADE"
            Object valueObject = entry.getValue();
            String valueString = (valueObject != null) ? valueObject.toString() : null;

            if(!isEmpty(valueString) && !semanticPlaceholder.equals("#VALOR_DESEJADO")){
                // Determinar o placeholder correspondente no template
                String placeholderInTemplate = null;
                // Tentar placeholder direto primeiro (ex: <...#DATA>)
                String directPlaceholder = "<...#" + semanticPlaceholder.substring(1) + ">"; // Remove #
                if (consultaIntermediaria.contains(directPlaceholder)) {
                    placeholderInTemplate = directPlaceholder;
                } else {
                    // Tentar encontrar via mapeamento indireto (ex: P0 -> #DATA => buscar <...#P0>)
                    for(Map.Entry<String, String> phEntry : placeholders.entrySet()){
                        if(phEntry.getValue().equals(semanticPlaceholder) && phEntry.getKey().matches("^[PpOo]\\d+$")) {
                            String indirectKey = "<...#" + phEntry.getKey() + ">";
                            if (consultaIntermediaria.contains(indirectKey)) {
                                placeholderInTemplate = indirectKey;
                                logger.trace("F2: Usando placeholder indireto '{}' para semântico '{}'", placeholderInTemplate, semanticPlaceholder);
                                break;
                            }
                        }
                    }
                }

                // Se nenhum placeholder foi encontrado no template, pula este mapeamento
                if (placeholderInTemplate == null) {
                    logger.trace("F2: Placeholder para semântico '{}' (direto '{}' ou indireto) não encontrado no template atual.", semanticPlaceholder, directPlaceholder);
                    continue;
                }

                // --- Preparar o valor da substituição ---
                String valueToSubstitute = null;
                if (semanticPlaceholder.equals("#DATA")) {
                    // CORREÇÃO: Resolve para ISO e formata como literal xsd:date
                    String dataISO = resolveDataToISODate(valueString); // Retorna YYYY-MM-DD
                    if (dataISO != null) {
                        // Formata como literal SPARQL: "YYYY-MM-DD"^^xsd:date
                        valueToSubstitute = "\"" + dataISO + "\"^^<" + XSD.date.getURI() + ">"; // Usa URI completa do XSD
                        logger.trace("F2: Subst #DATA -> '{}'", valueToSubstitute);
                    } else {
                        logger.warn("F2: Falha ao resolver #DATA para formato ISO: '{}'", valueString);
                    }
                } else if (semanticPlaceholder.equals("#ENTIDADE")) {
                    String uriPart = resolveEmpresa(valueString); // Retorna ID da empresa (ex: CSNA3)
                    if (uriPart != null) {
                        valueToSubstitute = "<" + BASE_URI + uriPart + ">"; // Constrói URI completa
                        logger.trace("F2: Subst #ENTIDADE -> '{}'", valueToSubstitute);
                    } else {
                        logger.warn("F2: Falha ao resolver #ENTIDADE: '{}'. A consulta pode falhar.", valueString);

                    }
                } else if (semanticPlaceholder.equals("#TIPO_ACAO")) {
                    // Assume que o valor já é a parte final da URI (Ordinaria/Preferencial)
                    valueToSubstitute = "<" + BASE_URI + valueString.trim().toUpperCase() + ">"; // Garante trim/uppercase
                    logger.trace("F2: Subst #TIPO_ACAO -> '{}'", valueToSubstitute);
                } else if (semanticPlaceholder.equals("#SETOR")) {
                    // Usa createUriSafe para gerar a parte final da URI do setor
                    String setorUriPart = createUriSafe(valueString);
                    valueToSubstitute = "<" + BASE_URI + setorUriPart + ">";
                    logger.trace("F2: Subst #SETOR -> '{}'", valueToSubstitute);
                }
                // Adicionar outros casos semânticos se necessário (#INDICADOR, etc.)
                else {
                    logger.warn("F2: Placeholder semântico não tratado: '{}'", semanticPlaceholder);
                }

                // Realiza a substituição se o valor foi preparado
                if (valueToSubstitute != null) {
                    // Substitui TODAS as ocorrências do placeholder encontrado
                    consultaIntermediaria = consultaIntermediaria.replace(placeholderInTemplate, valueToSubstitute);
                    logger.trace("F2: Realizada substituição de '{}' por '{}'", placeholderInTemplate, valueToSubstitute);
                } else {
                    logger.warn("F2: Não foi possível gerar valor de substituição para '{}' ('{}'). Placeholder '{}' NÃO será substituído.",
                            semanticPlaceholder, valueString, placeholderInTemplate);
                    // Considerar remover a linha/parte do WHERE que contém o placeholder não resolvido? (Complexo)
                }
            }
        }
        logger.info(">>> SPARQL Final (pós-substituições):\n{}", consultaIntermediaria);
        return consultaIntermediaria;
    }


    // --- MÉTODO resolveData CORRIGIDO para retornar "YYYY-MM-DD" ---
    private String resolveDataToISODate(String dataInput) {
        if (isEmpty(dataInput)) return null;
        String trimmedData = dataInput.trim();
        LocalDate parsedDate = null;
        try {
            // Tenta ISO primeiro
            parsedDate = LocalDate.parse(trimmedData, ISO_DATE_FORMATTER);
            logger.trace("Data '{}' parseada como ISO.", dataInput);
        } catch (DateTimeParseException e0) {
            logger.trace("Data '{}' não é ISO. Tentando dd/MM/yyyy...", dataInput);
            try {
                // Tenta formato com barra
                parsedDate = LocalDate.parse(trimmedData, DATE_FORMATTER_INPUT_SLASH);
                logger.trace("Data '{}' parseada como dd/MM/yyyy.", dataInput);
            } catch (DateTimeParseException e1) {
                logger.trace("Data '{}' não é dd/MM/yyyy. Tentando 'hoje'/'ontem'...", dataInput);
                // Tenta palavras chave
                String lowerData = trimmedData.toLowerCase();
                if (lowerData.contains("hoje")) {
                    parsedDate = LocalDate.now();
                    logger.trace("Data '{}' resolvida como 'hoje'.", dataInput);
                } else if (lowerData.contains("ontem")) {
                    parsedDate = LocalDate.now().minusDays(1);
                    logger.trace("Data '{}' resolvida como 'ontem'.", dataInput);
                } else if (lowerData.matches("^\\d{8}$")) { // Tenta YYYYMMDD
                    try {
                        parsedDate = LocalDate.parse(lowerData, DateTimeFormatter.BASIC_ISO_DATE);
                        logger.trace("Data '{}' parseada como YYYYMMDD.", dataInput);
                    } catch (DateTimeParseException e3) { /* Ignora se falhar */ }
                }
                // Adicionar outras palavras chave se necessário
            }
        }

        if (parsedDate != null) {
            // CORREÇÃO: Formata para YYYY-MM-DD
            String fmt = parsedDate.format(ISO_DATE_FORMATTER);
            logger.debug("Data '{}' resolvida para ISO -> '{}'", dataInput, fmt);
            return fmt;
        } else {
            logger.warn("Data não reconhecida ou formato inválido para ISO Date: '{}'.", dataInput);
            return null;
        }
    }


    // --- MÉTODO resolveEmpresa CORRIGIDO para usar mapa e fallback ---
    private String resolveEmpresa(String empresaInput) {
        if(isEmpty(empresaInput)) return null;
        // Normaliza chave para busca no mapa (maiúsculas, sem ponto, sem espaço extra)
        String keyNormalizada = empresaInput.toUpperCase().replace(".", "").trim();
        String uriPart = EMPRESA_URI_MAP.get(keyNormalizada);

        if(uriPart != null){
            logger.debug("Empresa '{}' (norm:'{}') mapeada para URI part -> '{}'", empresaInput, keyNormalizada, uriPart);
            return uriPart;
        } else {

            String fallbackUriPart = createUriSafe(keyNormalizada); // <<< Usa a versão consistente de createUriSafe
            logger.warn("Empresa '{}' (norm:'{}') não encontrada no mapa EMPRESA_URI_MAP. Usando fallback URI safe: '{}'. Verifique o mapeamento!",
                    empresaInput, keyNormalizada, fallbackUriPart);
            return fallbackUriPart;

        }
    }


    // --- createUriSafe (IDÊNTICO AO StockMarketOntology) e isEmpty ---
    private String createUriSafe(String input) {
        if (isEmpty(input)) return "id_" + UUID.randomUUID().toString();
        String sanitized = input.trim()
                .replaceAll("\\s+", "_") // Espaço para underscore
                .replaceAll("[^a-zA-Z0-9_\\-\\.]", ""); // Permite letras, numeros, _, -, . (remove outros)
        sanitized = Normalizer.normalize(sanitized, Normalizer.Form.NFD)
                .replaceAll("[^\\p{ASCII}]", ""); // Remove acentos/não-ASCII
        // Re-aplica filtro para garantir
        sanitized = sanitized.replaceAll("[^a-zA-Z0-9_\\-\\.]", "");
        // Garante que começa com letra (prefixo 'z_' se necessário)
        if (!sanitized.isEmpty() && !Character.isLetter(sanitized.charAt(0))) {
            sanitized = "z_" + sanitized;
        }
        if (isEmpty(sanitized)) return "id_" + UUID.randomUUID().toString();
        return sanitized;
    }
    private boolean isEmpty(String s){return s==null||s.trim().isEmpty();}

} // Fim da classe QuestionProcessor