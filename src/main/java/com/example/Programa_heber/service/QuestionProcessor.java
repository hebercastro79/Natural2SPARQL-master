package com.example.Programa_heber.service;

// --- IMPORTS ---
import com.example.Programa_heber.ontology.Ontology;
import org.apache.jena.query.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.vocabulary.XSD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.text.Normalizer;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class QuestionProcessor {

    private static final Logger logger = LoggerFactory.getLogger(QuestionProcessor.class);
    private static final Map<String, String> EMPRESA_NOME_MAP;

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.DEBUG);
        logger.info(">>> INICIALIZANDO EMPRESA_NOME_MAP a partir de JSON...");
        Map<String, String> tempMap = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        String resourcePath = "/empresa_nome_map.json";
        InputStream jsonStream = null;
        try {
            jsonStream = QuestionProcessor.class.getResourceAsStream(resourcePath);
            if (jsonStream == null) { logger.error("!!! CRÍTICO: Arquivo '{}' não encontrado! Mapa vazio.", resourcePath); }
            else {
                tempMap = mapper.readValue(jsonStream, new TypeReference<Map<String, String>>() {});
                logger.info(">>> EMPRESA_NOME_MAP carregado de '{}'. {} mapeamentos.", resourcePath, tempMap.size());
                if (!tempMap.isEmpty()) {
                    logger.info(">>> DIAGNÓSTICO MAPA NOMES CARREGADO (amostra):");
                    logger.info(">>> Chave 'ITAU': {}", tempMap.get("ITAU"));
                    logger.info(">>> Chave 'TAURUSARMAS': {}", tempMap.get("TAURUSARMAS"));
                    logger.info(">>> Chave 'TENDA': {}", tempMap.get("TENDA"));
                    logger.info(">>> Tamanho total: {}", tempMap.size());
                } else { logger.error("!!! CRÍTICO: tempMap (Nomes) está VAZIO !!!"); }
            }
        } catch (IOException e) { logger.error("!!! CRÍTICO: Falha ler/parsear '{}'! Mapa vazio. Erro: {}", resourcePath, e.getMessage(), e); }
        finally { if (jsonStream != null) { try { jsonStream.close(); } catch (IOException e) { /* Ignora */ } } }
        EMPRESA_NOME_MAP = Collections.unmodifiableMap(tempMap);
    }

    private final Ontology ontology;
    private static final String BASE_URI = "https://dcm.ffclrp.usp.br/lssb/stock-market-ontology#";
    private static final DateTimeFormatter DATE_FORMATTER_INPUT_SLASH = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final DateTimeFormatter ISO_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    @Autowired
    public QuestionProcessor(Ontology ontology) {
        logger.warn(">>>> CONSTRUTOR QuestionProcessor (Mapa Nomes JSON - Busca por Nome) INSTANCE HASH: {}", this.hashCode());
        this.ontology = ontology;
        if (EMPRESA_NOME_MAP.isEmpty()) { logger.error(">>>> ATENÇÃO: EMPRESA_NOME_MAP VAZIO! <<<<"); }
        else { logger.info("QuestionProcessor inicializado com {} mapeamentos nome.", EMPRESA_NOME_MAP.size()); }
    }

    public ResponseEntity<Map<String, Object>> processQuestion(Map<String, String> request) {
        String pergunta = request.get("pergunta");
        if (isEmpty(pergunta)) { return createErrorResponse("Pergunta vazia.", HttpStatus.BAD_REQUEST); }
        logger.info("Serviço: Processando pergunta: '{}'", pergunta);
        Map<String, Object> resultPython; String nomeTemplateCurto; Map<String, Object> mapeamentos;
        try {
            URL scriptUrl = getClass().getResource("/pln_processor.py");
            if (scriptUrl == null) { logger.error("Script PLN não encontrado."); return createErrorResponse("Erro interno: PLN.", HttpStatus.INTERNAL_SERVER_ERROR); }
            String scriptPath = getScriptPathFromUrl(scriptUrl);
            if (scriptPath == null) { return createErrorResponse("Erro interno: Caminho PLN.", HttpStatus.INTERNAL_SERVER_ERROR); }
            logger.info("Executando: python \"{}\" \"{}\"", scriptPath, pergunta);
            ProcessBuilder pb = new ProcessBuilder("python", scriptPath, pergunta);
            File scriptFile = new File(scriptPath);
            if (scriptFile.getParentFile() != null) { pb.directory(scriptFile.getParentFile()); } else { logger.warn("Não setou dir Python.");}
            Process process = pb.start();
            String outputString; String errorString; StringBuilder stdOut = new StringBuilder(); StringBuilder stdErr = new StringBuilder();
            Thread outThread = new Thread(() -> readStreamPython(process.getInputStream(), stdOut, "pln_stdout")); Thread errThread = new Thread(() -> readStreamPython(process.getErrorStream(), stdErr, "pln_stderr"));
            outThread.start(); errThread.start();
            boolean exited = process.waitFor(60, TimeUnit.SECONDS);
            outThread.join(5000); errThread.join(5000);
            if (!exited) { process.destroyForcibly(); logger.error("Python PLN timeout."); return createErrorResponse("Processamento demorado.", HttpStatus.REQUEST_TIMEOUT); }
            int exitCode = process.exitValue(); outputString = stdOut.toString().trim(); errorString = stdErr.toString().trim();
            logPythonOutput(exitCode, outputString, errorString);
            if (exitCode != 0) {
                String errDetails = buildErrorDetails(exitCode, errorString, outputString); logger.error("Python PLN falhou. EC:{}. Det: {}", exitCode, errDetails);
                if (outputString.startsWith("{")) { try { ObjectMapper m = new ObjectMapper(); Map<String,Object> e = m.readValue(outputString, new TypeReference<>() {}); if(e.containsKey("erro")) return createErrorResponse("Erro PL: " + e.get("erro"), HttpStatus.INTERNAL_SERVER_ERROR); } catch (IOException ex) {/*ignora*/} }
                return createErrorResponse("Falha PL: " + errDetails, HttpStatus.INTERNAL_SERVER_ERROR);
            }
            if (!outputString.startsWith("{") || !outputString.endsWith("}")) { logger.error("Saída Python não JSON:\n{}", outputString); return createErrorResponse("Erro comunicação (formato).", HttpStatus.INTERNAL_SERVER_ERROR); }
            ObjectMapper mapper = new ObjectMapper();
            try { resultPython = mapper.readValue(outputString, new TypeReference<Map<String, Object>>() {}); } catch (IOException e) { logger.error("Falha parse JSON:\n{}", outputString, e); return createErrorResponse("Erro comunicação (parse).", HttpStatus.INTERNAL_SERVER_ERROR); }
            nomeTemplateCurto = (String) resultPython.get("template_nome"); Object mapObj = resultPython.get("mapeamentos");
            if (mapObj instanceof Map) { @SuppressWarnings("unchecked") Map<String, Object> tempMap = (Map<String, Object>) mapObj; mapeamentos = tempMap; logger.debug("Mapeamentos recebidos: {}", mapeamentos); }
            else { logger.error("JSON 'mapeamentos' inválido."); return createErrorResponse("Erro resposta interna (map).", HttpStatus.INTERNAL_SERVER_ERROR); }
            if (isEmpty(nomeTemplateCurto) || mapeamentos == null) { logger.error("Faltando template/map."); return createErrorResponse("Erro resposta interna (dados).", HttpStatus.INTERNAL_SERVER_ERROR); }
        } catch (Exception e) { logger.error("Erro inesperado chamada Python.", e); return createErrorResponse("Erro comunicação externa.", HttpStatus.INTERNAL_SERVER_ERROR); }
        try {
            logger.info("Construindo consulta para template '{}'...", nomeTemplateCurto);
            String consultaSparql = buildSparqlFromSpecificTemplate(nomeTemplateCurto, mapeamentos);
            if (consultaSparql == null) { logger.error("Falha construir SPARQL '{}'.", nomeTemplateCurto); return createErrorResponse("Não foi possível montar consulta para '" + nomeTemplateCurto + "'.", HttpStatus.INTERNAL_SERVER_ERROR); }
            logger.info("SPARQL Gerada:\n---\n{}\n---", consultaSparql);
            Query query;
            try { query = QueryFactory.create(consultaSparql); logger.debug("Query criada."); } catch (QueryParseException e) { logger.error("Erro Parse SPARQL:\n{}\n---", consultaSparql, e); return createErrorResponse("Erro interno (sintaxe SPARQL inválida).", HttpStatus.INTERNAL_SERVER_ERROR); }
            String targetVariable = determineTargetVariable(query, mapeamentos);
            if (targetVariable == null || "ASK".equals(targetVariable)) { String msg = ("ASK".equals(targetVariable))?"ASK não suportado.":"Erro var alvo."; HttpStatus st = ("ASK".equals(targetVariable))?HttpStatus.BAD_REQUEST:HttpStatus.INTERNAL_SERVER_ERROR; logger.error(msg + " Alvo: {}", targetVariable); return createErrorResponse(msg, st); }
            logger.debug("Variável alvo: '{}'", targetVariable);
            logger.info(">>> EXECUTANDO QUERY com target:'{}' <<<", targetVariable);
            List<String> results = ontology.queryAndExtractList(query, targetVariable);
            logger.info(">>> QUERY EXECUTADA. Resultados: {}. <<<", (results != null ? results.size() : "NULO/ERRO"));
            Map<String, Object> finalResult = formatResults(results, targetVariable);
            logger.info("Resposta final serviço: {}", finalResult.get("resposta"));
            return new ResponseEntity<>(finalResult, HttpStatus.OK);
        } catch (Exception e) { logger.error("Erro GERAL SPARQL para:'{}'", pergunta, e); return createErrorResponse("Erro busca dados.", HttpStatus.INTERNAL_SERVER_ERROR); }
    }

    // --- Métodos Auxiliares ---

    private String getScriptPathFromUrl(URL scriptUrl) {
        try { return Paths.get(scriptUrl.toURI()).toString(); }
        catch (URISyntaxException | IllegalArgumentException | NullPointerException e) {
            logger.warn("Erro converter URL->URI: {}", (scriptUrl != null ? e.getMessage() : "URL nula"));
            if (scriptUrl == null) return null;
            try {
                String decodedPath = URLDecoder.decode(scriptUrl.getPath(), StandardCharsets.UTF_8.name());
                if (System.getProperty("os.name").toLowerCase().contains("win") && decodedPath.startsWith("/")) { decodedPath = decodedPath.substring(1); }
                if (Files.exists(Paths.get(decodedPath))) { logger.info("Usando fallback path: {}", decodedPath); return decodedPath; }
                else { logger.error("Fallback path não existe: {}", decodedPath); return null; }
            } catch (Exception fallbackEx) { logger.error("Falha fallback path.", fallbackEx); return null; }
        }
    }

    private void readStreamPython(InputStream stream, StringBuilder collector, String streamName) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line; while ((line = reader.readLine()) != null) { collector.append(line).append(System.lineSeparator()); }
        } catch (IOException e) { logger.error("Erro leitura stream '{}'", streamName, e); }
    }

    private void logPythonOutput(int exitCode, String outputString, String errorString) {
        logger.debug("Python stdout (EC:{}):\n---\n{}\n---", exitCode, outputString.isEmpty() ? "<vazio>" : outputString);
        if (!errorString.isEmpty()) { logger.error("Python stderr (EC:{}):\n---\n{}\n---", exitCode, errorString); }
        else { logger.debug("Python stderr (EC:{}): <vazio>", exitCode); }
    }

    private String buildErrorDetails(int exitCode, String errorString, String outputString) {
        StringBuilder details = new StringBuilder("EC:").append(exitCode).append(".");
        if (!errorString.isEmpty()) { String err = errorString.lines().filter(l->!l.trim().isEmpty()).limit(5).collect(Collectors.joining("|")); details.append(" Stderr:").append(err); }
        else if (exitCode != 0 && !outputString.isEmpty() && !outputString.startsWith("{")) { details.append(" Out(não JSON):").append(outputString.substring(0, Math.min(100,outputString.length()))).append("..."); }
        return details.toString();
    }

    private String loadResourceContent(String resourcePath) {
        logger.debug("Tentando carregar recurso: {}", resourcePath);
        try (InputStream inputStream = getClass().getResourceAsStream(resourcePath)) {
            if (inputStream == null) { logger.error("Recurso NÃO ENCONTRADO: {}!", resourcePath); return null; }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                String content = reader.lines().map(String::trim).filter(line -> !line.startsWith("#") && !line.isEmpty()).collect(Collectors.joining(System.lineSeparator()));
                if (isEmpty(content)) { logger.error("Conteúdo recurso '{}' vazio.", resourcePath); return null; }
                logger.debug("Conteúdo carregado de {}:\n{}", resourcePath, content);
                return content;
            }
        } catch (Exception e) { logger.error("Erro ao carregar/ler recurso '{}'", resourcePath, e); return null; }
    }

    private String buildSparqlFromSpecificTemplate(String templateNameFromPython, Map<String, Object> mapeamentos) {
        String resourceFileName = templateNameFromPython.replace(" ", "_") + ".txt";
        String resourcePath = "/" + resourceFileName;
        logger.info("Carregando template: {}", resourcePath);
        String templateContent = loadResourceContent(resourcePath);
        if (templateContent == null) return null;
        String consultaFinal = templateContent;
        logger.debug(">>> Aplicando Substituições Semânticas <<<");
        Set<String> placeholdersOriginais = new HashSet<>();
        Matcher matcher = Pattern.compile("(#\\w+#)").matcher(templateContent);
        while (matcher.find()) { placeholdersOriginais.add(matcher.group(1)); }
        logger.trace("Placeholders originais: {}", placeholdersOriginais);
        for (Map.Entry<String, Object> entry : mapeamentos.entrySet()) {
            String semanticKey = entry.getKey();
            Object valueObject = entry.getValue();
            String valueString = (valueObject != null) ? valueObject.toString() : null;
            String placeholderNoTemplate = semanticKey; // Usa chave do python com #..#
            if (semanticKey.equals("#VALOR_DESEJADO") || isEmpty(valueString)) { continue; }
            if (!consultaFinal.contains(placeholderNoTemplate)) {
                if (placeholdersOriginais.contains(placeholderNoTemplate)) { logger.warn("Placeholder '{}' já substituído ou ausente.", placeholderNoTemplate); }
                else { logger.trace("Placeholder '{}' não pertence ao template.", placeholderNoTemplate); }
                continue;
            }
            String valueToSubstitute = prepareSubstitutionValue(placeholderNoTemplate, semanticKey, valueString);
            if (valueToSubstitute == null) { logger.error("Falha gerar valor para '{}' ('{}'). Abortando.", placeholderNoTemplate, valueString); return null; }
            consultaFinal = consultaFinal.replace(placeholderNoTemplate, valueToSubstitute);
            logger.debug("Substituído '{}' por '{}'", placeholderNoTemplate, valueToSubstitute);
        }
        matcher = Pattern.compile("(#\\w+#)").matcher(consultaFinal);
        if (matcher.find()) { logger.error("ERRO: Consulta final contém placeholders! Ex: '{}'. Query:\n{}", matcher.group(1), consultaFinal); return null; }
        logger.info("Construção SPARQL concluída.");
        return consultaFinal;
    }

    private String prepareSubstitutionValue(String placeholderFound, String semanticKey, String valueString) {
        logger.trace("Preparando valor: placeholder='{}', chave='{}', valor='{}'", placeholderFound, semanticKey, valueString);
        if (valueString == null) { logger.error("Valor nulo para placeholder '{}'.", placeholderFound); return null; }
        switch (placeholderFound) {
            case "#DATA#":
                String dataISO = resolveDataToISODate(valueString);
                if (dataISO != null) return "\"" + dataISO + "\"^^<" + XSD.date.getURI() + ">";
                else { logger.error("Valor #DATA# inválido: '{}'", valueString); return null; }
            case "#ENTIDADE_NOME#":
                String escapedValue = valueString.replace("\"", "\\\"");
                return "\"" + escapedValue + "\"";
            case "#ENTIDADE#":
                String uriPart = resolveEmpresaByMap(valueString);
                if (uriPart != null) return "<" + BASE_URI + uriPart + ">";
                else { logger.warn("Valor #ENTIDADE# ('{}') não resolvido via mapa.", valueString); return null; }
            case "#TIPO_ACAO#": return "<" + BASE_URI + valueString.trim().toUpperCase() + ">";
            case "#SETOR#": String setorUriPart = createUriSafe(valueString); return "<" + BASE_URI + setorUriPart + ">";
            default:
                logger.warn("Placeholder não tratado: '{}'. Tratando como literal.", placeholderFound);
                String escapedDefault = valueString.replace("\"", "\\\"");
                return "\"" + escapedDefault + "\"";
        }
    }

    private String determineTargetVariable(@NonNull Query query, @NonNull Map<String, Object> mapeamentos) {
        logger.debug("Determinando variável alvo...");
        if (query.isAskType()) { logger.warn("Query ASK."); return "ASK"; }
        List<Var> projectVars = query.getProjectVars();
        if (projectVars == null || projectVars.isEmpty()) { logger.error("SELECT sem variáveis!"); return null; }
        Object vdObj = mapeamentos.get("#VALOR_DESEJADO");
        if (vdObj != null) {
            String vdKey = vdObj.toString().toLowerCase().trim(); logger.debug("Tentando alvo por #VD='{}'", vdKey);
            for (Var v : projectVars) {
                String varName = v.getVarName(); String vnl = varName.toLowerCase();
                if (vnl.equals(vdKey)) { logger.info("Alvo por #VD (exato): '{}'", varName); return varName; }
                if (vdKey.equals("codigo") && vnl.contains("ticker")) { logger.info("Alvo por #VD=codigo->ticker: '{}'", varName); return varName; }
                if (vdKey.equals("nome") && (vnl.contains("nome") || vnl.contains("label"))) { logger.info("Alvo por #VD=nome->nome/label: '{}'", varName); return varName; }
                // Adiciona verificação de sufixo SEM prefixo comum (ex: preco_fechamento vs fechamento)
                String vdSemPrefixo = vdKey.startsWith("preco_") ? vdKey.substring(6) : vdKey;
                if (vnl.endsWith(vdSemPrefixo)) { logger.info("Alvo por #VD (sufixo s/ prefixo): '{}'", varName); return varName; }
                if (vnl.endsWith(vdKey)) { logger.info("Alvo por #VD (sufixo direto): '{}'", varName); return varName; }

            } logger.warn("#VD='{}' não match var: {}", vdKey, projectVars);
        } else { logger.warn("#VALOR_DESEJADO não encontrado."); }
        String firstVar = projectVars.get(0).getName(); logger.warn("Fallback alvo: '{}'", firstVar); return firstVar;
    }

    private ResponseEntity<Map<String, Object>> createErrorResponse(String message, HttpStatus status) {
        return new ResponseEntity<>(Map.of("erro", message), status);
    }

    private Map<String, Object> formatResults(List<String> results, String targetVariable) {
        Map<String, Object> finalResult = new HashMap<>(); String resposta;
        if (results == null) { resposta = "Erro resultados."; logger.error("Resultados NULOS p/ '{}'.", targetVariable); }
        else if (results.isEmpty()) { resposta = "Não foram encontrados resultados."; }
        else if (results.size() == 1) { resposta = Optional.ofNullable(results.get(0)).orElse("(Nulo)"); }
        else {
            // Se múltiplos resultados para ticker, junta com vírgula
            if(targetVariable.toLowerCase().contains("ticker")) {
                resposta = results.stream().filter(Objects::nonNull).collect(Collectors.joining(", "));
                if (results.stream().anyMatch(Objects::isNull)) {
                    resposta += " (e valor(es) nulo(s))";
                }
                logger.debug("Juntando múltiplos tickers: {}", resposta);
            } else { // Senão, formata como lista
                StringBuilder sb = new StringBuilder("Resultados (" + results.size() + "):\n");
                results.stream().limit(20).forEach(i -> sb.append("- ").append(Optional.ofNullable(i).orElse("(nulo)")).append("\n"));
                if (results.size() > 20) sb.append("... (+").append(results.size() - 20).append(")");
                resposta = sb.toString().trim();
            }
        }
        finalResult.put("resposta", resposta);
        return finalResult;
    }

    private String resolveDataToISODate(String dataInput) {
        if (isEmpty(dataInput)) return null; String d = dataInput.trim(); LocalDate pDate = null;
        try { pDate = LocalDate.parse(d, ISO_DATE_FORMATTER); } catch (DateTimeParseException e0) {
            try { pDate = LocalDate.parse(d, DATE_FORMATTER_INPUT_SLASH); } catch (DateTimeParseException e1) {
                if (d.matches("^\\d{8}$")) { try { pDate = LocalDate.parse(d, DateTimeFormatter.BASIC_ISO_DATE); } catch (DateTimeParseException e3) {} }
                if (pDate == null) { String l = d.toLowerCase(); if(l.contains("hoje")||l.contains("hj")) pDate = LocalDate.now(); else if(l.contains("ontem")) pDate = LocalDate.now().minusDays(1); else {logger.warn("Data '{}' não reconhecida.", d); return null;} } } }
        if (pDate != null) { String fmt = pDate.format(ISO_DATE_FORMATTER); logger.debug("Data '{}' -> ISO '{}'", d, fmt); return fmt; } return null;
    }

    private String resolveEmpresaByMap(String empresaInput) {
        if(isEmpty(empresaInput)) return null;
        String keyUpper = empresaInput.trim().toUpperCase();
        String mappedValue = EMPRESA_NOME_MAP.get(keyUpper);
        if (mappedValue == null) {
            String normalizedInput = normalizeJava(keyUpper);
            if (normalizedInput != null) { mappedValue = EMPRESA_NOME_MAP.get(normalizedInput); }
        }
        // Retorna o VALOR do mapa (que agora é o nome canônico ou ticker combinado)
        // Se o placeholder for #ENTIDADE#, ele espera a parte do URI (ticker combinado)
        // Se o placeholder for #ENTIDADE_NOME#, ele espera o nome canônico.
        // A lógica atual retorna o valor do mapa para ambos. PRECISA AJUSTAR prepareSubstitutionValue.
        // --> Ajustado em prepareSubstitutionValue: #ENTIDADE# busca no mapa e usa o valor como URI part.
        // -->                                      #ENTIDADE_NOME# usa o valor do python diretamente como literal.
        return mappedValue;
    }

    private String normalizeJava(String text) {
        if (text == null) return null; String n = text.toUpperCase().trim();
        try { n = Normalizer.normalize(n, Normalizer.Form.NFD).replaceAll("\\p{M}", ""); }
        catch (Exception e) { logger.warn("Falha norm acentos Java: '{}'", text, e); }
        n = n.replaceAll("\\b(S\\.?A\\.?|S/?A|CIA\\.?|COMPANHIA|LTDA\\.?|ON|PN|N[12]|PREF\\.?|ORD\\.?|NM|ED|EJ|MA)\\b", "");
        n = n.replaceAll("[^\\p{Alnum}]+", "");
        return n.isEmpty() ? null : n;
    }

    private String createUriSafe(String input) {
        if (isEmpty(input)) return "id_" + UUID.randomUUID().toString().substring(0, 8);
        String normalized = Normalizer.normalize(input.trim(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
        String sanitized = normalized.replaceAll("[^a-zA-Z0-9_\\-\\.]+", "_").replaceAll("\\s+", "_").replaceAll("_+", "_").replaceAll("^_|_$", "");
        if (sanitized.isEmpty()) return "gen_" + UUID.randomUUID().toString().substring(0, 8);
        if (!sanitized.isEmpty() && !Character.isLetter(sanitized.charAt(0)) && sanitized.charAt(0) != '_') { sanitized = "id_" + sanitized; }
        return sanitized;
    }

    private boolean isEmpty(String s){ return s == null || s.trim().isEmpty(); }

} // Fim da classe QuestionProcessor