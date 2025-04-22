package com.example.Programa_heber.service;

// --- IMPORTS NECESSÁRIOS ---
import com.example.Programa_heber.ontology.StockMarketOntology;
// Imports do Apache Jena
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.vocabulary.XSD;
// Imports do Spring Framework
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
// Imports de Logging (SLF4J e Logback)
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
// Imports do Java IO e NIO
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
// Imports do Java Net
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
// Imports do Java Text
import java.text.Normalizer;
// Imports do Java Time
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
// Imports do Java Util
import java.util.*; // Importa todas as classes util, incluindo List, Map, HashMap, etc.
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
// Imports do Jackson Databind
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
// --- FIM DOS IMPORTS ---


@RestController
@Service
public class QuestionProcessor {

    private static final Logger logger = LoggerFactory.getLogger(QuestionProcessor.class);

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.DEBUG);
    }

    private final StockMarketOntology ontology;

    private static final String BASE_URI = "https://dcm.ffclrp.usp.br/lssb/stock-market-ontology#";
    private static final DateTimeFormatter DATE_FORMATTER_INPUT_SLASH = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final DateTimeFormatter ISO_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    // --- MAPA DE EMPRESAS CORRIGIDO ---
    private static final Map<String, String> EMPRESA_URI_MAP = new HashMap<>();
    static {
        EMPRESA_URI_MAP.put("CSN", "CMIN3");
        EMPRESA_URI_MAP.put("CSNMINERACAO", "CMIN3");
        EMPRESA_URI_MAP.put("CMIN3", "CMIN3");
        EMPRESA_URI_MAP.put("CSNA3", "CSNA3");
        //EMPRESA_URI_MAP.put("GERDAU", "GGBR3GGBR4"); // Mapeia nome para URI combinada
        //EMPRESA_URI_MAP.put("GGBR3GGBR4", "GGBR3GGBR4"); // Mapeia URI part para ela mesma
        // Remover mapeamentos individuais se não forem entidades separadas no grafo para outras queries
        // EMPRESA_URI_MAP.remove("GGBR3");
        // EMPRESA_URI_MAP.remove("GGBR4");
        EMPRESA_URI_MAP.put("VALE", "VALE3");
        EMPRESA_URI_MAP.put("VALE3", "VALE3");
        EMPRESA_URI_MAP.put("CBAV", "CBAV3");
        EMPRESA_URI_MAP.put("CBAV3", "CBAV3");
        EMPRESA_URI_MAP.put("ITAU", "ITUB4"); // Exemplo usando ITUB4
        EMPRESA_URI_MAP.put("ITAUUNIBANCO", "ITUB4");
        EMPRESA_URI_MAP.put("ITUB3", "ITUB3");
        EMPRESA_URI_MAP.put("ITUB4", "ITUB4");
    }
    // --- FIM DO MAPA DE EMPRESAS ---

    @Autowired
    public QuestionProcessor(StockMarketOntology ontology) {
        logger.warn(">>>> CONSTRUTOR QuestionProcessor Final (v8 - Ticker Combinado Formatado) INSTANCE HASH: {}", this.hashCode());
        this.ontology = ontology;
        logger.info("QP Final (v8) inicializado.");
    }

    @PostMapping("/processarPergunta")
    public ResponseEntity<Map<String, Object>> processQuestion(@RequestBody Map<String, String> request) {
        String pergunta = request.get("pergunta");
        if (isEmpty(pergunta)) { return createErrorResponse("Pergunta vazia.", HttpStatus.BAD_REQUEST); }
        logger.info("Recebida pergunta: '{}'", pergunta);

        Map<String, Object> resultPython;
        String nomeTemplateCurto;
        Map<String, Object> mapeamentos;

        try {
            // --- Chamada Python ---
            URL scriptUrl = getClass().getResource("/pln_processor.py");
            if (scriptUrl == null) { logger.error("!!! Script 'pln_processor.py' NÃO encontrado !!!"); return createErrorResponse("Erro interno: Script PLN.", HttpStatus.INTERNAL_SERVER_ERROR); }
            String scriptPath = getScriptPathFromUrl(scriptUrl);
            if (scriptPath == null) { return createErrorResponse("Erro interno: Caminho script PLN.", HttpStatus.INTERNAL_SERVER_ERROR); }

            logger.info("Executando: python \"{}\" \"{}\"", scriptPath, pergunta);
            ProcessBuilder pb = new ProcessBuilder("python", scriptPath, pergunta);

            File scriptFile = new File(scriptPath);
            if (scriptFile.getParentFile() != null && scriptFile.getParentFile().isDirectory()) { pb.directory(scriptFile.getParentFile()); logger.info("Dir Python: {}", pb.directory()); }
            else { logger.warn("Não setou dir Python."); }

            Process process;
            try { process = pb.start(); }
            catch (IOException startException) { logger.error("Falha iniciar Python.", startException); return createErrorResponse("Erro ao iniciar processamento externo.", HttpStatus.INTERNAL_SERVER_ERROR); }

            // --- Leitura stdout/stderr ---
            String outputString; String errorString;
            StringBuilder stdOutCollector = new StringBuilder(); StringBuilder stdErrCollector = new StringBuilder();
            Thread stdOutThread = new Thread(() -> readStream(process.getInputStream(), stdOutCollector, "stdout"));
            Thread stdErrThread = new Thread(() -> readStream(process.getErrorStream(), stdErrCollector, "stderr"));
            stdOutThread.start(); stdErrThread.start();
            boolean exited = process.waitFor(60, TimeUnit.SECONDS);
            stdOutThread.join(5000); stdErrThread.join(5000);
            if (!exited) { process.destroyForcibly(); logger.error("Python timeout."); return createErrorResponse("Processamento demorou.", HttpStatus.INTERNAL_SERVER_ERROR); }
            int exitCode = process.exitValue(); outputString = stdOutCollector.toString().trim(); errorString = stdErrCollector.toString().trim();
            logPythonOutput(exitCode, outputString, errorString);

            // --- Verificação Erros e Parse JSON ---
            if (exitCode != 0 || (!outputString.startsWith("{") || !outputString.endsWith("}"))) {
                String errorDetails = buildErrorDetails(exitCode, errorString); logger.error("Falha Python. EC:{}. Detalhes: {}", exitCode, errorDetails);
                return createErrorResponse("Falha processamento linguagem: " + errorDetails, HttpStatus.INTERNAL_SERVER_ERROR);
            }
            ObjectMapper mapper = new ObjectMapper();
            try { resultPython = mapper.readValue(outputString, new TypeReference<Map<String, Object>>() {}); }
            catch (IOException e) { logger.error("Falha parse JSON:\n{}", outputString, e); return createErrorResponse("Erro comunicação interna (JSON).", HttpStatus.INTERNAL_SERVER_ERROR); }
            if (resultPython.containsKey("erro")) {
                logger.error("Erro explícito Python: {}", resultPython.get("erro")); String detalhes = resultPython.getOrDefault("detalhes", "").toString();
                return createErrorResponse("Erro processamento linguagem: " + resultPython.get("erro") + (!detalhes.isEmpty() ? " Detalhes: " + detalhes : ""), HttpStatus.INTERNAL_SERVER_ERROR);
            }

            // --- Extração Dados ---
            nomeTemplateCurto = (String) resultPython.get("template_nome");
            Object mapeamentosObj = resultPython.get("mapeamentos");
            if (mapeamentosObj instanceof Map) {
                @SuppressWarnings("unchecked") Map<String, Object> tempMapeamentos = (Map<String, Object>) mapeamentosObj;
                mapeamentos = tempMapeamentos; logger.debug("Mapeamentos: {}", mapeamentos);
            } else { logger.error("JSON 'mapeamentos' inválido."); return createErrorResponse("Erro resposta interna (mapeamentos).", HttpStatus.INTERNAL_SERVER_ERROR); }
            if (isEmpty(nomeTemplateCurto) || mapeamentos == null) { logger.error("Faltando template/mapeamentos."); return createErrorResponse("Erro resposta interna (dados).", HttpStatus.INTERNAL_SERVER_ERROR); }

        } catch (InterruptedException e) { Thread.currentThread().interrupt(); logger.error("Thread interrompida.", e); return createErrorResponse("Processamento interrompido.", HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) { logger.error("Erro inesperado chamada Python.", e); return createErrorResponse("Erro inesperado comunicação externa.", HttpStatus.INTERNAL_SERVER_ERROR); }

        // --- Geração SPARQL e Execução ---
        try {
            logger.info("Construindo consulta para template '{}'...", nomeTemplateCurto);
            String consultaSparql = buildSparqlFromSpecificTemplate(nomeTemplateCurto, mapeamentos);
            if (consultaSparql == null) { return createErrorResponse("Não foi possível montar consulta para '" + nomeTemplateCurto + "'.", HttpStatus.INTERNAL_SERVER_ERROR); }
            logger.info("SPARQL Gerada:\n{}", consultaSparql);

            Query query;
            try { query = QueryFactory.create(consultaSparql); logger.debug("Query criada."); }
            catch (QueryParseException e) { logger.error("Erro Parse SPARQL:\n{}", consultaSparql, e); return createErrorResponse("Erro interno (sintaxe SPARQL).", HttpStatus.INTERNAL_SERVER_ERROR); }

            String targetVariable = determineTargetVariable(query, mapeamentos); // Passa mapeamentos
            if (targetVariable == null || "ASK".equals(targetVariable)) {
                String errorMsg = ("ASK".equals(targetVariable)) ? "Perguntas Sim/Não não suportadas." : "Erro interno (variável alvo).";
                HttpStatus status = ("ASK".equals(targetVariable)) ? HttpStatus.BAD_REQUEST : HttpStatus.INTERNAL_SERVER_ERROR;
                logger.error(errorMsg + " Alvo: {}", targetVariable); return createErrorResponse(errorMsg, status);
            }
            logger.debug("Variável alvo: '{}'", targetVariable);

            logger.info(">>> EXECUTANDO QUERY com target:'{}' <<<", targetVariable);
            List<String> results = ontology.queryAndExtractList(query, targetVariable);
            logger.info(">>> QUERY EXECUTADA. Resultados: {}. <<<", (results != null ? results.size() : "NULO"));

            // Passa a variável alvo para formatResults para tratamento especial de tickers
            Map<String, Object> finalResult = formatResults(results, targetVariable);
            logger.info("Resposta final: {}", finalResult.get("resposta"));
            return new ResponseEntity<>(finalResult, HttpStatus.OK);

        } catch (Exception e) {
            logger.error("Erro GERAL durante geração/execução SPARQL para:'{}'", pergunta, e);
            return createErrorResponse("Erro inesperado busca dados.", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    // --- Métodos Auxiliares ---

    private String getScriptPathFromUrl(URL scriptUrl) {
        try { return Paths.get(scriptUrl.toURI()).toString(); }
        catch (URISyntaxException | IllegalArgumentException e) {
            logger.warn("Erro converter URL->URI (fallback): {}", e.getMessage());
            try {
                String decodedPath = URLDecoder.decode(scriptUrl.getPath(), StandardCharsets.UTF_8.name());
                if (System.getProperty("os.name").toLowerCase().contains("win") && decodedPath.startsWith("/")) { decodedPath = decodedPath.substring(1); }
                if (new File(decodedPath).exists()){ return decodedPath; } else { logger.error("Fallback path script não existe: {}", decodedPath); return null; }
            } catch (Exception fallbackEx) { logger.error("Falha fallback path script.", fallbackEx); return null; }
        }
    }

    private void readStream(InputStream stream, StringBuilder collector, String streamName) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line; while ((line = reader.readLine()) != null) { collector.append(line).append(System.lineSeparator()); }
        } catch (IOException e) { logger.error("Erro leitura stream '{}'", streamName, e); }
    }

    private void logPythonOutput(int exitCode, String outputString, String errorString) {
        logger.debug("Python stdout (EC:{}):\n---\n{}\n---", exitCode, outputString.isEmpty() ? "<vazio>" : outputString);
        if (!errorString.isEmpty()) { logger.error("Python stderr (EC:{}):\n---\n{}\n---", exitCode, errorString); }
        else { logger.debug("Python stderr (EC:{}): <vazio>", exitCode); }
    }

    private String buildErrorDetails(int exitCode, String errorString) {
        String details = "EC:" + exitCode + ".";
        if (!errorString.isEmpty()) {
            String relevantError = errorString.lines().filter(l -> !l.trim().isEmpty()).limit(5).collect(Collectors.joining(" | "));
            details += " Stderr: " + relevantError;
        } return details;
    }

    private String buildSparqlFromSpecificTemplate(String templateNameFromPython, Map<String, Object> mapeamentos) {
        String resourceFileName = templateNameFromPython.replace(" ", "_") + ".txt";
        String resourcePath = "/" + resourceFileName;
        logger.info("Carregando template: {}", resourcePath);
        String templateContent = loadResourceContent(resourcePath);
        if (templateContent == null) return null;

        String consultaFinal = templateContent;
        logger.debug(">>> Aplicando Substituições Semânticas <<<");
        for (Map.Entry<String, Object> entry : mapeamentos.entrySet()) {
            String semanticKey = entry.getKey();
            Object valueObject = entry.getValue();
            String valueString = (valueObject != null) ? valueObject.toString() : null;

            String placeholderInTemplate = null;
            if (semanticKey.equals("#DATA")) {
                if (templateContent.contains("#DATA_URI#")) placeholderInTemplate = "#DATA_URI#";
                else if (templateContent.contains("#DATA#")) placeholderInTemplate = "#DATA#";
                else logger.warn("Placeholder para #DATA não encontrado no template.");
            } else {
                placeholderInTemplate = semanticKey + "#";
                if (!templateContent.contains(placeholderInTemplate)) { logger.warn("Placeholder direto '{}' não encontrado.", placeholderInTemplate); placeholderInTemplate = null; }
            }

            if (placeholderInTemplate == null || semanticKey.equals("#VALOR_DESEJADO") || isEmpty(valueString)) { continue; }

            String valueToSubstitute = prepareSubstitutionValue(placeholderInTemplate, semanticKey, valueString);

            if (valueToSubstitute != null) {
                consultaFinal = consultaFinal.replace(placeholderInTemplate, valueToSubstitute);
                logger.debug("Substituído '{}' por '{}'", placeholderInTemplate, valueToSubstitute);
            } else { logger.warn("Não gerado valor para '{}' ('{}').", semanticKey, valueString); }
        }

        if (consultaFinal.matches(".*#\\w+#.*")) { logger.error("ERRO: Consulta final contém placeholders! Query:\n{}", consultaFinal); return null; }
        logger.info("SPARQL Final:\n{}", consultaFinal);
        return consultaFinal;
    }

    private String loadResourceContent(String resourcePath) {
        try (InputStream inputStream = getClass().getResourceAsStream(resourcePath)) {
            if (inputStream == null) { logger.error("!!! Recurso NÃO ENCONTRADO: {} !!!", resourcePath); return null; }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                String content = reader.lines().filter(line -> !line.trim().startsWith("#")).collect(Collectors.joining(System.lineSeparator()));
                if (isEmpty(content)) { logger.error("Conteúdo do recurso '{}' vazio.", resourcePath); return null; }
                logger.debug("Conteúdo carregado de {}:\n{}", resourcePath, content);
                return content;
            }
        } catch (IOException e) { logger.error("Erro IO ao ler '{}'", resourcePath, e); return null;
        } catch (Exception e) { logger.error("Erro inesperado ao carregar '{}'", resourcePath, e); return null; }
    }

    private String prepareSubstitutionValue(String placeholderFound, String semanticKey, String valueString) {
        logger.trace("Preparando valor para placeholder '{}' (chave '{}', valor '{}')", placeholderFound, semanticKey, valueString);
        if ("#DATA_URI#".equals(placeholderFound)) {
            String dataISO = resolveDataToISODate(valueString);
            if (dataISO != null) { String dataUriPart = dataISO.replace("-", ""); return "<" + BASE_URI + dataUriPart + ">"; } else { return null; }
        } else if ("#DATA#".equals(placeholderFound)) {
            String dataISO = resolveDataToISODate(valueString);
            if (dataISO != null) { return "\"" + dataISO + "\"^^<" + XSD.date.getURI() + ">"; } else { return null; }
        } else if ("#ENTIDADE#".equals(placeholderFound)) {
            String uriPart = resolveEmpresa(valueString);
            if (uriPart != null) { return "<" + BASE_URI + uriPart + ">"; } else { return null; }
        } else if ("#TIPO_ACAO#".equals(placeholderFound)) {
            return "<" + BASE_URI + valueString.trim().toUpperCase() + ">";
        } else if ("#SETOR#".equals(placeholderFound)) {
            String setorUriPart = createUriSafe(valueString); return "<" + BASE_URI + setorUriPart + ">";
        } else { logger.warn("Placeholder não tratado: '{}'", placeholderFound); return null; }
    }

    // --- determineTargetVariable CORRIGIDO ---
    private String determineTargetVariable(@NonNull Query query, @NonNull Map<String, Object> mapeamentos) {
        logger.debug("Determinando variável alvo...");
        if (query.isAskType()) { logger.warn("Query ASK não suportada."); return "ASK"; }
        Object valorDesejadoObj = mapeamentos.get("#VALOR_DESEJADO");
        if (valorDesejadoObj != null) {
            String valorDesejadoKey = valorDesejadoObj.toString().toLowerCase();
            for (Var v : query.getProjectVars()) {
                String varNameLower = v.getVarName().toLowerCase();
                if (varNameLower.equals(valorDesejadoKey)) { logger.info("Alvo por #VD (exato): '{}'", v.getName()); return v.getName(); }
                // Match especial para 'codigo' -> variável contendo 'ticker'
                if (valorDesejadoKey.equals("codigo") && varNameLower.contains("ticker")) { logger.info("Alvo por #VD=codigo -> ticker: '{}'", v.getName()); return v.getName(); }
                String varNameSemPrefixo = varNameLower.replaceFirst("^preco", ""); String vdSemPrefixo = valorDesejadoKey.replaceFirst("^preco", "");
                if (!varNameSemPrefixo.equals(varNameLower) && varNameSemPrefixo.equals(vdSemPrefixo)) { logger.info("Alvo por #VD (sem prefixo): '{}'", v.getName()); return v.getName(); }
            } logger.warn("#VD='{}' não correspondeu a var em {}", valorDesejadoKey, query.getProjectVars());
        } else { logger.warn("#VALOR_DESEJADO não encontrado."); }
        List<Var> projectVars = query.getProjectVars();
        if (projectVars != null && !projectVars.isEmpty()) { String firstVarName = projectVars.get(0).getName(); logger.warn("Usando fallback (1a SELECT): '{}'", firstVarName); return firstVarName; }
        else { logger.error("Nenhuma variável no SELECT (não ASK)! Query:\n{}", query); return null; }
    }
    // --- FIM DA CORREÇÃO ---

    private ResponseEntity<Map<String, Object>> createErrorResponse(String message, HttpStatus status) {
        Map<String, Object> error = new HashMap<>(); error.put("erro", message); return new ResponseEntity<>(error, status);
    }

    // --- formatResults CORRIGIDO para separar tickers ---
    private Map<String, Object> formatResults(List<String> results, String targetVariable) {
        Map<String, Object> finalResult = new HashMap<>();
        String respostaFormatada;
        if (results == null) { respostaFormatada = "Erro resultados."; logger.error("Resultados NULOS.");}
        else if (results.isEmpty()) { respostaFormatada = "Não foram encontrados resultados."; }
        else if (results.size() == 1) {
            String result = results.get(0);
            // Se a variável alvo contiver 'ticker' (case-insensitive) e o resultado contiver vírgula
            if (result != null && targetVariable != null && targetVariable.toLowerCase().contains("ticker") && result.contains(",")) {
                List<String> tickers = Arrays.asList(result.split(",")); // Separa pela vírgula
                if (tickers.size() > 1) {
                    respostaFormatada = "Códigos encontrados: " + tickers.stream().map(String::trim).collect(Collectors.joining(", ")); // Formata como lista
                } else { respostaFormatada = result; } // Não conseguiu separar ou só tinha um
            } else {
                respostaFormatada = result != null ? result : "(Resultado Nulo)"; // Caso normal
            }
        } else { // Múltiplos resultados
            StringBuilder sb = new StringBuilder("Resultados (" + results.size() + "):\n");
            results.stream().limit(20).forEach(item -> sb.append("- ").append(item != null ? item : "(nulo)").append("\n"));
            if (results.size() > 20) sb.append("... (+").append(results.size() - 20).append(")");
            respostaFormatada = sb.toString().trim();
        }
        finalResult.put("resposta", respostaFormatada);
        return finalResult;
    }
    // --- FIM DA CORREÇÃO ---

    private String resolveDataToISODate(String dataInput) {
        if (isEmpty(dataInput)) return null; String trimmedData = dataInput.trim(); LocalDate parsedDate = null;
        try { parsedDate = LocalDate.parse(trimmedData, ISO_DATE_FORMATTER); }
        catch (DateTimeParseException e0) {
            try { parsedDate = LocalDate.parse(trimmedData, DATE_FORMATTER_INPUT_SLASH); }
            catch (DateTimeParseException e1) {
                String lowerData = trimmedData.toLowerCase();
                if (lowerData.contains("hoje")) parsedDate = LocalDate.now();
                else if (lowerData.contains("ontem")) parsedDate = LocalDate.now().minusDays(1);
                else { logger.warn("Data não reconhecida: '{}'", dataInput); return null; }
            }
        }
        if (parsedDate != null) { String fmt = parsedDate.format(ISO_DATE_FORMATTER); logger.debug("Data '{}' -> ISO '{}'", dataInput, fmt); return fmt; }
        return null;
    }

    // --- resolveEmpresa CORRIGIDO ---
    private String resolveEmpresa(String empresaInput) {
        if(isEmpty(empresaInput)) return null;
        String keyNormalizada = empresaInput.toUpperCase().replace(".", "").trim();
        String uriPart = EMPRESA_URI_MAP.get(keyNormalizada); // Usa o mapa ajustado
        if(uriPart != null){ logger.debug("Empresa '{}' mapeada -> '{}'", empresaInput, uriPart); return uriPart; }
        else { String fallbackUriPart = createUriSafe(keyNormalizada); logger.warn("Empresa '{}' não mapeada. Usando fallback: '{}'", empresaInput, fallbackUriPart); return fallbackUriPart; }
    }
    // --- FIM DA CORREÇÃO ---

    private String createUriSafe(String input) {
        if (isEmpty(input)) return "recurso_" + UUID.randomUUID().toString().substring(0,8);
        String normalized = Normalizer.normalize(input.trim(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
        String sanitized = normalized.replaceAll("[^a-zA-Z0-9_\\-\\.]+", "_").replaceAll("\\s+", "_").replaceAll("_+", "_").replaceAll("^_|_$", "");
        if (sanitized.isEmpty()) return "recurso_vazio_" + UUID.randomUUID().toString().substring(0,8);
        if (!Character.isLetter(sanitized.charAt(0)) && !sanitized.startsWith("_")) sanitized = "id_" + sanitized;
        return sanitized;
    }
    private boolean isEmpty(String s){ return s == null || s.trim().isEmpty(); }

} // Fim da classe