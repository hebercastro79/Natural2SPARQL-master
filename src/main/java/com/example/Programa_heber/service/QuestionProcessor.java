package com.example.Programa_heber.service;

import com.example.Programa_heber.ontology.Ontology;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.QueryParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class QuestionProcessor {

    private static final Logger logger = LoggerFactory.getLogger(QuestionProcessor.class);

    private final Ontology ontology;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final long PYTHON_SCRIPT_TIMEOUT = 30;

    private static final Map<String, String> placeholderToVariableMap = Map.of(
            "preco_fechamento", "precoFechamento",
            "preco_abertura", "precoAbertura",
            "preco_maximo", "precoMaximo",
            "preco_minimo", "precoMinimo",
            "volume", "volumeNegociacao",
            "ticker", "ticker_literal",
            "codigo", "ticker_literal"
    );

    @Autowired
    public QuestionProcessor(Ontology ontology) {
        this.ontology = ontology;
        logger.info("QuestionProcessor inicializado. Instância de Ontology injetada: {}", (ontology != null ? "OK" : "FALHOU!"));
    }

    public String processQuestion(String question) {
        logger.info("Serviço: Processando pergunta: '{}'", question);
        Map<String, Object> pythonResult = null;
        Map<String, Object> mappings = null;
        String templateName = null;

        try {
            pythonResult = executePythonScript(question);

            if (pythonResult == null) { return "Erro: Falha na execução/comunicação com o script Python."; }
            logger.debug("Resultado COMPLETO recebido do Python: {}", pythonResult);

            if (pythonResult.containsKey("erro")) {
                String pyError = (String) pythonResult.getOrDefault("erro", "Erro script Python.");
                String pyDetails = (String) pythonResult.get("detalhes");
                logger.error("Script Python retornou erro: {} (Detalhes: {})", pyError, pyDetails);
                return "Erro processador linguagem: " + pyError + (pyDetails != null ? " (" + pyDetails + ")" : "");
            }

            templateName = (String) pythonResult.get("template_nome");
            Object mappingsObj = pythonResult.get("mapeamentos");

            if (templateName == null || !(mappingsObj instanceof Map)) { logger.error("Chave 'template_nome'/'mapeamentos' ausente/inválida: {}", pythonResult); return "Erro: Formato inesperado script Python."; }

            @SuppressWarnings("unchecked") Map<String, Object> castedMappings = (Map<String, Object>) mappingsObj;
            mappings = castedMappings;

            logger.info(">>> DEBUG: Mapeamentos recebidos: {}", mappings);
            logger.info(">>> DEBUG: Chaves: {}", mappings.keySet());
            logger.info(">>> DEBUG: Contém '#DATA#'? {}", mappings.containsKey("#DATA#"));
            if (mappings.containsKey("#DATA#")) logger.info(">>> DEBUG: Valor '#DATA#': {} ({})", mappings.get("#DATA#"), mappings.get("#DATA#")!=null?mappings.get("#DATA#").getClass().getName():"null");
            else logger.warn(">>> DEBUG: Chave '#DATA#' NÃO encontrada (pode ser normal).");

            String sparqlQuery = buildSparqlQuery(templateName, mappings);
            if (sparqlQuery == null) { return "Erro processamento: Não foi possível montar consulta para '" + templateName + "'. Logs?"; }

            String targetVariable = determineTargetVariable(templateName, mappings);
            if (targetVariable == null) { return "Erro: Não foi possível identificar o que buscar na consulta."; }
            logger.debug("Variável alvo: '{}'", targetVariable);

            logger.info(">>> EXECUTANDO QUERY com target:'{}' <<<", targetVariable);
            if (ontology == null) { logger.error("ERRO CRÍTICO: Instância Ontology nula!"); return "Erro interno: Serviço ontologia indisponível."; }
            List<String> results = ontology.executeQuery(sparqlQuery, targetVariable);
            logger.info(">>> QUERY EXECUTADA. Resultados: {}. <<<", results != null ? results.size() : "ERRO/NULL");

            if (results == null) { logger.error("Execução query retornou null. Verificar logs Ontology."); return "Erro executar consulta SPARQL."; }
            else if (results.isEmpty()) { logger.info("Nenhum resultado encontrado."); return "Não foram encontrados resultados."; }
            else { String joined = results.stream().collect(Collectors.joining(", ")); logger.info("Resultados: {}", joined); return joined; }

        } catch (QueryParseException qpe) { logger.error("Erro sintaxe query SPARQL.", qpe); return "Erro: Consulta gerada inválida."; }
        catch (IOException | URISyntaxException e) { logger.error("Erro I/O ou URI ao executar script.", e); return "Erro interno preparar execução processador."; }
        catch (TimeoutException e) { logger.error("Timeout executar script Python ({}s).", PYTHON_SCRIPT_TIMEOUT, e); return "Erro: Tempo limite excedido processar linguagem."; }
        catch (Exception e) { logger.error("Erro inesperado processar pergunta: '{}'. Tpl: '{}'. Maps: {}. PyRes: {}", question, templateName, mappings, pythonResult, e); return "Erro inesperado processamento."; }
    }

    /** Executa script Python com leitura concorrente e timeout. */
    private Map<String, Object> executePythonScript(String question) throws IOException, URISyntaxException, TimeoutException {
        // *** MODIFICADO PARA USAR findPythonScriptPath ***
        Path scriptPath = findPythonScriptPath();
        if (scriptPath == null) {
            throw new FileNotFoundException("Script Python pln_processor.py não encontrado em nenhum local esperado.");
        }
        // *** FIM DA MODIFICAÇÃO ***

        String pythonExecutable = "python";
        List<String> commandList = List.of(pythonExecutable, scriptPath.toString(), question);
        logger.info("Executando comando: {}", commandList);
        ProcessBuilder pb = new ProcessBuilder(commandList); Process process = pb.start();
        final StringBuilder output = new StringBuilder(); final StringBuilder errorOutput = new StringBuilder();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> stdoutFut = executor.submit(() -> { try (BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) { String l; while ((l = r.readLine()) != null) { synchronized(output){ output.append(l).append(System.lineSeparator()); } } } catch (IOException e) { logger.error("Erro ler stdout python", e); } });
        Future<?> stderrFut = executor.submit(() -> { try (BufferedReader r = new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) { String l; while ((l = r.readLine()) != null) { synchronized(errorOutput){ errorOutput.append(l).append(System.lineSeparator()); } } } catch (IOException e) { logger.error("Erro ler stderr python", e); } });
        int exitCode = -1; boolean finished = false;
        try {
            finished = process.waitFor(PYTHON_SCRIPT_TIMEOUT, TimeUnit.SECONDS);
            if (!finished) { logger.error("Script Python TIMEOUT ({}s). Destruindo...", PYTHON_SCRIPT_TIMEOUT); process.destroy(); try { process.destroyForcibly().waitFor(5, TimeUnit.SECONDS); } catch(InterruptedException ie) { Thread.currentThread().interrupt(); } throw new TimeoutException("Timeout script Python."); }
            exitCode = process.exitValue();
            try { stdoutFut.get(5, TimeUnit.SECONDS); stderrFut.get(5, TimeUnit.SECONDS); } catch (Exception e) { logger.warn("Erro/timeout esperar threads leitoras.", e); }
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); logger.error("Interrompido esperando python.", e); process.destroyForcibly(); return null;
        } finally { executor.shutdownNow(); }
        String rawOut = output.toString().trim(); String rawErr = errorOutput.toString().trim();
        logger.debug("Python stdout (EC:{}):\n---\n{}\n---", exitCode, rawOut); if (!rawErr.isEmpty()) { logger.error("Python stderr (EC:{}):\n---\n{}\n---", exitCode, rawErr); }
        if (exitCode != 0) { logger.error("Script Python falhou EC:{}. Stderr: {}", exitCode, rawErr); return null; }
        if (rawOut.isEmpty() || !rawOut.startsWith("{")) { logger.error("Script Python (EC:0) não retornou JSON válido. Stdout: '{}'. Stderr: '{}'", rawOut, rawErr); return null; }
        try { return objectMapper.readValue(rawOut, new TypeReference<Map<String, Object>>() {}); }
        catch (IOException e) { logger.error("Falha parsear JSON Python: '{}'. Erro: {}", rawOut, e.getMessage(), e); return null; }
    }

    /**
     * Encontra o caminho absoluto para o script pln_processor.py.
     * MODIFICADO para priorizar src/main/resources.
     * @return O Path para o script, ou null se não encontrado.
     */
    private Path findPythonScriptPath() {
        // Caminho relativo à raiz do projeto (onde geralmente o IDE executa)
        Path scriptInSrc = Paths.get("src/main/resources/pln_processor.py").toAbsolutePath();
        if (Files.isRegularFile(scriptInSrc)) {
            logger.info("Script Python encontrado diretamente em: {}", scriptInSrc);
            return scriptInSrc;
        }
        logger.warn("Script Python não encontrado em {}", scriptInSrc);

        // Fallback 1: Tentar relativo ao diretório atual (pode ser target/classes se executado de lá)
        Path scriptInCurrent = Paths.get("./pln_processor.py").toAbsolutePath();
        if (Files.isRegularFile(scriptInCurrent) && !scriptInCurrent.equals(scriptInSrc)) { // Verifica se não é o mesmo que já tentamos
            logger.info("Script Python encontrado como fallback em: {}", scriptInCurrent);
            return scriptInCurrent;
        }
        logger.warn("Script Python não encontrado em {}", scriptInCurrent);

        // Fallback 2: Tentar na pasta target/classes explicitamente (menos ideal)
        Path scriptInTarget = Paths.get("target/classes/pln_processor.py").toAbsolutePath();
        if (Files.isRegularFile(scriptInTarget) && !scriptInTarget.equals(scriptInCurrent) && !scriptInTarget.equals(scriptInSrc) ) {
            logger.info("Script Python encontrado como fallback em: {}", scriptInTarget);
            return scriptInTarget;
        }
        logger.warn("Script Python não encontrado em {}", scriptInTarget);


        logger.error("ERRO CRÍTICO: Script Python 'pln_processor.py' não encontrado nos locais esperados (src/main/resources, ./, target/classes).");
        return null;
    }


    /** Constrói a string da consulta SPARQL final substituindo os placeholders. */
    private String buildSparqlQuery(String templateName, Map<String, Object> mappings) {
        logger.info("Construindo query para template '{}'...", templateName);
        String templatePath = "/" + templateName.replace(" ", "_") + ".txt";
        logger.info("Carregando template: {}", templatePath);
        String queryTemplate = loadTemplateContent(templatePath);
        if (queryTemplate == null) { logger.error("Falha carregar template '{}'.", templatePath); return null; }
        logger.debug("Template ANTES:\n---\n{}\n---", queryTemplate); logger.debug("Mapeamentos: {}", mappings);
        String finalQuery = queryTemplate; final String dataKey = "#DATA#"; final String entidadeKey = "#ENTIDADE_NOME#";

        // Substituir #DATA#
        if (finalQuery.contains(dataKey)) {
            if (mappings.containsKey(dataKey)) {
                Object val = mappings.get(dataKey); if (val instanceof String && ((String)val).matches("\\d{4}-\\d{2}-\\d{2}")) { String repl = "\"" + val + "\"^^<" + XSDDatatype.XSDdate.getURI() + ">"; logger.debug("Subst '{}' -> '{}'", dataKey, repl); finalQuery = finalQuery.replace(dataKey, repl); }
                else { logger.error("ERRO: Valor inválido p/ {}: {} ({})", dataKey, val, val!=null?val.getClass():"null"); return null; }
            } else { logger.error("ERRO: Template '{}' requer {}!", templateName, dataKey); return null; }
        } else { logger.debug("Template '{}' não contém {}.", templateName, dataKey); }

        // Substituir #ENTIDADE_NOME#
        if (finalQuery.contains(entidadeKey)) {
            if (mappings.containsKey(entidadeKey)) {
                Object val = mappings.get(entidadeKey); if (val instanceof String) { String repl = "\"" + escapeSparqlLiteral((String)val) + "\""; logger.debug("Subst '{}' -> '{}'", entidadeKey, repl); finalQuery = finalQuery.replace(entidadeKey, repl); }
                else { logger.error("ERRO: Valor inválido p/ {}: {} ({})", entidadeKey, val, val!=null?val.getClass():"null"); return null; }
            } else { logger.error("ERRO: Template '{}' requer {}!", templateName, entidadeKey); return null; }
        } else { logger.debug("Template '{}' não contém {}.", templateName, entidadeKey); }

        mappings.keySet().stream().filter(k -> !k.equals(dataKey) && !k.equals(entidadeKey) && !k.startsWith("_")).forEach(k -> logger.warn("Placeholder não tratado: {} -> {}", k, mappings.get(k)));
        Matcher matcher = Pattern.compile("(#\\w+#)").matcher(finalQuery); if (matcher.find()) { logger.error("ERRO: Placeholder não substituído: {}", matcher.group(1)); logger.error("Query:\n{}", finalQuery); return null; }
        logger.info("Construção SPARQL OK p/ '{}'.", templateName); logger.info("SPARQL Gerada:\n---\n{}\n---", finalQuery);
        return finalQuery;
    }

    /** Determina variável alvo via #VALOR_DESEJADO#. */
    private String determineTargetVariable(String templateName, Map<String, Object> mappings) {
        logger.debug("Determinando variável alvo..."); final String key = "#VALOR_DESEJADO";
        if (!mappings.containsKey(key)) { logger.error("Placeholder '{}' não encontrado p/ template '{}'.", key, templateName); /* Fallback? */ return null; }
        Object valObj = mappings.get(key); if (!(valObj instanceof String)) { logger.error("Valor p/ '{}' não é String: {}", key, valObj); return null; }
        String val = ((String) valObj).toLowerCase(); String targetVar = placeholderToVariableMap.get(val);
        if (targetVar != null) { logger.info("Var alvo por map de '{}': '{}'", val, targetVar); return targetVar; }
        else { logger.error("Valor '{}' não encontrado no map. Usando fallback.", val); return val; }
    }

    /** Carrega conteúdo do template. */
    private String loadTemplateContent(String resourcePath) {
        logger.debug("Carregando template: {}", resourcePath);
        try (InputStream is = QuestionProcessor.class.getResourceAsStream(resourcePath)) {
            if (is == null) { logger.error("Template não encontrado: {}", resourcePath); return null; }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) { return reader.lines().collect(Collectors.joining(System.lineSeparator())); }
        } catch (IOException e) { logger.error("Erro I/O ler template {}: {}", resourcePath, e.getMessage()); return null; }
        catch (Exception e) { logger.error("Erro inesperado carregar template {}: {}", resourcePath, e.getMessage()); return null; }
    }

    /** Escapa caracteres especiais para literais SPARQL. */
    private String escapeSparqlLiteral(String literal) { if (literal == null) return ""; return literal.replace("\\", "\\\\").replace("\"", "\\\""); }
}