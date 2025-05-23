package com.example.Programa_heber.service;

import com.example.Programa_heber.model.ProcessamentoDetalhadoResposta;
import com.example.Programa_heber.ontology.Ontology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class QuestionProcessor {

    private static final Logger logger = LoggerFactory.getLogger(QuestionProcessor.class);
    private static final String PYTHON_SCRIPT_NAME = "pln_processor.py";
    private static final String BASE_ONTOLOGY_URI = "https://dcm.ffclrp.usp.br/lssb/stock-market-ontology#";

    @Autowired
    private Ontology ontology;

    private Path pythonScriptPath;

    @PostConstruct
    public void initialize() {
        logger.info("Iniciando QuestionProcessor (@PostConstruct)...");
        try {
            Resource resource = new ClassPathResource("scripts/" + PYTHON_SCRIPT_NAME);
            if (!resource.exists()) {
                logger.warn("Script Python '{}' não encontrado em 'resources/scripts/'. Tentando na raiz de 'resources'.", PYTHON_SCRIPT_NAME);
                resource = new ClassPathResource(PYTHON_SCRIPT_NAME);
            }

            if (!resource.exists()) {
                logger.error("CRÍTICO: Script Python '{}' não encontrado no classpath. O processamento de perguntas falhará.", PYTHON_SCRIPT_NAME);
                throw new FileNotFoundException("Script Python essencial não encontrado: " + PYTHON_SCRIPT_NAME);
            }

            String resourceURIPath = resource.getURI().toString();
            if (resourceURIPath.startsWith("jar:")) {
                Path tempDir = Files.createTempDirectory("pyscripts_temp_");
                tempDir.toFile().deleteOnExit();
                this.pythonScriptPath = tempDir.resolve(PYTHON_SCRIPT_NAME);
                try (InputStream inputStream = resource.getInputStream()) {
                    Files.copy(inputStream, this.pythonScriptPath);
                }
                boolean executableSet = this.pythonScriptPath.toFile().setExecutable(true, false);
                if (executableSet) {
                    logger.info("Script Python '{}' extraído para path temporário e marcado como executável: {}", PYTHON_SCRIPT_NAME, this.pythonScriptPath);
                } else {
                    logger.warn("Não foi possível marcar o script Python temporário ({}) como executável.", this.pythonScriptPath);
                }
                this.pythonScriptPath.toFile().deleteOnExit();
            } else {
                this.pythonScriptPath = Paths.get(resource.getURI());
                logger.info("Script Python '{}' encontrado diretamente no sistema de arquivos: {}", PYTHON_SCRIPT_NAME, this.pythonScriptPath);
                if (!Files.isExecutable(this.pythonScriptPath)) {
                    if(!this.pythonScriptPath.toFile().setExecutable(true, false)){
                        logger.error("Falha ao marcar script Python {} como executável.", this.pythonScriptPath);
                    } else {
                        logger.info("Script Python {} marcado como executável.", this.pythonScriptPath);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("CRÍTICO: Erro de IO ao inicializar QuestionProcessor/preparar script Python: {}. Processamento indisponível.", e.getMessage(), e);
            this.pythonScriptPath = null;
        } catch (Exception eAll) {
            logger.error("CRÍTICO: Erro inesperado durante a inicialização do QuestionProcessor: {}. ", eAll.getMessage(), eAll);
            this.pythonScriptPath = null;
        }
        logger.info("QuestionProcessor @PostConstruct: Finalizado. Path do script Python: {}", (this.pythonScriptPath != null ? this.pythonScriptPath.toString() : "NÃO CONFIGURADO/ERRO"));
    }

    public ProcessamentoDetalhadoResposta processQuestion(String question) {
        logger.info("Serviço QuestionProcessor: Iniciando processamento da pergunta: '{}'", question);
        ProcessamentoDetalhadoResposta respostaDetalhada = new ProcessamentoDetalhadoResposta();
        String sparqlQueryGerada = "N/A - Query não gerada.";

        if (this.pythonScriptPath == null || !Files.exists(this.pythonScriptPath)) {
            logger.error("Erro Crítico: Script Python não está configurado ou não existe ({}).", this.pythonScriptPath);
            respostaDetalhada.setErro("Erro crítico interno: Componente de processamento de linguagem não está disponível.");
            respostaDetalhada.setSparqlQuery(sparqlQueryGerada);
            return respostaDetalhada;
        }

        try {
            Map<String, Object> resultadoPython = executePythonScript(question);
            ObjectMapper objectMapper = new ObjectMapper();

            String templateId = (String) resultadoPython.get("template_nome");
            @SuppressWarnings("unchecked")
            Map<String, String> placeholders = (Map<String, String>) resultadoPython.get("mapeamentos");

            if (resultadoPython.containsKey("_debug_info") && resultadoPython.get("_debug_info") != null) {
                Object debugInfoObj = resultadoPython.get("_debug_info");
                String debugInfoStr = (debugInfoObj instanceof String) ? (String) debugInfoObj : objectMapper.writeValueAsString(debugInfoObj);
                logger.info("Debug Info do Python: {}", debugInfoStr);
                respostaDetalhada.setDebugInfo(debugInfoStr);
            }

            if (resultadoPython.containsKey("erro") && resultadoPython.get("erro") != null) {
                String erroPython = String.valueOf(resultadoPython.get("erro"));
                logger.error("Script Python retornou erro lógico: '{}'. Pergunta: '{}'", erroPython, question);
                respostaDetalhada.setErro("Falha no processamento da linguagem: " + erroPython);
                return respostaDetalhada;
            }

            if (templateId == null || templateId.trim().isEmpty()) {
                logger.error("Python não retornou 'template_nome' válido. Pergunta: '{}'", question);
                respostaDetalhada.setErro("Não foi possível determinar o tipo da pergunta (template não identificado).");
                return respostaDetalhada;
            }
            if (placeholders == null) {
                logger.error("Python não retornou 'mapeamentos' válidos (null) para template '{}'. Pergunta: '{}'", templateId, question);
                respostaDetalhada.setErro("Erro interno: Falha ao obter detalhes da pergunta do processador de linguagem.");
                return respostaDetalhada;
            }

            logger.info("Python para pergunta '{}': Template ID='{}', Placeholders={}", question, templateId, placeholders);

            String conteudoTemplate = readTemplateContent(templateId);
            sparqlQueryGerada = buildSparqlQuery(conteudoTemplate, placeholders, templateId);
            respostaDetalhada.setSparqlQuery(sparqlQueryGerada);
            logger.info("SPARQL Gerada para template '{}':\n---\n{}\n---", templateId, sparqlQueryGerada);

            String variavelAlvoSparql = "valor";
            // *** CORREÇÃO PARA Template 2A ***
            if ("Template 2A".equals(templateId)) {
                variavelAlvoSparql = "individualTicker";
                logger.debug("Template 2A: Usando variável alvo '{}'", variavelAlvoSparql);
            }
            // Para Template 3A e outros que usam ?valor, o padrão está OK.

            List<String> listaResultados = ontology.executeQuery(sparqlQueryGerada, variavelAlvoSparql);

            if (listaResultados == null) {
                logger.error("Ontology.executeQuery retornou null. Query: {}", sparqlQueryGerada);
                respostaDetalhada.setErro("Erro ao executar a consulta na base de conhecimento.");
                listaResultados = new ArrayList<>();
            }

            if (listaResultados.isEmpty()) {
                logger.info("Nenhum resultado encontrado para a consulta SPARQL.");
                respostaDetalhada.setResposta("Não foram encontrados resultados que correspondam à sua pergunta.");
            } else {
                StringJoiner joinerResultados = new StringJoiner(", ");
                listaResultados.forEach(item -> {
                    if (item != null) {
                        String limpo = item;
                        if (item.startsWith(BASE_ONTOLOGY_URI)) limpo = item.substring(BASE_ONTOLOGY_URI.length());
                        limpo = limpo.replaceAll("\\^\\^<http://www.w3.org/2001/XMLSchema#string>", "");
                        limpo = limpo.replaceAll("\\^\\^xsd:string", "");
                        limpo = limpo.replaceAll("\\^\\^<http://www.w3.org/2001/XMLSchema#date>", "");
                        limpo = limpo.replaceAll("\\^\\^xsd:date", "");
                        if (!limpo.trim().isEmpty()) joinerResultados.add(limpo.trim());
                    }
                });
                String resFmt = joinerResultados.toString();
                respostaDetalhada.setResposta(resFmt.isEmpty() ? "Não foram encontrados resultados válidos." : resFmt);
                logger.info("Resultados formatados: {}", resFmt);
            }

        } catch (FileNotFoundException e_fnf) {
            logger.error("Erro (Arquivo não encontrado - template SPARQL?) ao processar '{}': {}", question, e_fnf.getMessage(), e_fnf);
            respostaDetalhada.setErro("Erro interno: Componente necessário não encontrado (" + e_fnf.getMessage() + ").");
        } catch (IOException e_io) {
            logger.error("Erro de IO ao processar '{}': {}", question, e_io.getMessage(), e_io);
            respostaDetalhada.setErro("Erro interno (IO): " + e_io.getMessage());
        } catch (InterruptedException e_intr) {
            logger.error("Processamento de '{}' interrompido: {}", question, e_intr.getMessage(), e_intr);
            Thread.currentThread().interrupt();
            respostaDetalhada.setErro("Processamento interrompido.");
        } catch (RuntimeException e_rt) {
            logger.error("Erro de Runtime (Python ou outro) ao processar '{}': {}", question, e_rt.getMessage(), e_rt);
            respostaDetalhada.setErro("Erro inesperado no servidor: " + e_rt.getMessage());
        } catch (Exception e_geral) {
            logger.error("Erro GENÉRICO e inesperado ao processar '{}': {}", question, e_geral.getMessage(), e_geral);
            respostaDetalhada.setErro("Erro genérico e inesperado no servidor.");
        }
        if (respostaDetalhada.getSparqlQuery() == null) respostaDetalhada.setSparqlQuery(sparqlQueryGerada);
        logger.info("Processamento da pergunta '{}' finalizado. Resposta: {}", question, respostaDetalhada);
        return respostaDetalhada;
    }

    private Map<String, Object> executePythonScript(String question) throws IOException, InterruptedException {
        String pythonExec = System.getProperty("python.executable", "python3");
        Path pythonExecPath = Paths.get(pythonExec);
        if (!Files.exists(pythonExecPath) || !Files.isExecutable(pythonExecPath)) {
            // Tenta encontrar no PATH se o caminho absoluto falhar ou não for fornecido
            boolean foundInPath = false;
            String systemPath = System.getenv("PATH");
            if (systemPath != null) {
                for (String pathDir : systemPath.split(File.pathSeparator)) {
                    Path potentialPath = Paths.get(pathDir, pythonExec);
                    if (Files.exists(potentialPath) && Files.isExecutable(potentialPath)) {
                        pythonExec = potentialPath.toString();
                        foundInPath = true;
                        break;
                    }
                }
            }
            if (!foundInPath) { // Fallback para "python" simples
                logger.warn("'python3' não encontrado ou não executável, tentando 'python'.");
                pythonExec = "python";
            }
        }


        ProcessBuilder pb = new ProcessBuilder(pythonExec, this.pythonScriptPath.toString(), question);
        pb.environment().put("PYTHONIOENCODING", "UTF-8");
        logger.info("Executando comando Python: {}", pb.command());
        Process process = pb.start();

        StringBuilder stdoutCollector = new StringBuilder();
        StringBuilder stderrCollector = new StringBuilder();
        try (
                BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
                BufferedReader stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))
        ) {
            String line;
            while ((line = stdoutReader.readLine()) != null) stdoutCollector.append(line).append(System.lineSeparator());
            while ((line = stderrReader.readLine()) != null) stderrCollector.append(line).append(System.lineSeparator());
        }

        int exitCode = process.waitFor();
        String stdoutResult = stdoutCollector.toString().trim();
        String stderrResult = stderrCollector.toString().trim();

        if (!stderrResult.isEmpty()) logger.warn("Script Python (stderr):\n---\n{}\n---", stderrResult);
        logger.info("Script Python (stdout) (Cód. Saída: {}):\n---\n{}\n---", exitCode, stdoutResult);

        if (stdoutResult.isEmpty()) {
            String errorMsg = "Falha na execução do script Python: Sem saída JSON.";
            if (exitCode != 0) errorMsg += " Código de erro: " + exitCode;
            if (!stderrResult.isEmpty()) errorMsg += ". Stderr: " + stderrResult;
            logger.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }

        try {
            return new ObjectMapper().readValue(stdoutResult, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e_parse) {
            logger.error("Erro ao desserializar JSON do Python: {}. Stdout: '{}', Stderr: '{}'", e_parse.getMessage(), stdoutResult, stderrResult, e_parse);
            throw new RuntimeException("Erro ao processar resposta JSON do script Python. Verifique os logs.", e_parse);
        }
    }

    public String readTemplateContent(String templateId) throws IOException {
        String templateFileName = templateId.trim().replace(" ", "_") + ".txt";
        String templateResourcePath = "templates/" + templateFileName;
        logger.info("Lendo template SPARQL: {}", templateResourcePath);

        Resource resource = new ClassPathResource(templateResourcePath);
        if (!resource.exists()) {
            logger.error("ARQUIVO DE TEMPLATE SPARQL NÃO ENCONTRADO: {}", templateResourcePath);
            throw new FileNotFoundException("Template SPARQL não encontrado: " + templateResourcePath);
        }
        try (InputStream inputStream = resource.getInputStream();
             BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return bufferedReader.lines().collect(Collectors.joining(System.lineSeparator()));
        }
    }

    private String buildSparqlQuery(String templateContent, Map<String, String> placeholders, String templateId) {
        String queryAtual = templateContent;
        logger.debug("Construindo query SPARQL para template '{}'. Placeholders: {}", templateId, placeholders);

        if (placeholders != null && !placeholders.isEmpty()) {
            for (Map.Entry<String, String> entry : placeholders.entrySet()) {
                String phKey = entry.getKey();
                String phValue = entry.getValue();
                String valorSubstituicao;

                if (phValue == null) {
                    logger.warn("Valor NULO para placeholder: '{}' (template '{}'). Substituindo por string de erro.", phKey, templateId);
                    valorSubstituicao = "\"__ERRO_PH_VALOR_NULO_" + phKey.replace("#", "") + "__\"";
                } else {
                    String valorLimpo = phValue.replace("\"", "\\\"").trim();
                    if (valorLimpo.isEmpty() && !phKey.toLowerCase().contains("opcional")) {
                        logger.warn("Valor VAZIO para placeholder: '{}' (template '{}'). Substituindo por string de erro.", phKey, templateId);
                        valorSubstituicao = "\"__ERRO_PH_VALOR_VAZIO_" + phKey.replace("#", "") + "__\"";
                    } else {
                        switch (phKey) {
                            case "#ENTIDADE_NOME#":
                                valorSubstituicao = "\"" + valorLimpo + "\"";
                                break;
                            case "#DATA#":
                                valorSubstituicao = "\"" + valorLimpo + "\"^^xsd:date";
                                break;
                            case "#VALOR_DESEJADO#":
                                if (!valorLimpo.isEmpty()) {
                                    valorSubstituicao = "b3:" + valorLimpo;
                                } else {
                                    logger.error("Valor para #VALOR_DESEJADO# (predicado) VAZIO. Template {}.", templateId);
                                    valorSubstituicao = "b3:ERRO_PREDICADO_VAZIO";
                                }
                                break;
                            case "#SETOR#":
                                valorSubstituicao = "\"" + valorLimpo + "\"";
                                break;
                            default:
                                logger.warn("Placeholder não tratado no switch: '{}' (template '{}'). Tratando como literal: '{}'", phKey, templateId, valorLimpo);
                                valorSubstituicao = "\"" + valorLimpo + "\"";
                                break;
                        }
                    }
                }

                if (queryAtual.contains(phKey)) {
                    queryAtual = queryAtual.replace(phKey, valorSubstituicao);
                    logger.trace("Template '{}': Placeholder '{}' -> '{}'", templateId, phKey, valorSubstituicao);
                } else if (!phKey.toLowerCase().contains("opcional")){
                    logger.warn("Template '{}': Placeholder '{}' (valor: '{}') recebido do Python, mas NÃO encontrado no corpo do template.", templateId, phKey, phValue);
                }
            }
        } else {
            logger.warn("Nenhum placeholder recebido para o template '{}'.", templateId);
        }

        Matcher matcherNaoSubst = Pattern.compile("#[A-Z_0-9]+#").matcher(queryAtual);
        if (matcherNaoSubst.find()) {
            logger.warn("Query final para template '{}' AINDA CONTÉM placeholders não substituídos (ex: {}). Query:\n{}",
                    templateId, matcherNaoSubst.group(0), queryAtual);
        }
        logger.info("Construção da query SPARQL para template '{}' finalizada.", templateId);
        return queryAtual;
    }
}