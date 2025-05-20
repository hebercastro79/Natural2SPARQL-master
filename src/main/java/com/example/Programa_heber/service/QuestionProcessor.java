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
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
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
        try {
            Resource resource = new ClassPathResource(PYTHON_SCRIPT_NAME);
            if (resource.exists()) {
                if (resource.isFile() && resource.getURI().getScheme().equals("file")) {
                    pythonScriptPath = Paths.get(resource.getURI());
                    logger.info("Script Python encontrado diretamente em: {}", pythonScriptPath);
                } else {
                    Path tempDir = Files.createTempDirectory("pyscripts_");
                    pythonScriptPath = tempDir.resolve(PYTHON_SCRIPT_NAME);
                    try (InputStream is = resource.getInputStream()) {
                        Files.copy(is, pythonScriptPath);
                        logger.info("Script Python extraído do JAR para: {}", pythonScriptPath);
                    }
                    try {
                        if (!pythonScriptPath.toFile().setExecutable(true)) {
                            logger.warn("Não foi possível marcar o script Python temporário como executável: {}", pythonScriptPath);
                        }
                    } catch (SecurityException se) {
                        logger.warn("Não foi possível marcar o script Python temporário como executável devido a restrições de segurança: {}", se.getMessage());
                    }
                    pythonScriptPath.toFile().deleteOnExit();
                    tempDir.toFile().deleteOnExit();
                }
            } else {
                logger.error("Script Python '{}' não encontrado no classpath.", PYTHON_SCRIPT_NAME);
                throw new FileNotFoundException("Script Python não encontrado: " + PYTHON_SCRIPT_NAME);
            }
            logger.info("QuestionProcessor inicializado. Instância de Ontology injetada: OK");
        } catch (IOException e) {
            logger.error("Erro fatal ao inicializar QuestionProcessor ou localizar/preparar script Python: {}", e.getMessage(), e);
            throw new RuntimeException("Falha crítica ao inicializar QuestionProcessor: não foi possível preparar o script Python.", e);
        }
        logger.info("QuestionProcessor @PostConstruct: Serviço pronto.");
    }

    public ProcessamentoDetalhadoResposta processQuestion(String question) {
        logger.info("Serviço: Processando pergunta: '{}'", question);
        ProcessamentoDetalhadoResposta respostaDetalhada = new ProcessamentoDetalhadoResposta();
        String finalQuery = "N/A - Query não gerada devido a erro anterior.";

        try {
            Map<String, Object> pythonResult = executePythonScript(question);

            if (pythonResult == null || !pythonResult.containsKey("template_nome") || !pythonResult.containsKey("mapeamentos")) {
                logger.error("Resultado inválido ou incompleto do script Python. Resultado: {}", pythonResult);
                respostaDetalhada.setErro("Erro: Falha ao interpretar a resposta do processador de linguagem.");
                respostaDetalhada.setSparqlQuery(finalQuery);
                return respostaDetalhada;
            }

            String templateId = (String) pythonResult.get("template_nome");
            @SuppressWarnings("unchecked")
            Map<String, String> placeholders = (Map<String, String>) pythonResult.get("mapeamentos");

            if (pythonResult.containsKey("_debug_info")) {
                logger.debug("Informações de debug do Python: {}", pythonResult.get("_debug_info"));
            }

            if (templateId == null || templateId.isEmpty()) {
                logger.warn("Script Python não retornou um ID de template válido ('template_nome').");
                respostaDetalhada.setErro("Não foi possível determinar o tipo da pergunta.");
                respostaDetalhada.setSparqlQuery(finalQuery);
                return respostaDetalhada;
            }
            if (placeholders == null) {
                logger.error("Script Python não retornou um mapa de placeholders válido ('mapeamentos').");
                respostaDetalhada.setErro("Erro: Falha ao obter os detalhes da pergunta do processador de linguagem.");
                respostaDetalhada.setSparqlQuery(finalQuery);
                return respostaDetalhada;
            }

            String templateContent = readTemplateContent(templateId);
            if (templateContent == null || templateContent.isEmpty()) {
                logger.error("Não foi possível ler o conteúdo do template: {}", templateId);
                respostaDetalhada.setErro("Erro interno: Template SPARQL não encontrado ou vazio.");
                respostaDetalhada.setSparqlQuery(finalQuery);
                return respostaDetalhada;
            }

            finalQuery = buildSparqlQuery(templateContent, placeholders, templateId);
            respostaDetalhada.setSparqlQuery(finalQuery);
            logger.info("SPARQL Final Gerada:\n---\n{}\n---", finalQuery);

            String targetVariable = "valor";
            if ("Template 2A".equals(templateId)) {
                targetVariable = "individualTicker";
            }

            logger.info("Executando consulta SPARQL para template '{}' com variável alvo '{}'", templateId, targetVariable);
            List<String> resultsList = ontology.executeQuery(finalQuery, targetVariable);

            if (resultsList == null) {
                logger.error("Execução da query SPARQL (via Ontology.executeQuery) retornou null. Query: {}", finalQuery);
                respostaDetalhada.setErro("Erro ao executar a consulta SPARQL na base de conhecimento.");
            } else if (resultsList.isEmpty()) {
                logger.info("Nenhum resultado encontrado para a consulta SPARQL.");
                respostaDetalhada.setResposta("Não foram encontrados resultados que correspondam à sua pergunta.");
            } else {
                StringJoiner joiner = new StringJoiner(", ");
                resultsList.forEach(result -> {
                    String cleanResult = result.replace(BASE_ONTOLOGY_URI, "b3:");
                    cleanResult = cleanResult.replace("http://www.w3.org/2000/01/rdf-schema#", "rdfs:");
                    cleanResult = cleanResult.replace("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:");
                    if (cleanResult != null && !cleanResult.trim().isEmpty()) {
                        joiner.add(cleanResult.trim());
                    }
                });
                String formattedResult = joiner.toString();
                if (!formattedResult.isEmpty()) {
                    logger.info("Resultados formatados: {}", formattedResult);
                    respostaDetalhada.setResposta(formattedResult);
                } else {
                    logger.info("Nenhum resultado válido encontrado após limpeza/formatação.");
                    respostaDetalhada.setResposta("Não foram encontrados resultados que correspondam à sua pergunta.");
                }
            }

        } catch (IOException e) {
            logger.error("Erro de IO durante o processamento da pergunta '{}': {}", question, e.getMessage(), e);
            respostaDetalhada.setErro("Erro interno (IO) durante o processamento: " + e.getMessage());
            respostaDetalhada.setSparqlQuery(finalQuery);
        } catch (InterruptedException e) {
            logger.error("Processamento da pergunta '{}' interrompido: {}", question, e.getMessage(), e);
            Thread.currentThread().interrupt();
            respostaDetalhada.setErro("Processamento interrompido.");
            respostaDetalhada.setSparqlQuery(finalQuery);
        } catch (RuntimeException e) {
            logger.error("Erro inesperado (RuntimeException) ao processar pergunta '{}': {}", question, e.getMessage(), e);
            respostaDetalhada.setErro("Erro inesperado no servidor: " + e.getMessage());
            respostaDetalhada.setSparqlQuery(finalQuery);
        } catch (Exception e) {
            logger.error("Erro completamente inesperado ao processar pergunta '{}': {}", question, e.getMessage(), e);
            respostaDetalhada.setErro("Erro genérico inesperado no servidor.");
            respostaDetalhada.setSparqlQuery(finalQuery);
        }

        logger.info("Pergunta '{}' processada. RespostaDetalhada: {}", question, respostaDetalhada);
        return respostaDetalhada;
    }

    private Map<String, Object> executePythonScript(String question) throws IOException, InterruptedException {
        if (pythonScriptPath == null || !Files.exists(pythonScriptPath)) {
            logger.error("Caminho do script Python não está configurado ou o arquivo não existe: {}", pythonScriptPath);
            throw new FileNotFoundException("Script Python não encontrado ou não configurado.");
        }
        ProcessBuilder pb = new ProcessBuilder("python", pythonScriptPath.toString(), question);
        pb.redirectErrorStream(true);
        logger.info("Executando comando: {}", pb.command());
        Process process = pb.start();
        String output;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            output = reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }
        int exitCode = process.waitFor();
        logger.info("Python stdout/stderr (EC:{}):\n---\n{}\n---", exitCode, output);
        if (exitCode != 0) {
            logger.error("Script Python terminou com erro (Código: {}).", exitCode);
            throw new RuntimeException("Erro na execução do script Python. Código de saída: " + exitCode + ". Verifique os logs para detalhes da saída.");
        }
        String jsonOutput = null;
        String[] lines = output.split("\\r?\\n");
        for (int i = lines.length - 1; i >= 0; i--) {
            String line = lines[i].trim();
            if (!line.isEmpty() && line.startsWith("{") && line.endsWith("}")) {
                jsonOutput = line;
                break;
            }
        }
        if (jsonOutput == null) {
            logger.error("Não foi encontrada a saída JSON esperada do script Python.");
            throw new RuntimeException("Formato de saída inesperado do script Python (JSON não encontrado).");
        }
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(jsonOutput, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            logger.error("Erro ao desserializar JSON do Python: {}. JSON recebido: {}", e.getMessage(), jsonOutput, e);
            throw new RuntimeException("Erro ao processar resposta JSON do script Python.", e);
        }
    }

    public String readTemplateContent(String templateId) throws IOException {
        String templateFileName = templateId.replace(" ", "_") + ".txt";
        logger.info("Tentando ler template do classpath: {}", "templates/" + templateFileName);
        Resource resource = new ClassPathResource("templates/" + templateFileName);

        if (!resource.exists()) {
            logger.error("Arquivo de template não encontrado no classpath: {}", "templates/" + templateFileName);
            resource = new ClassPathResource(templateFileName); // Fallback para raiz
            if (!resource.exists()) {
                logger.error("Arquivo de template também não encontrado na raiz do classpath: {}", templateFileName);
                throw new FileNotFoundException("Template não encontrado: " + templateFileName);
            }
            logger.warn("Template encontrado na raiz do resources, mas o esperado é em /templates/");
        }

        try (InputStream inputStream = resource.getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining(System.lineSeparator()));
        } catch (IOException e) {
            logger.error("Erro ao ler arquivo de template: {}", templateFileName, e);
            throw e;
        }
    }

    // *** MÉTODO buildSparqlQuery CORRIGIDO ***
    private String buildSparqlQuery(String templateContent, Map<String, String> placeholders, String templateId) {
        String finalQuery = templateContent;
        logger.debug("Iniciando construção da query para template '{}' com placeholders: {}", templateId, placeholders);

        // Itera sobre os placeholders para fazer as substituições
        for (Map.Entry<String, String> entry : placeholders.entrySet()) {
            String placeholderKey = entry.getKey();
            String value = entry.getValue();
            String replacement = null;

            if (value == null) {
                logger.warn("Valor nulo para placeholder: {}", placeholderKey);
                replacement = "__ERRO_VALOR_NULO_" + placeholderKey.replace("#","").replace("<","").replace(">","") + "__"; // Evita erro de sintaxe
            } else {
                switch (placeholderKey) {
                    case "#ENTIDADE_NOME#":
                        // Adiciona aspas duplas SEMPRE. Funciona para ticker em 1A/1B e label em 2A.
                        replacement = "\"" + value.replace("\"", "\\\"") + "\"";
                        break;

                    case "#DATA#":
                        replacement = "\"" + value + "\"^^xsd:date";
                        break;

                    case "#VALOR_DESEJADO#":
                        // Para templates 1A/1B, este valor (ex: "precoFechamento") vira o predicado URI.
                        if (templateId.equals("Template 1A") || templateId.equals("Template 1B")) {
                            if (value.equals("codigo")) { // "codigo" não é um predicado de valor para estes templates
                                logger.error("Erro de lógica: Template {} recebeu 'codigo' como #VALOR_DESEJADO#, que deveria ser um predicado de valor.", templateId);
                                replacement = "b3:ERRO_VALOR_INVALIDO_PARA_PREDICADO";
                            } else {
                                String cleanValue = value.split("#")[0].trim(); // Remove comentários, se houver
                                if (!cleanValue.isEmpty()) {
                                    replacement = "b3:" + cleanValue; // Adiciona prefixo para URI
                                } else {
                                    logger.error("Valor para #VALOR_DESEJADO# (predicado) resultou em vazio após limpeza: '{}'. Query será inválida.", value);
                                    replacement = "b3:ERRO_PREDICADO_VAZIO";
                                }
                            }
                        } else {
                            // Para Template 2A, o valor é "codigo". O placeholder #VALOR_DESEJADO#
                            // não é usado como predicado na query SPARQL do Template 2A.
                            // Mantém o placeholder original para ser removido depois se não estiver no template.
                            replacement = placeholderKey; // Mantém o placeholder original
                            logger.trace("Placeholder #VALOR_DESEJADO# ('{}') não usado como predicado direto para template {}", value, templateId);
                        }
                        break;

                    default:
                        logger.warn("Placeholder não reconhecido: {}. Substituição direta com valor: '{}'", placeholderKey, value);
                        replacement = value; // Fallback para substituição direta
                        break;
                }
            }

            // Realiza a substituição se replacement foi definido E é diferente do placeholder original
            if (replacement != null && !replacement.equals(placeholderKey)) {
                logger.trace("Substituindo '{}' por '{}'", placeholderKey, replacement);
                finalQuery = finalQuery.replace(placeholderKey, replacement);
            } else if (replacement != null && replacement.equals(placeholderKey)) {
                // Caso onde o placeholder não deveria ser substituído (ex: #VALOR_DESEJADO# para Template 2A)
                logger.trace("Placeholder {} mantido como está (não será substituído por este case, pode ser removido depois se não usado no template).", placeholderKey);
            } else {
                // replacementValue é null (por causa do value == null)
                logger.warn("Replacement nulo para placeholder: {} (valor original era nulo). Placeholder original mantido para remoção.", placeholderKey);
                finalQuery = finalQuery.replace(placeholderKey, "__ERRO_PLACEHOLDER_COM_VALOR_NULO__"); // Garante que é substituído
            }
        }

        // Limpeza final: remove placeholders que não foram substituídos e não deveriam estar lá
        // Ex: Se #VALOR_DESEJADO# não for usado no Template 2A, ele será removido.
        String queryAntesDaLimpezaFinal = finalQuery;
        finalQuery = finalQuery.replaceAll("#[A-Z_]+#", ""); // Remove qualquer #PLACEHOLDER# restante

        if (!finalQuery.equals(queryAntesDaLimpezaFinal)) {
            logger.warn("Um ou mais placeholders não substituídos foram removidos da query. Query antes: \n{}\nQuery depois:\n{}", queryAntesDaLimpezaFinal, finalQuery);
        }

        logger.info("Construção da query SPARQL finalizada.");
        return finalQuery;
    }
}