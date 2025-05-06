package com.example.Programa_heber.service;

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
    private static final String BASE_ONTOLOGY_URI = "https://dcm.ffclrp.usp.br/lssb/stock-market-ontology#"; // Usado para limpar prefixos na saída

    @Autowired
    private Ontology ontology; // Injeta a instância da Ontologia gerenciada pelo Spring

    private Path pythonScriptPath; // Caminho para o script Python (determinado no initialize)

    @PostConstruct
    public void initialize() {
        // Lógica para encontrar/extrair o script Python na inicialização
        try {
            Resource resource = new ClassPathResource(PYTHON_SCRIPT_NAME);
            if (resource.exists()) {
                // Checa se o recurso está no sistema de arquivos (fora do JAR)
                if (resource.isFile() && resource.getURI().getScheme().equals("file")) {
                    pythonScriptPath = Paths.get(resource.getURI());
                    logger.info("Script Python encontrado diretamente em: {}", pythonScriptPath);
                } else { // Assume que está dentro de um JAR
                    // Extrai para um diretório temporário
                    Path tempDir = Files.createTempDirectory("pyscripts_"); // Adiciona _ para evitar conflito se rodar multiplas vezes
                    pythonScriptPath = tempDir.resolve(PYTHON_SCRIPT_NAME);
                    try (InputStream is = resource.getInputStream()) {
                        Files.copy(is, pythonScriptPath); // Copia o script do JAR para o temp
                        logger.info("Script Python extraído do JAR para: {}", pythonScriptPath);
                    }
                    // Tenta tornar executável (importante em ambientes Linux/Mac)
                    try {
                        if (!pythonScriptPath.toFile().setExecutable(true)) {
                            logger.warn("Não foi possível marcar o script Python temporário como executável: {}", pythonScriptPath);
                        }
                    } catch (SecurityException se) {
                        logger.warn("Não foi possível marcar o script Python temporário como executável devido a restrições de segurança: {}", se.getMessage());
                    }
                    // Agenda a exclusão dos arquivos/diretórios temporários quando a JVM encerrar
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
            // Lança uma exceção Runtime para impedir que a aplicação inicie incorretamente
            throw new RuntimeException("Falha crítica ao inicializar QuestionProcessor: não foi possível preparar o script Python.", e);
        }
        logger.info("QuestionProcessor @PostConstruct: Serviço pronto.");
    }

    public String processQuestion(String question) { // Removido throws IOException, InterruptedException - serão tratadas internamente
        logger.info("Serviço: Processando pergunta: '{}'", question);
        String resultadoFinal; // Variável para armazenar o resultado ou mensagem de erro

        try {
            // 1. Executa o script Python para análise NLP
            Map<String, Object> pythonResult = executePythonScript(question);

            // 2. Valida e Extrai dados do resultado Python
            if (pythonResult == null || !pythonResult.containsKey("template_nome") || !pythonResult.containsKey("mapeamentos")) {
                logger.error("Resultado inválido ou incompleto do script Python. Chaves 'template_nome' ou 'mapeamentos' ausentes. Resultado: {}", pythonResult);
                return "Erro: Falha ao interpretar a resposta do processador de linguagem."; // Retorna erro imediatamente
            }

            String templateId = (String) pythonResult.get("template_nome");
            @SuppressWarnings("unchecked")
            Map<String, String> placeholders = (Map<String, String>) pythonResult.get("mapeamentos");

            // Log de Debug (Opcional)
            if (pythonResult.containsKey("_debug_info")) {
                logger.debug("Informações de debug do Python: {}", pythonResult.get("_debug_info"));
            }

            if (templateId == null || templateId.isEmpty()) {
                logger.warn("Script Python não retornou um ID de template válido ('template_nome').");
                return "Não foi possível determinar o tipo de pergunta.";
            }
            if (placeholders == null) {
                logger.error("Script Python não retornou um mapa de placeholders válido ('mapeamentos').");
                return "Erro: Falha ao obter os detalhes da pergunta do processador de linguagem.";
            }

            // 3. Lê o conteúdo do template SPARQL
            String templateContent = readTemplateContent(templateId);
            if (templateContent == null || templateContent.isEmpty()) {
                logger.error("Não foi possível ler o conteúdo do template: {}", templateId);
                return "Erro interno: Template SPARQL não encontrado ou vazio.";
            }

            // 4. Constrói a query SPARQL final
            String finalQuery = buildSparqlQuery(templateContent, placeholders, templateId); // Passa templateId para lógica de substituição
            logger.info("SPARQL Final Gerada:\n---\n{}\n---", finalQuery);

            // 5. Determina a variável alvo
            String targetVariable = "valor"; // Padrão para 1A, 1B
            if ("Template 2A".equals(templateId)) {
                targetVariable = "individualTicker"; // Específico para 2A ajustado
            }
            // Adicione mais 'else if' para outros templates se necessário

            // 6. Executa a query SPARQL na ontologia
            logger.info("Executando consulta SPARQL para template '{}' com variável alvo '{}'", templateId, targetVariable);
            List<String> resultsList = ontology.executeQuery(finalQuery, targetVariable); // Assumindo que retorna List<String>

            // 7. Formata o resultado final
            if (resultsList == null) {
                // Indica um erro na execução da query (provavelmente sintaxe ou problema na Ontology.java)
                logger.error("Execução da query SPARQL (via Ontology.executeQuery) retornou null.");
                resultadoFinal = "Erro ao executar a consulta SPARQL na base de conhecimento.";
            } else if (resultsList.isEmpty()) {
                logger.info("Nenhum resultado encontrado para a consulta SPARQL.");
                resultadoFinal = "Não foram encontrados resultados que correspondam à sua pergunta.";
            } else {
                // Junta os resultados encontrados com ", "
                StringJoiner joiner = new StringJoiner(", ");
                resultsList.forEach(result -> {
                    // Limpeza opcional de prefixos na saída
                    String cleanResult = result.replace(BASE_ONTOLOGY_URI, "b3:");
                    cleanResult = cleanResult.replace("http://www.w3.org/2000/01/rdf-schema#", "rdfs:");
                    cleanResult = cleanResult.replace("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:");
                    // Adiciona apenas se não estiver vazio após limpeza
                    if (cleanResult != null && !cleanResult.trim().isEmpty()) {
                        joiner.add(cleanResult.trim());
                    }
                });
                resultadoFinal = joiner.toString();
                // Loga apenas se o resultado final não estiver vazio
                if (!resultadoFinal.isEmpty()) {
                    logger.info("Resultados formatados: {}", resultadoFinal);
                } else {
                    // Se a lista não era vazia, mas ficou vazia após limpeza, informa que não encontrou.
                    logger.info("Nenhum resultado válido encontrado após limpeza/formatação.");
                    resultadoFinal = "Não foram encontrados resultados que correspondam à sua pergunta."; // Mensagem padrão se ficou vazio
                }
            }

        } catch (IOException e) {
            logger.error("Erro de IO durante o processamento da pergunta '{}': {}", question, e.getMessage(), e);
            resultadoFinal = "Erro interno (IO) durante o processamento: " + e.getMessage();
        } catch (InterruptedException e) {
            logger.error("Processamento da pergunta '{}' interrompido: {}", question, e.getMessage(), e);
            Thread.currentThread().interrupt(); // Re-interrompe a thread
            resultadoFinal = "Processamento interrompido.";
        } catch (RuntimeException e) { // Captura erros não verificados (ex: erro no Python, JSON inválido)
            logger.error("Erro inesperado (RuntimeException) ao processar pergunta '{}': {}", question, e.getMessage(), e);
            resultadoFinal = "Erro inesperado no servidor: " + e.getMessage();
        } catch (Exception e) { // Captura genérica para qualquer outra coisa
            logger.error("Erro completamente inesperado ao processar pergunta '{}': {}", question, e.getMessage(), e);
            resultadoFinal = "Erro genérico inesperado no servidor.";
        }

        logger.info("Pergunta '{}' processada. Resposta final: '{}'", question, resultadoFinal);
        return resultadoFinal;
    }

    // --- MÉTODOS AUXILIARES ---

    private Map<String, Object> executePythonScript(String question) throws IOException, InterruptedException {
        if (pythonScriptPath == null || !Files.exists(pythonScriptPath)) {
            logger.error("Caminho do script Python não está configurado ou o arquivo não existe: {}", pythonScriptPath);
            throw new FileNotFoundException("Script Python não encontrado ou não configurado.");
        }

        ProcessBuilder pb = new ProcessBuilder("python", pythonScriptPath.toString(), question);
        pb.redirectErrorStream(true); // Junta stdout e stderr

        logger.info("Executando comando: {}", pb.command());
        Process process = pb.start();

        // Lê a saída do processo
        String output;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            output = reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }

        int exitCode = process.waitFor(); // Espera o processo terminar

        // Loga a saída completa para depuração
        logger.info("Python stdout/stderr (EC:{}):\n---\n{}\n---", exitCode, output);

        if (exitCode != 0) {
            logger.error("Script Python terminou com erro (Código: {}).", exitCode);
            // Lança uma exceção mais específica para ser capturada no método chamador
            throw new RuntimeException("Erro na execução do script Python. Código de saída: " + exitCode + ". Verifique os logs para detalhes da saída.");
        }

        // Encontra a linha JSON na saída
        String jsonOutput = null;
        String[] lines = output.split("\\r?\\n");
        for (int i = lines.length - 1; i >= 0; i--) {
            String line = lines[i].trim();
            // Procura por uma linha que começa com { e termina com }
            if (!line.isEmpty() && line.startsWith("{") && line.endsWith("}")) {
                jsonOutput = line;
                break; // Encontrou a linha JSON esperada
            }
        }

        if (jsonOutput == null) {
            logger.error("Não foi encontrada a saída JSON esperada do script Python.");
            throw new RuntimeException("Formato de saída inesperado do script Python (JSON não encontrado).");
        }

        // Desserializa a saída JSON
        ObjectMapper mapper = new ObjectMapper();
        try {
            // Usa TypeReference para desserializar para Map<String, Object> corretamente
            return mapper.readValue(jsonOutput, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            logger.error("Erro ao desserializar JSON do Python: {}. JSON recebido: {}", e.getMessage(), jsonOutput, e);
            throw new RuntimeException("Erro ao processar resposta JSON do script Python.", e);
        }
    }

    private String readTemplateContent(String templateId) throws IOException {
        // Gera o nome do arquivo (ex: Template_2A.txt)
        String templateFileName = templateId.replace(" ", "_") + ".txt";
        logger.info("Tentando ler template do classpath: {}", templateFileName);
        Resource resource = new ClassPathResource(templateFileName); // Busca no classpath

        if (!resource.exists()) {
            logger.error("Arquivo de template não encontrado no classpath: {}", templateFileName);
            // Poderia lançar uma exceção aqui ou retornar null/vazio dependendo do requisito
            throw new FileNotFoundException("Template não encontrado: " + templateFileName);
        }

        // Lê o conteúdo do arquivo usando UTF-8
        try (InputStream inputStream = resource.getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining(System.lineSeparator()));
        } catch (IOException e) {
            logger.error("Erro ao ler arquivo de template: {}", templateFileName, e);
            throw e; // Relança a exceção para ser tratada acima
        }
    }

    // *** MÉTODO buildSparqlQuery CORRIGIDO ***
    private String buildSparqlQuery(String templateContent, Map<String, String> placeholders, String templateId) {
        String finalQuery = templateContent;
        logger.debug("Iniciando construção da query para template '{}' com placeholders: {}", templateId, placeholders);

        // Itera sobre os placeholders para fazer as substituições
        for (Map.Entry<String, String> entry : placeholders.entrySet()) {
            String placeholder = entry.getKey();
            String value = entry.getValue();
            String replacement = null; // Determina o que substituir

            if (value == null) {
                logger.warn("Valor nulo para placeholder: {}", placeholder);
                replacement = "__ERRO_VALOR_NULO_" + placeholder.replace("#","") + "__"; // Evita erro de sintaxe
            } else {
                switch (placeholder) {
                    case "#ENTIDADE_NOME#":
                        // Adiciona aspas duplas SEMPRE. Funciona para ticker em 1A/1B e label em 2A.
                        replacement = "\"" + value.replace("\"", "\\\"") + "\"";
                        break;

                    case "#DATA#":
                        replacement = "\"" + value + "\"^^xsd:date";
                        break;

                    case "#VALOR_DESEJADO#":
                        // Para templates 1A/1B, este valor (ex: "precoFechamento") vira o predicado URI.
                        // Para template 2A, o valor é "codigo" e não deve ser usado como predicado.
                        if (templateId.equals("Template 1A") || templateId.equals("Template 1B")) {
                            if (value.equals("codigo")) {
                                logger.error("Erro: Template {} recebeu 'codigo' como #VALOR_DESEJADO#, o que é inesperado.", templateId);
                                replacement = "b3:ERRO_VALOR_INVALIDO"; // Causa erro SPARQL intencional
                            } else {
                                // Remove possíveis comentários remanescentes do arquivo .txt (DEFENSIVO)
                                String cleanValue = value.split("#")[0].trim();
                                if (!cleanValue.isEmpty()) {
                                    replacement = "b3:" + cleanValue; // Adiciona prefixo para URI
                                } else {
                                    logger.error("Valor para #VALOR_DESEJADO# resultou em vazio após limpeza: '{}'. Query será inválida.", value);
                                    replacement = "b3:ERRO_VALOR_VAZIO";
                                }
                            }
                        } else {
                            // Para outros templates (como 2A), este placeholder não é substituído aqui.
                            replacement = placeholder; // Mantém para ser removido depois, se não for usado.
                            logger.trace("Placeholder #VALOR_DESEJADO# não substituído como predicado para template {}", templateId);
                        }
                        break;

                    // Não precisamos mais do case #PREDICADO_VALOR#

                    default:
                        logger.warn("Placeholder não reconhecido: {}. Fazendo substituição direta com o valor '{}'.", placeholder, value);
                        replacement = value; // Substituição direta para casos não previstos
                        break;
                }
            }

            // Realiza a substituição se replacement foi definido e é diferente do placeholder original
            if (replacement != null && !replacement.equals(placeholder)) {
                logger.trace("Substituindo '{}' por '{}'", placeholder, replacement);
                // Usar replace para todas as ocorrências
                finalQuery = finalQuery.replace(placeholder, replacement);
            } else {
                logger.trace("Nenhuma substituição realizada para placeholder: {}", placeholder);
            }
        }

        // Remove placeholders não substituídos que não deveriam estar na query final (ex: #VALOR_DESEJADO# no Template 2A)
        int placeholdersRemovidos = 0;
        String queryAntesDaRemocao = finalQuery;
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("#[A-Z_]+#"); // Encontra qualquer #PLACEHOLDER#
        java.util.regex.Matcher matcher = pattern.matcher(finalQuery);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String placeholderEncontrado = matcher.group();
            logger.warn("Removendo placeholder não substituído/utilizado: {}", placeholderEncontrado);
            placeholdersRemovidos++;
            matcher.appendReplacement(sb, ""); // Substitui por string vazia
        }
        matcher.appendTail(sb);
        finalQuery = sb.toString();

        if (placeholdersRemovidos > 0) {
            logger.warn("{} placeholders foram removidos da query final.", placeholdersRemovidos);
            logger.debug("Query antes da remoção:\n---\n{}\n---", queryAntesDaRemocao);
            logger.debug("Query após remoção:\n---\n{}\n---", finalQuery);
        }

        logger.info("Construção da query SPARQL finalizada.");
        return finalQuery;
    }
}