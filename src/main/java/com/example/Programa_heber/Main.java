package com.example.Programa_heber;

import com.example.Programa_heber.model.PerguntaRequest;
// Importa a nova classe de resposta detalhada
import com.example.Programa_heber.model.ProcessamentoDetalhadoResposta;
import com.example.Programa_heber.service.QuestionProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

@SpringBootApplication
@RestController
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private final QuestionProcessor questionProcessor;

    @Autowired
    public Main(QuestionProcessor questionProcessor) {
        this.questionProcessor = questionProcessor;
        if (this.questionProcessor != null) {
            logger.info("Classe Main (Controller) inicializada com QuestionProcessor: OK");
        } else {
            logger.error("!!!!!!!!!! CRÍTICO: QuestionProcessor não foi injetado via construtor !!!!!!!!!!");
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
        logger.info("Aplicação Natural2SPARQL iniciada.");
    }

    @GetMapping(value = "/", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> index() {
        logger.debug("Requisição recebida para / (index)");
        try {
            ClassPathResource resource = new ClassPathResource("static/index2.html");
            if (!resource.exists()) {
                logger.warn("Arquivo index2.html não encontrado em static/, tentando na raiz do classpath...");
                resource = new ClassPathResource("index2.html");
                if (!resource.exists()) {
                    logger.error("Arquivo index2.html não encontrado na raiz do classpath também.");
                    return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Página inicial não encontrada (index2.html).");
                }
            }

            String htmlContent;
            try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
                htmlContent = FileCopyUtils.copyToString(reader);
            }
            logger.debug("Servindo index2.html de {}", resource.getPath());
            return ResponseEntity.ok(htmlContent);
        } catch (IOException e) {
            logger.error("Erro ao ler o arquivo index2.html", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Erro ao carregar a página inicial.");
        }
    }


    @PostMapping("/processar_pergunta")
    // Muda o tipo de retorno para ResponseEntity<ProcessamentoDetalhadoResposta>
    public ResponseEntity<ProcessamentoDetalhadoResposta> processarPergunta(@RequestBody PerguntaRequest request) {
        if (request == null || request.getPergunta() == null || request.getPergunta().trim().isEmpty()) {
            logger.warn("Requisição POST recebida sem pergunta válida no corpo.");
            ProcessamentoDetalhadoResposta errorReply = new ProcessamentoDetalhadoResposta();
            errorReply.setErro("Nenhuma pergunta fornecida.");
            errorReply.setSparqlQuery("N/A - Nenhuma pergunta fornecida.");
            return ResponseEntity.badRequest().body(errorReply);
        }

        if (questionProcessor == null) {
            logger.error("CRÍTICO: QuestionProcessor é nulo no momento da requisição!");
            ProcessamentoDetalhadoResposta errorReply = new ProcessamentoDetalhadoResposta();
            errorReply.setErro("Erro interno crítico: Serviço de processamento indisponível.");
            errorReply.setSparqlQuery("N/A - Serviço indisponível.");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorReply);
        }

        logger.info("Requisição POST recebida em /processar_pergunta com pergunta: '{}'", request.getPergunta());

        // Chama o método que agora retorna ProcessamentoDetalhadoResposta
        ProcessamentoDetalhadoResposta respostaDetalhada = questionProcessor.processQuestion(request.getPergunta());
        HttpStatus status = HttpStatus.OK; // Status padrão para sucesso

        // Verifica se houve erro no processamento vindo do QuestionProcessor
        if (respostaDetalhada.getErro() != null) {
            logger.warn("Erro retornado pelo serviço QuestionProcessor: {}", respostaDetalhada.getErro());
            // Determina o status HTTP baseado no erro
            status = HttpStatus.INTERNAL_SERVER_ERROR; // Padrão
            String erroMsg = respostaDetalhada.getErro().toLowerCase(); // Para facilitar a verificação

            if (erroMsg.contains("falha ao interpretar") ||
                    erroMsg.contains("não foi possível determinar o tipo") ||
                    erroMsg.contains("falha ao obter os detalhes") ||
                    erroMsg.contains("template sparql não encontrado") ||
                    erroMsg.contains("informação faltando")) {
                status = HttpStatus.BAD_REQUEST; // Erro de input do usuário ou configuração de template
            } else if (erroMsg.contains("erro na execução do script python") ||
                    erroMsg.contains("comunicar com o processador") ||
                    erroMsg.contains("script python não encontrado") ||
                    erroMsg.contains("falha crítica ao inicializar")) {
                status = HttpStatus.SERVICE_UNAVAILABLE; // Problema com o serviço Python
            } else if (erroMsg.contains("erro ao executar a consulta sparql")){
                // Mantém INTERNAL_SERVER_ERROR ou pode ser específico se a causa for mais detalhada
            }
        } else if (respostaDetalhada.getResposta() == null || respostaDetalhada.getResposta().equals("Não foram encontrados resultados que correspondam à sua pergunta.")) {
            logger.info("Nenhum resultado encontrado para a pergunta ou resposta padrão de 'não encontrado'.");
            // Status OK, mas a resposta indica que não há dados.
        } else {
            logger.info("Pergunta processada com sucesso.");
        }

        return ResponseEntity.status(status).body(respostaDetalhada);
    }
}