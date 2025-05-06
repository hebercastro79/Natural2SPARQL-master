package com.example.Programa_heber;

import com.example.Programa_heber.model.PerguntaRequest;
import com.example.Programa_heber.model.RespostaReply;
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

import java.io.IOException; // Ainda necessário para o try-catch do index()
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
        } catch (IOException e) { // try-catch necessário para File I/O aqui
            logger.error("Erro ao ler o arquivo index2.html", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Erro ao carregar a página inicial.");
        }
    }


    @PostMapping("/processar_pergunta")
    public ResponseEntity<RespostaReply> processarPergunta(@RequestBody PerguntaRequest request) {
        if (request == null || request.getPergunta() == null || request.getPergunta().trim().isEmpty()) {
            logger.warn("Requisição POST recebida sem pergunta válida no corpo.");
            RespostaReply errorReply = new RespostaReply();
            errorReply.setErro("Nenhuma pergunta fornecida.");
            return ResponseEntity.badRequest().body(errorReply);
        }

        if (questionProcessor == null) {
            logger.error("CRÍTICO: QuestionProcessor é nulo no momento da requisição!");
            RespostaReply errorReply = new RespostaReply();
            errorReply.setErro("Erro interno crítico: Serviço de processamento indisponível.");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorReply);
        }

        logger.info("Requisição POST recebida em /processar_pergunta com pergunta: '{}'", request.getPergunta());
        String resultado;
        RespostaReply reply = new RespostaReply();
        HttpStatus status = HttpStatus.OK;

        try {
            // A chamada a processQuestion agora não declara checked exceptions
            resultado = questionProcessor.processQuestion(request.getPergunta());

            // Trata a string de resultado retornada
            if (resultado == null || resultado.trim().isEmpty()) {
                logger.error("QuestionProcessor retornou resultado nulo ou vazio para a pergunta: {}", request.getPergunta());
                reply.setErro("Erro interno ao processar a pergunta (resultado vazio).");
                status = HttpStatus.INTERNAL_SERVER_ERROR;
            } else if (resultado.startsWith("Erro:")) { // Checa se o resultado é uma string de erro
                logger.warn("Erro retornado pelo serviço QuestionProcessor: {}", resultado);
                reply.setErro(resultado);
                status = HttpStatus.INTERNAL_SERVER_ERROR; // Padrão para erros lógicos
                if (resultado.contains("Falha ao processar a pergunta com o script") || resultado.contains("comunicar com o processador") || resultado.contains("Script Python não encontrado")) {
                    status = HttpStatus.SERVICE_UNAVAILABLE;
                } else if (resultado.contains("Template não encontrado")) {
                    status = HttpStatus.NOT_IMPLEMENTED; // Ou INTERNAL_SERVER_ERROR
                }
            } else if (resultado.equals("Não foram encontrados resultados que correspondam à sua pergunta.")) {
                logger.info("Nenhum resultado encontrado para a pergunta.");
                reply.setResposta(resultado);
            } else {
                logger.info("Pergunta processada com sucesso. Resposta: '{}'", resultado);
                reply.setResposta(resultado);
            }

        } catch (RuntimeException e) { // Captura erros não verificados (ex: erro no Python, JSON)
            logger.error("Erro inesperado (RuntimeException) ao processar pergunta '{}': {}", request.getPergunta(), e.getMessage(), e);
            reply.setErro("Erro inesperado no servidor: " + e.getMessage());
            status = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        // Não precisa mais de catch para IOException ou InterruptedException aqui

        return ResponseEntity.status(status).body(reply);
    }
}