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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

@SpringBootApplication
@RestController
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private final QuestionProcessor questionProcessor;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @Autowired
    public Main(QuestionProcessor questionProcessor) {
        this.questionProcessor = questionProcessor;
        if (this.questionProcessor != null) {
            logger.info("Classe Main inicializada com QuestionProcessor: OK");
        } else {
            logger.error("!!!!!!!!!! Classe Main inicializada com QuestionProcessor NULO !!!!!!!!!!");
        }
    }

    @GetMapping(value = "/", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> index() {
        logger.debug("Requisição recebida para / (index)");
        try {
            ClassPathResource resource = new ClassPathResource("static/index2.html");
            if (!resource.exists()) {
                logger.error("Arquivo index2.html não encontrado em static/");
                resource = new ClassPathResource("index2.html");
                if (!resource.exists()){
                    logger.error("Arquivo index2.html também não encontrado na raiz do resources/");
                    return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Página inicial não encontrada.");
                }
                logger.warn("Arquivo index2.html encontrado na raiz do resources, mas deveria estar em /static.");
            }

            String htmlContent;
            try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
                htmlContent = FileCopyUtils.copyToString(reader);
            }
            logger.debug("Servindo index2.html");
            return ResponseEntity.ok(htmlContent);
        } catch (IOException e) {
            logger.error("Erro ao ler o arquivo index2.html", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Erro ao carregar a página inicial.");
        }
    }

    @PostMapping("/processar_pergunta")
    public ResponseEntity<RespostaReply> processarPergunta(@RequestBody PerguntaRequest request) {
        if (questionProcessor == null) {
            logger.error("QuestionProcessor não foi injetado corretamente!");
            RespostaReply errorReply = new RespostaReply();
            errorReply.setErro("Erro interno crítico: Serviço de processamento indisponível.");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorReply);
        }

        if (request == null || request.getPergunta() == null || request.getPergunta().trim().isEmpty()) {
            logger.warn("Requisição POST recebida sem pergunta válida.");
            RespostaReply errorReply = new RespostaReply();
            errorReply.setErro("Nenhuma pergunta fornecida.");
            return ResponseEntity.badRequest().body(errorReply);
        }

        logger.info("Requisição POST recebida em /processar_pergunta com pergunta: '{}'", request.getPergunta());
        String resultado = questionProcessor.processQuestion(request.getPergunta());
        RespostaReply reply = new RespostaReply();

        if (resultado == null || resultado.trim().isEmpty()) {
            logger.error("QuestionProcessor retornou resultado nulo ou vazio para a pergunta: {}", request.getPergunta());
            reply.setErro("Erro interno ao processar a pergunta.");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(reply);
        } else if (resultado.startsWith("Erro")) {
            logger.warn("Erro retornado pelo serviço QuestionProcessor: {}", resultado);
            reply.setErro(resultado);
            // Ajusta o status baseado na mensagem de erro
            HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR; // Padrão para erros
            if (resultado.contains("Não foi possível montar consulta") || resultado.contains("identificar o que buscar")) {
                status = HttpStatus.INTERNAL_SERVER_ERROR;
            } else if (resultado.contains("Falha ao processar a pergunta com o script") || resultado.contains("comunicar com o processador")) {
                status = HttpStatus.SERVICE_UNAVAILABLE; // Ou INTERNAL_SERVER_ERROR
            }
            return ResponseEntity.status(status).body(reply);
        } else if (resultado.equals("Não foram encontrados resultados.")) {
            logger.info("Nenhum resultado encontrado para a pergunta.");
            reply.setResposta(resultado);
            return ResponseEntity.ok(reply);
        } else {
            logger.info("Pergunta processada com sucesso. Resposta: '{}'", resultado);
            reply.setResposta(resultado);
            return ResponseEntity.ok(reply);
        }
    }
}