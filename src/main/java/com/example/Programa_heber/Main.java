package com.example.Programa_heber;

import com.example.Programa_heber.model.PerguntaRequest;
import com.example.Programa_heber.model.RespostaReply;
import com.example.Programa_heber.service.QuestionProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

@SpringBootApplication
@RestController
public class Main {

    private final ResourceLoader resourceLoader;
    private final QuestionProcessor questionProcessor;

    @Autowired // Injeção de dependência do QuestionProcessor
    public Main(ResourceLoader resourceLoader, QuestionProcessor questionProcessor) {
        this.resourceLoader = resourceLoader;
        this.questionProcessor = questionProcessor;
    }

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @GetMapping("/")
    public ResponseEntity<String> index() throws IOException {
        org.springframework.core.io.Resource resource = resourceLoader.getResource("classpath:static/index2.html");
        byte[] bytes = Files.readAllBytes(Paths.get(resource.getURI()));
        String htmlContent = new String(bytes, StandardCharsets.UTF_8);
        return ResponseEntity.ok().body(htmlContent);
    }

    @PostMapping("/processar_pergunta")
    public ResponseEntity<RespostaReply> processarPergunta(@RequestBody PerguntaRequest request) {
        if (questionProcessor == null) {
            System.err.println("Erro: questionProcessor é nulo!");
            RespostaReply respostaReply = new RespostaReply();
            respostaReply.setResposta("Erro: questionProcessor não foi inicializado corretamente.");
            return new ResponseEntity<>(respostaReply, HttpStatus.INTERNAL_SERVER_ERROR);
        }

        try {
            ResponseEntity<Map<String, Object>> response = questionProcessor.processQuestion(Map.of("pergunta", request.getPergunta()));
            Map<String, Object> result = response.getBody();

            RespostaReply respostaReply = new RespostaReply();
            if (result != null && result.containsKey("resposta")) {
                respostaReply.setResposta(String.valueOf(result.get("resposta")));
            } else {
                respostaReply.setResposta("Erro ao obter resposta.");
            }
            return new ResponseEntity<>(respostaReply, HttpStatus.OK);

        } catch (Exception e) {
            System.err.println("Erro ao processar a pergunta: " + e.getMessage());
            RespostaReply respostaReply = new RespostaReply();
            respostaReply.setResposta("Erro ao processar a pergunta. Por favor, tente novamente.");
            return new ResponseEntity<>(respostaReply, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}