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
import org.slf4j.Logger; // Importar o Logger SLF4J
import org.slf4j.LoggerFactory; // Importar o LoggerFactory

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

@SpringBootApplication
@RestController // Define esta classe como um Controller que retorna respostas no corpo HTTP
public class Main {

    // Logger para a classe Main
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private final ResourceLoader resourceLoader;
    private final QuestionProcessor questionProcessor; // O Serviço que contém a lógica

    // Injeção de dependências via construtor (boa prática)
    @Autowired
    public Main(ResourceLoader resourceLoader, QuestionProcessor questionProcessor) {
        this.resourceLoader = resourceLoader;
        this.questionProcessor = questionProcessor;
        logger.info("Classe Main inicializada com QuestionProcessor: {}",
                (this.questionProcessor != null ? "OK" : "FALHOU"));
    }

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    // Endpoint para servir a página HTML inicial
    @GetMapping("/")
    public ResponseEntity<String> index() {
        logger.debug("Requisição recebida para / (index)");
        try {
            org.springframework.core.io.Resource resource = resourceLoader.getResource("classpath:static/index2.html");
            if (!resource.exists()) {
                logger.error("Arquivo index2.html não encontrado em classpath:static/");
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Página inicial não encontrada.");
            }
            byte[] bytes = Files.readAllBytes(Paths.get(resource.getURI()));
            String htmlContent = new String(bytes, StandardCharsets.UTF_8);
            logger.debug("Servindo index2.html");
            return ResponseEntity.ok().body(htmlContent);
        } catch (IOException e) {
            logger.error("Erro ao ler o arquivo index2.html", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Erro ao carregar a página inicial.");
        }
    }

    // Endpoint para processar a pergunta vinda do frontend
    @PostMapping("/processar_pergunta")
    public ResponseEntity<RespostaReply> processarPergunta(@RequestBody PerguntaRequest request) {
        logger.info("Requisição POST recebida em /processar_pergunta com pergunta: '{}'", request.getPergunta());

        // Verificação defensiva (embora a injeção deva garantir que não seja nulo)
        if (questionProcessor == null) {
            logger.error("Erro crítico: questionProcessor é nulo dentro do método processarPergunta!");
            RespostaReply respostaErro = new RespostaReply();
            respostaErro.setResposta("Erro interno: Serviço de processamento não disponível.");
            return new ResponseEntity<>(respostaErro, HttpStatus.INTERNAL_SERVER_ERROR);
        }

        RespostaReply respostaReply = new RespostaReply();
        try {
            // Chama o método do *serviço* QuestionProcessor, passando um Map
            ResponseEntity<Map<String, Object>> serviceResponse = questionProcessor.processQuestion(Map.of("pergunta", request.getPergunta()));
            Map<String, Object> resultBody = serviceResponse.getBody();
            HttpStatus serviceStatus = (HttpStatus) serviceResponse.getStatusCode();

            logger.debug("Resposta do serviço QuestionProcessor: Status={}, Corpo={}", serviceStatus, resultBody);

            // Processa a resposta do serviço
            if (serviceStatus == HttpStatus.OK && resultBody != null && resultBody.containsKey("resposta")) {
                respostaReply.setResposta(String.valueOf(resultBody.get("resposta")));
                logger.info("Pergunta processada com sucesso. Resposta: '{}'", respostaReply.getResposta());
                return new ResponseEntity<>(respostaReply, HttpStatus.OK);
            } else if (resultBody != null && resultBody.containsKey("erro")) {
                // Se o serviço retornou um erro conhecido (ex: do Python)
                String erroMsg = String.valueOf(resultBody.get("erro"));
                respostaReply.setResposta("Erro no processamento: " + erroMsg);
                logger.warn("Erro retornado pelo serviço QuestionProcessor: {}", erroMsg);
                // Decide qual status retornar para o cliente (pode ser BAD_REQUEST ou INTERNAL_SERVER_ERROR dependendo do erro)
                return new ResponseEntity<>(respostaReply, serviceStatus != null ? serviceStatus : HttpStatus.INTERNAL_SERVER_ERROR);
            } else {
                // Caso genérico de erro ou resposta inesperada do serviço
                respostaReply.setResposta("Erro inesperado ao obter resposta do serviço.");
                logger.error("Resposta inesperada do serviço: Status={}, Corpo={}", serviceStatus, resultBody);
                return new ResponseEntity<>(respostaReply, HttpStatus.INTERNAL_SERVER_ERROR);
            }

        } catch (Exception e) {
            // Pega exceções inesperadas ocorridas durante a chamada ao serviço ou processamento aqui
            logger.error("Erro não tratado ao processar a pergunta na classe Main: {}", e.getMessage(), e);
            respostaReply.setResposta("Erro interno no servidor ao processar a pergunta.");
            return new ResponseEntity<>(respostaReply, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}