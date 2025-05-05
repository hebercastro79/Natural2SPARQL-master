package com.example.Programa_heber.model;

// Se usar Lombok, pode substituir os getters/setters por @Getter @Setter ou @Data
// import lombok.Getter;
// import lombok.Setter;

// @Getter
// @Setter
public class RespostaReply {

    private String resposta;
    private String erro; // Campo para mensagens de erro

    // --- Getters ---
    public String getResposta() {
        return resposta;
    }

    public String getErro() {
        return erro;
    }

    // --- Setters ---
    public void setResposta(String resposta) {
        this.resposta = resposta;
    }

    public void setErro(String erro) {
        this.erro = erro;
    }

    // --- Construtores ---
    public RespostaReply() {
        // Construtor padrão vazio
    }

    public RespostaReply(String resposta, String erro) {
        this.resposta = resposta;
        this.erro = erro;
    }
}