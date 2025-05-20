package com.example.Programa_heber.model;


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