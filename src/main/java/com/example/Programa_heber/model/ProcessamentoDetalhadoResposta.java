package com.example.Programa_heber.model;

public class ProcessamentoDetalhadoResposta {
    private String resposta;
    private String erro;
    private String sparqlQuery;

    // Construtores
    public ProcessamentoDetalhadoResposta() {}

    public ProcessamentoDetalhadoResposta(String resposta, String erro, String sparqlQuery) {
        this.resposta = resposta;
        this.erro = erro;
        this.sparqlQuery = sparqlQuery;
    }

    // Getters e Setters
    public String getResposta() {
        return resposta;
    }

    public void setResposta(String resposta) {
        this.resposta = resposta;
    }

    public String getErro() {
        return erro;
    }

    public void setErro(String erro) {
        this.erro = erro;
    }

    public String getSparqlQuery() {
        return sparqlQuery;
    }

    public void setSparqlQuery(String sparqlQuery) {
        this.sparqlQuery = sparqlQuery;
    }

    // Opcional: Sobrescrever toString() para logging mais fácil
    @Override
    public String toString() {
        return "ProcessamentoDetalhadoResposta{" +
                "resposta='" + resposta + '\'' +
                ", erro='" + erro + '\'' +
                ", sparqlQuery='" + (sparqlQuery != null ? sparqlQuery.substring(0, Math.min(sparqlQuery.length(), 100)) + "..." : "null") + '\'' + // Evita logar queries muito longas
                '}';
    }
}