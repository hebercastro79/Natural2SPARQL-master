package com.example.Programa_heber.model;

import com.fasterxml.jackson.annotation.JsonInclude; // Opcional, para omitir nulos no JSON de resposta

@JsonInclude(JsonInclude.Include.NON_NULL) // Opcional: não serializa campos nulos para JSON
public class ProcessamentoDetalhadoResposta {

    private String sparqlQuery;
    private String resposta;
    private String erro;
    private String debugInfo; // Campo para informações de debug do script Python

    // Construtores
    public ProcessamentoDetalhadoResposta() {
        // Construtor padrão
    }

    // Getters e Setters
    public String getSparqlQuery() {
        return sparqlQuery;
    }

    public void setSparqlQuery(String sparqlQuery) {
        this.sparqlQuery = sparqlQuery;
    }

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

    public String getDebugInfo() {
        return debugInfo;
    }

    public void setDebugInfo(String debugInfo) {
        this.debugInfo = debugInfo;
    }

    @Override
    public String toString() {
        // Evita quebras de linha excessivas no log, útil para sparqlQuery e debugInfo
        String queryNoNewline = (sparqlQuery != null) ? sparqlQuery.replace("\n", " ").replace("\r", " ") : null;
        String debugNoNewline = (debugInfo != null) ? debugInfo.replace("\n", " ").replace("\r", " ") : null;

        return "ProcessamentoDetalhadoResposta{" +
                "sparqlQuery='" + queryNoNewline + '\'' +
                ", resposta='" + resposta + '\'' +
                ", erro='" + erro + '\'' +
                ", debugInfo='" + debugNoNewline + '\'' +
                '}';
    }
}