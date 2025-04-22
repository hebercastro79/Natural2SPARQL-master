package com.example.Programa_heber.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "informacoes_empresas") // Nome da tabela
public class InformacoesEmpresasDb {

    @Id
    @Column(name = "id") // Nome da coluna
    private Integer id;

    @Column(name = "Empresa_Capital_Aberto")
    private String empresaCapitalAberto;

    @Column(name = "Codigo_Negociacao")
    private String codigoNegociacao;

    @Column(name = "Setor_Atuacao")
    private String setorAtuacao;

    @Column(name = "Setor_Atuacao2")
    private String setorAtuacao2;

    @Column(name = "Setor_Atuacao3")
    private String setorAtuacao3;

    // Construtores
    public InformacoesEmpresasDb() {}

    //Getters e Setters
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getEmpresaCapitalAberto() {
        return empresaCapitalAberto;
    }

    public void setEmpresaCapitalAberto(String empresaCapitalAberto) {
        this.empresaCapitalAberto = empresaCapitalAberto;
    }

    public String getCodigoNegociacao() {
        return codigoNegociacao;
    }

    public void setCodigoNegociacao(String codigoNegociacao) {
        this.codigoNegociacao = codigoNegociacao;
    }

    public String getSetorAtuacao() {
        return setorAtuacao;
    }

    public void setSetorAtuacao(String setorAtuacao) {
        this.setorAtuacao = setorAtuacao;
    }

    public String getSetorAtuacao2() {
        return setorAtuacao2;
    }

    public void setSetorAtuacao2(String setorAtuacao2) {
        this.setorAtuacao2 = setorAtuacao2;
    }

    public String getSetorAtuacao3() {
        return setorAtuacao3;
    }

    public void setSetorAtuacao3(String setorAtuacao3) {
        this.setorAtuacao3 = setorAtuacao3;
    }
}