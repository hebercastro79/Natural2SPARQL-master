package com.example.Programa_heber.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "informacoes_empresas") // Verifique se o nome da tabela está correto
public class InformacoesEmpresasDb {

    @Id
    // Se o ID for gerado pelo banco (ex: auto_increment), descomente a linha abaixo
    // @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id") // Verifique se o nome da coluna ID está correto
    private Integer id;

    // Verifique os nomes das colunas abaixo para corresponderem exatamente ao seu banco de dados
    @Column(name = "Empresa_Capital_Aberto")
    private String empresaCapitalAberto;

    @Column(name = "Codigo_Negociacao")
    private String codigoNegociacao;

    @Column(name = "Setor_Atuacao") // Seria Setor_Atuacao1 do Excel
    private String setorAtuacao;

    @Column(name = "Setor_Atuacao2") // Seria Setor_Atuacao2 do Excel
    private String setorAtuacao2;

    @Column(name = "Setor_Atuacao3") // Seria Setor_Atuacao3 do Excel (importante para Template 3A)
    private String setorAtuacao3;

    // Construtores
    public InformacoesEmpresasDb() {
        // Construtor padrão JPA
    }

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

    @Override
    public String toString() {
        return "InformacoesEmpresasDb{" +
                "id=" + id +
                ", empresaCapitalAberto='" + empresaCapitalAberto + '\'' +
                ", codigoNegociacao='" + codigoNegociacao + '\'' +
                ", setorAtuacao='" + setorAtuacao + '\'' +
                ", setorAtuacao2='" + setorAtuacao2 + '\'' +
                ", setorAtuacao3='" + setorAtuacao3 + '\'' +
                '}';
    }
}