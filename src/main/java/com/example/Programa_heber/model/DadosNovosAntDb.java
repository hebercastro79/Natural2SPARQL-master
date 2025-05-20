package com.example.Programa_heber.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity // Indica que esta classe é uma entidade JPA
@Table(name = "dados_novos_anterior") // Nome da tabela no banco de dados
public class DadosNovosAntDb {

    @Id // Indica que 'tipoRegistro' é a chave primária
    @Column(name = "tipo_registro")
    private Integer tipoRegistro;

    @Column(name = "data_pregao")
    private Integer dataPregao;

    @Column(name = "cod_bdi")
    private Integer codBdi;

    @Column(name = "cod_negociacao")
    private String codNegociacao;

    @Column(name = "tipo_mercado")
    private Integer tipoMercado;

    @Column(name = "nome_empresa")
    private String nomeEmpresa;

    @Column(name = "especificacao_papel")
    private String especificacaoPapel;

    @Column(name = "preco_abertura")
    private Double precoAbertura;

    @Column(name = "preco_maximo")
    private Double precoMaximo;

    @Column(name = "preco_minimo")
    private Double precoMinimo;

    @Column(name = "preco_medio")
    private Double precoMedio;

    @Column(name = "preco_ultimo_negocio")
    private Double precoUltimoNegocio;

    @Column(name = "numero_negocios")
    private Integer numeroNegocios;

    @Column(name = "quantidade_papeis_negociados")
    private Integer quantidadePapeisNegociados;

    @Column(name = "volume_total_negociado")
    private Long volumeTotalNegociado;

    @Column(name = "tipo_acao")
    private String tipoAcao;

    // Construtores (obrigatório para JPA)
    public DadosNovosAntDb() {
    }


    public Integer getTipoRegistro() {
        return tipoRegistro;
    }

    public void setTipoRegistro(Integer tipoRegistro) {
        this.tipoRegistro = tipoRegistro;
    }

    public Integer getDataPregao() {
        return dataPregao;
    }

    public void setDataPregao(Integer dataPregao) {
        this.dataPregao = dataPregao;
    }

    public Integer getCodBdi() {
        return codBdi;
    }

    public void setCodBdi(Integer codBdi) {
        this.codBdi = codBdi;
    }

    public String getCodNegociacao() {
        return codNegociacao;
    }

    public void setCodNegociacao(String codNegociacao) {
        this.codNegociacao = codNegociacao;
    }

    public Integer getTipoMercado() {
        return tipoMercado;
    }

    public void setTipoMercado(Integer tipoMercado) {
        this.tipoMercado = tipoMercado;
    }

    public String getNomeEmpresa() {
        return nomeEmpresa;
    }

    public void setNomeEmpresa(String nomeEmpresa) {
        this.nomeEmpresa = nomeEmpresa;
    }

    public String getEspecificacaoPapel() {
        return especificacaoPapel;
    }

    public void setEspecificacaoPapel(String especificacaoPapel) {
        this.especificacaoPapel = especificacaoPapel;
    }

    public Double getPrecoAbertura() {
        return precoAbertura;
    }

    public void setPrecoAbertura(Double precoAbertura) {
        this.precoAbertura = precoAbertura;
    }

    public Double getPrecoMaximo() {
        return precoMaximo;
    }

    public void setPrecoMaximo(Double precoMaximo) {
        this.precoMaximo = precoMaximo;
    }

    public Double getPrecoMinimo() {
        return precoMinimo;
    }

    public void setPrecoMinimo(Double precoMinimo) {
        this.precoMinimo = precoMinimo;
    }

    public Double getPrecoMedio() {
        return precoMedio;
    }

    public void setPrecoMedio(Double precoMedio) {
        this.precoMedio = precoMedio;
    }

    public Double getPrecoUltimoNegocio() {
        return precoUltimoNegocio;
    }

    public void setPrecoUltimoNegocio(Double precoUltimoNegocio) {
        this.precoUltimoNegocio = precoUltimoNegocio;
    }

    public Integer getNumeroNegocios() {
        return numeroNegocios;
    }

    public void setNumeroNegocios(Integer numeroNegocios) {
        this.numeroNegocios = numeroNegocios;
    }

    public Integer getQuantidadePapeisNegociados() {
        return quantidadePapeisNegociados;
    }

    public void setQuantidadePapeisNegociados(Integer quantidadePapeisNegociados) {
        this.quantidadePapeisNegociados = quantidadePapeisNegociados;
    }

    public Long getVolumeTotalNegociado() {
        return volumeTotalNegociado;
    }

    public void setVolumeTotalNegociado(Long volumeTotalNegociado) {
        this.volumeTotalNegociado = volumeTotalNegociado;
    }

    public String getTipoAcao() {
        return tipoAcao;
    }

    public void setTipoAcao(String tipoAcao) {
        this.tipoAcao = tipoAcao;
    }
}