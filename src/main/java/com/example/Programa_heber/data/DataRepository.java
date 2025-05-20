package com.example.Programa_heber.data;

import com.example.Programa_heber.model.DadosNovosAntDb;
import com.example.Programa_heber.model.DadosNovosDb;
import com.example.Programa_heber.model.InformacoesEmpresasDb;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.List;
import java.util.ArrayList;

@Repository
public class DataRepository {

    private Connection connection;

    private final String DB_URL = "jdbc:mysql://localhost:3306/bd_sparql"; //BANCO de DADOS
    private final String DB_USER = "heber";       //USUARIO
    private final String DB_PASSWORD = "G@b1p3p3"; //SENHA

    public DataRepository() {
        try {
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to connect to the database.", e);
        }
    }

    public List<DadosNovosAntDb> getAllDadosNovosAnt() {
        List<DadosNovosAntDb> dados = new ArrayList<>();
        String sql = "SELECT * FROM dados_novos_anterior"; // Consulta SQL

        try (PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                DadosNovosAntDb dado = new DadosNovosAntDb();


                dado.setTipoRegistro(rs.getObject("tipo_registro") != null ? rs.getInt("tipo_registro") : null);
                dado.setDataPregao(rs.getObject("data_pregao") != null ? rs.getInt("data_pregao") : null);
                dado.setCodBdi(rs.getObject("cod_bdi") != null ? rs.getInt("cod_bdi") : null);
                dado.setCodNegociacao(rs.getString("cod_negociacao")); // getString é ok para VARCHAR
                dado.setTipoMercado(rs.getObject("tipo_mercado") != null ? rs.getInt("tipo_mercado") : null);
                dado.setNomeEmpresa(rs.getString("nome_empresa"));
                dado.setEspecificacaoPapel(rs.getString("especificacao_papel"));
                dado.setPrecoAbertura(rs.getObject("preco_abertura") != null ? rs.getDouble("preco_abertura") : null);
                dado.setPrecoMaximo(rs.getObject("preco_maximo") != null ? rs.getDouble("preco_maximo") : null);
                dado.setPrecoMinimo(rs.getObject("preco_minimo") != null ? rs.getDouble("preco_minimo") : null);
                dado.setPrecoMedio(rs.getObject("preco_medio") != null ? rs.getDouble("preco_medio") : null);
                dado.setPrecoUltimoNegocio(rs.getObject("preco_ultimo_negocio") != null ? rs.getDouble("preco_ultimo_negocio") : null);
                dado.setNumeroNegocios(rs.getObject("numero_negocios") != null ? rs.getInt("numero_negocios") : null);
                dado.setQuantidadePapeisNegociados(rs.getObject("quantidade_papeis_negociados") != null ? rs.getInt("quantidade_papeis_negociados") : null);
                dado.setVolumeTotalNegociado(rs.getObject("volume_total_negociado") != null ? rs.getLong("volume_total_negociado") : null);
                dado.setTipoAcao(rs.getString("tipo_acao"));

                dados.add(dado);
            }

        } catch (SQLException e) {

            System.err.println("Erro ao buscar dados de dados_novos_anterior: " + e.getMessage());
            e.printStackTrace(); // Imprime o stack trace completo

            throw new RuntimeException("Erro ao buscar dados de dados_novos_anterior", e);
        }
        return dados;
    }

    public List<DadosNovosDb> getAllDadosNovos() {
        List<DadosNovosDb> dados = new ArrayList<>();
        String sql = "SELECT * FROM dados_novos_atual";

        try (PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                DadosNovosDb dado = new DadosNovosDb();


                dado.setTipoRegistro(rs.getObject("tipo_registro") != null ? rs.getInt("tipo_registro") : null);
                //dado.setDataPregao(rs.getObject("data_pregao") != null ? rs.getInt("data_pregao") : null);
                dado.setDataPregao(rs.getObject("data_pregao") != null ? rs.getInt("data_pregao") : null);
                dado.setCodBdi(rs.getObject("cod_bdi") != null ? rs.getInt("cod_bdi") : null);
                dado.setCodNegociacao(rs.getString("cod_negociacao"));
                //dado.setTicker(rs.getString("cod_negociacao")); //Preenchido para teste, remova se não quiser
                dado.setTipoMercado(rs.getObject("tipo_mercado") != null ? rs.getInt("tipo_mercado") : null);
                dado.setNomeEmpresa(rs.getString("nome_empresa"));
                //dado.setEmpresas(rs.getString("nome_empresa"));
                dado.setEspecificacaoPapel(rs.getString("especificacao_papel"));
                dado.setPrecoAbertura(rs.getObject("preco_abertura") != null ? rs.getDouble("preco_abertura") : null);
                dado.setPrecoMaximo(rs.getObject("preco_maximo") != null ? rs.getDouble("preco_maximo") : null);
                dado.setPrecoMinimo(rs.getObject("preco_minimo") != null ? rs.getDouble("preco_minimo") : null);
                dado.setPrecoMedio(rs.getObject("preco_medio") != null ? rs.getDouble("preco_medio") : null);
                dado.setPrecoUltimoNegocio(rs.getObject("preco_ultimo_negocio") != null ? rs.getDouble("preco_ultimo_negocio") : null);
                dado.setNumeroNegocios(rs.getObject("numero_negocios") != null ? rs.getInt("numero_negocios") : null);
                dado.setQuantidadePapeisNegociados(rs.getObject("quantidade_papeis_negociados") != null ? rs.getInt("quantidade_papeis_negociados") : null);
                dado.setVolumeTotalNegociado(rs.getObject("volume_total_negociado") != null ? rs.getLong("volume_total_negociado") : null);
                dado.setTipoAcao(rs.getString("tipo_acao"));
                dados.add(dado);
            }
        } catch (SQLException e) {
            System.err.println("Erro ao buscar dados de dados_novos_atual: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Erro ao buscar dados de dados_novos_atual", e);
        }
        return dados;
    }

    public List<InformacoesEmpresasDb> getAllInformacoesEmpresas() {
        List<InformacoesEmpresasDb> dados = new ArrayList<>();
        String sql = "SELECT * FROM Informacoes_Empresas";

        try (PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                InformacoesEmpresasDb dado = new InformacoesEmpresasDb();


                dado.setId(rs.getObject("id") != null ? rs.getInt("id") : null); // Trata NULL
                dado.setEmpresaCapitalAberto(rs.getString("Empresa_Capital_Aberto")); // Nomes corretos
                dado.setCodigoNegociacao(rs.getString("Codigo_Negociacao"));
                dado.setSetorAtuacao(rs.getString("Setor_Atuacao"));
                dado.setSetorAtuacao2(rs.getString("Setor_Atuacao2"));
                dado.setSetorAtuacao3(rs.getString("Setor_Atuacao3"));

                dados.add(dado);
            }
        } catch (SQLException e) {
            System.err.println("Erro ao buscar dados de Informacoes_Empresas: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Erro ao buscar dados de Informacoes_Empresas", e);
        }

        return dados;
    }


    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}