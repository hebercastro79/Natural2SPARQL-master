package com.example.Programa_heber.data;

import com.example.Programa_heber.model.DadosNovosAntDb;
import com.example.Programa_heber.model.DadosNovosDb;
import com.example.Programa_heber.model.InformacoesEmpresasDb;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

// Repositório para DadosNovosAntDb
@Repository
public interface DataRepository extends JpaRepository<DadosNovosAntDb, Integer> {
    // Spring Data JPA fornece implementações automáticas para métodos CRUD básicos
    List<DadosNovosAntDb> findAll(); // Exemplo de método que você pode usar
}

// Repositório para DadosNovosDb
@Repository
public interface DataRepositoryNovos extends JpaRepository<DadosNovosDb, Integer> {
    List<DadosNovosDb> findAll();
}

// Repositório para InformacoesEmpresasDb
@Repository
public interface DataRepositoryEmpresas extends JpaRepository<InformacoesEmpresasDb, Integer> {
    List<InformacoesEmpresasDb> findAll();
}package com.example.Programa_heber.data;

import com.example.Programa_heber.model.DadosNovosAntDb;
import com.example.Programa_heber.model.DadosNovosDb;
import com.example.Programa_heber.model.InformacoesEmpresasDb;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

// Interface para DadosNovosAntDb
@Repository
public interface DataRepository extends JpaRepository<DadosNovosAntDb, Integer> {
    // O Spring Data JPA fornece implementações padrão para operações CRUD
    // Você *não* precisa implementar os métodos, a menos que precise de consultas customizadas
    List<DadosNovosAntDb> findAll(); // Equivalente ao seu getAllDadosNovosAnt()
}

// Interface para DadosNovosDb
@Repository
public interface DataRepositoryNovos extends JpaRepository<DadosNovosDb, Integer> {
     List<DadosNovosDb> findAll(); //Equivalente ao getAllDadosNovos
}

// Interface para InformacoesEmpresasDb
@Repository
public interface DataRepositoryEmpresas extends JpaRepository<InformacoesEmpresasDb, Integer>{
    List<InformacoesEmpresasDb> findAll(); //Equivalente ao getAllInformacoesEmpresas
}