package com.example.Programa_heber.data;

import com.example.Programa_heber.model.DadosNovosDb;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface DataRepositoryNovos extends JpaRepository<DadosNovosDb, Integer> {
    List<DadosNovosDb> findAll(); // Método para buscar todos
}