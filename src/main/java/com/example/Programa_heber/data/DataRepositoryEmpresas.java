package com.example.Programa_heber.data;

import com.example.Programa_heber.model.InformacoesEmpresasDb; // Importe a entidade
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface DataRepositoryEmpresas extends JpaRepository<InformacoesEmpresasDb, Integer> {
    List<InformacoesEmpresasDb> findAll(); // Método para buscar todos
}