# Natural2SPARQL

<!-- Uma breve descrição (1-2 frases) do que o projeto faz. -->
Um framework Java para converter perguntas e afirmações em linguagem natural (Português) para consultas SPARQL, utilizando Processamento de Linguagem Natural e Ontologias OWL.

<!-- Opcional: Badges/Escudos (ex: status do build, versão, licença) -->
<!-- ![License](https://img.shields.io/badge/License-MIT-blue.svg) -->

## 📝 Sobre o Projeto

<!-- Explique um pouco mais o objetivo, o problema que resolve, e talvez o contexto. -->
Este projeto visa facilitar o acesso a dados armazenados em grafos de conhecimento (Knowledge Graphs) RDF por meio de perguntas feitas em linguagem natural. Ele utiliza técnicas de PLN com Stanford CoreNLP para analisar a entrada do usuário e a biblioteca OWL API junto com Apache Jena para interpretar uma ontologia de domínio e gerar a consulta SPARQL correspondente.

<!-- Exemplo: Mencione o domínio específico se houver -->
<!-- Atualmente, o foco é em perguntas sobre [Mencione o domínio da sua ontologia, ex: filmes, livros, dados acadêmicos]. -->

## ✨ Funcionalidades Principais

*   Análise sintática e semântica de frases em Português (usando Stanford CoreNLP).
*   Reconhecimento de Entidades Nomeadas (NER) relevantes para a ontologia.
*   Mapeamento de termos da linguagem natural para conceitos e propriedades da ontologia OWL.
*   Geração de consultas SPARQL (SELECT, ASK, etc.) baseadas na pergunta e na ontologia.
*   

## 🚀 Tecnologias Utilizadas

*   [Java](https://www.java.com/) - Linguagem de programação principal (<!-- Especifique a versão do JDK, ex: JDK 11 -->)
*   [Maven](https://maven.apache.org/) - Gerenciamento de dependências e build
*   [Apache Jena](https://jena.apache.org/) - Framework para manipulação de RDF, SPARQL e ontologias
*   [Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP/) - Biblioteca para Processamento de Linguagem Natural
*   [OWL API](https://owlapi.sourceforge.net/) - API para manipulação de ontologias OWL
*   [JUnit](https://junit.org/junit5/) - Framework para testes unitários

## ⚙️ Configuração e Instalação

<!-- Instruções passo a passo para que alguém possa rodar seu projeto. -->

### Pré-requisitos

*   **Java Development Kit (JDK):** Versão <!-- Ex: 11 --> ou superior instalada e configurada (variável `JAVA_HOME`).
*   **Apache Maven:** Instalado e configurado (comando `mvn` disponível no terminal).
*   **Ontologia:** Um arquivo de ontologia no formato OWL (`.owl`) é necessário. <!-- Especifique o nome esperado ou onde colocá-lo, ex: `src/main/resources/sua_ontologia.owl` -->.
*   **Modelos Stanford CoreNLP:** Pode ser necessário baixar os modelos para o idioma Português separadamente. Verifique a documentação do CoreNLP. <!-- Adicione instruções se houver passos específicos para o seu projeto -->

### Instalação

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/hebercastro79/Natural2SPARQL.git
    ```
2.  **Navegue até o diretório do projeto:**
    ```bash
    cd Natural2SPARQL
    ```
3.  **Instale as dependências com Maven:**
    ```bash
    mvn clean install
    ```
    *   **Observação:** Atualmente, algumas dependências podem estar sendo gerenciadas via `<scope>system</scope>` e a pasta `lib/`. O ideal é migrá-las para o gerenciamento padrão do Maven. Por enquanto, certifique-se de que a pasta `lib/` foi clonada corretamente.


## ▶️ Como Usar

