package com.example.Programa_heber.ontology;

import jakarta.annotation.PostConstruct; // Usando jakarta para Spring Boot 3+
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shared.JenaException;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors; // Import necessário para Collectors.joining

@Component
public class Ontology {

    private static final Logger logger = LoggerFactory.getLogger(Ontology.class);

    private Model baseModel;
    private InfModel infModel;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // Prefixo da sua ontologia - ajuste se necessário
    private static final String ONT_PREFIX = "https://dcm.ffclrp.usp.br/lssb/stock-market-ontology#";

    // Arquivos de dados (esperados em src/main/resources)
    private static final String[] PREGAO_FILES = {
            "/dados_novos_anterior.xlsx", // Planilha antiga
            "/dados_novos_atual.xlsx"     // Planilha mais recente
    };
    private static final String SCHEMA_FILE = "/stock_market.owl"; // Schema OWL
    private static final String BASE_DATA_FILE = "/ontologiaB3.ttl"; // Dados base TTL (empresas, etc.)
    // private static final String RULES_FILE = "/regras.rules"; // Arquivo de regras (opcional, não usado atualmente)

    // Arquivo de saída para o modelo inferido (será salvo na raiz do projeto)
    private static final String INFERENCE_OUTPUT_FILE = "ontologiaB3_com_inferencia.ttl";

    @PostConstruct
    public void init() {
        logger.info(">>> INICIANDO Inicialização Ontology (@PostConstruct)...");
        lock.writeLock().lock(); // Bloqueio de escrita para inicialização segura
        try {
            // Cria o modelo base RDF
            baseModel = ModelFactory.createDefaultModel();
            // Define os prefixos de namespace para facilitar a leitura/escrita do RDF
            baseModel.setNsPrefix("ont", ONT_PREFIX); // Prefixo da sua ontologia
            baseModel.setNsPrefix("rdf", RDF.uri);
            baseModel.setNsPrefix("rdfs", RDFS.uri);
            baseModel.setNsPrefix("owl", OWL.NS);
            baseModel.setNsPrefix("xsd", XSDDatatype.XSD + "#"); // Prefixo para tipos de dados XSD
            logger.info("   Modelo base criado e prefixos definidos.");

            // 1. Carregar Schema OWL (estrutura da ontologia)
            loadRdfData(SCHEMA_FILE, Lang.RDFXML, "Schema OWL");

            // 2. Carregar Dados Base TTL (instâncias iniciais, como empresas, se houver)
            loadRdfData(BASE_DATA_FILE, Lang.TURTLE, "Dados base TTL");

            // 3. Carregar Dados Dinâmicos das Planilhas de Pregão
            logger.info("--- Iniciando carregamento planilhas de pregão ---");
            for (String filePath : PREGAO_FILES) {
                // Chama o método corrigido para carregar dados do Excel
                loadExcelData(filePath);
            }
            logger.info("--- Carregamento planilhas pregão concluído ---");

            long baseSizeBeforeInfer = baseModel.size();
            logger.info("Total triplas BASE (pós-load): {}", baseSizeBeforeInfer);

            // Validações pós-carga
            validateBaseModelLoad(baseSizeBeforeInfer);

            // 4. Configurar e Aplicar Raciocinador (Inferência)
            logger.info("--- Configurando Reasoner ---");
            Reasoner reasoner = getReasoner(); // Obtém o raciocinador (atualmente RDFS)
            logger.info("--- Criando modelo inferência ---");
            infModel = ModelFactory.createInfModel(reasoner, baseModel); // Cria o modelo com inferência
            long infSize = infModel.size();
            long inferredCount = infSize - baseSizeBeforeInfer;
            logger.info("--- Modelo inferência criado. Base:{}, Inferidas:{}, Total:{} ---",
                    baseSizeBeforeInfer, (inferredCount < 0 ? 0 : inferredCount), infSize); // Garante que inferidas não seja negativo

            // 5. Salvar modelo inferido (opcional, útil para depuração)
            saveInferredModel();

            logger.info("<<< Ontology INICIALIZADA COM SUCESSO >>>");

        } catch (Exception e) {
            logger.error("!!!!!!!! FALHA GRAVE NA INICIALIZAÇÃO DA ONTOLOGY !!!!!!!!", e);
            // Garante que os modelos fiquem nulos em caso de falha grave
            baseModel = null;
            infModel = null;
        } finally {
            lock.writeLock().unlock(); // Libera o bloqueio de escrita
        }
    }

    /**
     * Carrega dados RDF de um arquivo no classpath para o modelo base.
     * Inclui tratamento de erros mais detalhado.
     */
    private void loadRdfData(String resourcePath, Lang language, String description) {
        String cleanPath = resourcePath.startsWith("/") ? resourcePath : "/" + resourcePath;
        logger.info("   Tentando carregar {} de classpath: {}", description, cleanPath);
        InputStream in = null;
        try {
            in = Ontology.class.getResourceAsStream(cleanPath);
            if (in == null) {
                logger.error("   !!!!!!!! ARQUIVO ESSENCIAL '{}' ({}) NÃO ENCONTRADO no classpath (src/main/resources) !!!!!!!!!", cleanPath, description);
                // Considerar lançar uma exceção aqui se o arquivo for crítico (como o schema)
                if (SCHEMA_FILE.equals(cleanPath)) {
                    throw new FileNotFoundException("Arquivo de Schema OWL não encontrado: " + cleanPath);
                }
                return; // Continua se não for o schema (pode ser opcional)
            }
            // Usa um BufferedInputStream para melhor performance
            try (InputStream bis = new BufferedInputStream(in)) {
                RDFDataMgr.read(baseModel, bis, language);
            }
            logger.info("   {} '{}' carregado com sucesso.", description, cleanPath);
        } catch (FileNotFoundException fnfe) {
            // Exceção lançada acima se o schema não for encontrado
            throw new RuntimeException(fnfe);
        } catch (JenaException e) {
            // Erro específico do Jena (geralmente sintaxe RDF)
            logger.error("   Erro de SINTAXE RDF ou Jena ao carregar {} de {}: {}", description, cleanPath, e.getMessage());
            // Logar mais detalhes da exceção pode ajudar
            logger.debug("   Detalhes do erro Jena:", e);
            // Considerar lançar exceção para interromper a inicialização se houver erro de sintaxe
            // throw new RuntimeException("Erro de sintaxe no arquivo RDF: " + cleanPath, e);
        } catch (IOException e) {
            logger.error("   Erro de I/O ao ler {} de {}", description, cleanPath, e);
        } catch (Exception e) {
            logger.error("   Erro INESPERADO ao carregar {} de {}", description, cleanPath, e);
        } finally {
            // Fecha o InputStream original (o BufferedInputStream já é fechado pelo try-with-resources)
            if (in != null) {
                try { in.close(); } catch (IOException e) { logger.error("   Erro ao fechar InputStream original para {}", cleanPath, e); }
            }
        }
    }

    /**
     * Valida o modelo base após o carregamento dos dados.
     */
    private void validateBaseModelLoad(long baseModelSize) {
        if (baseModelSize == 0) {
            logger.error("!!!!!!!!!!!!! MODELO BASE ESTÁ VAZIO APÓS CARREGAMENTO! NENHUM DADO FOI CARREGADO. VERIFIQUE OS ARQUIVOS .OWL, .TTL E O PROCESSO DE CARGA DO EXCEL !!!!!!!!!!!!!");
            // Lançar exceção aqui pode ser apropriado para parar a aplicação
            // throw new IllegalStateException("Modelo RDF base vazio após carregamento.");
        } else {
            // Verifica se instâncias de Pregao foram carregadas do Excel
            boolean hasPregacoes = false;
            try {
                hasPregacoes = baseModel.listSubjectsWithProperty(RDF.type, getResource("Pregao")).hasNext();
            } catch (Exception e) {
                logger.error("Erro ao verificar a existência de Pregões no modelo", e);
            }

            if (!hasPregacoes) {
                logger.warn("Modelo base contém {} triplas, mas NENHUM recurso do tipo '{}Pregao' foi encontrado. Verifique o método loadExcelData (índices, formato de datas/números) e o conteúdo das planilhas Excel.", baseModelSize, ONT_PREFIX);
            } else {
                logger.info("   Validação pós-carga: Modelo base contém {} triplas e instâncias de Pregão foram encontradas.", baseModelSize);
            }
        }
    }


    /**
     * Obtém o raciocinador Jena (atualmente RDFS).
     * Poderia ser configurado para OWL ou regras customizadas.
     */
    private Reasoner getReasoner() {
        // Opções:
        // 1. RDFS Simples: ReasonerRegistry.getRDFSReasoner()
        // 2. OWL Micro: ReasonerRegistry.getOWLMicroReasoner()
        // 3. OWL Mini: ReasonerRegistry.getOWLMiniReasoner()
        // 4. OWL Completo (Transitivo): ReasonerRegistry.getOWLReasoner()
        // 5. Com Regras Customizadas (se tiver /regras.rules): ReasonerRegistry.getRDFSSimpleReasoner().bindSchema(baseModel).addRules(Rule.rulesFromURL(Ontology.class.getResource(RULES_FILE).toString()));

        logger.info("   Usando RDFS Reasoner padrão (ReasonerRegistry.getRDFSReasoner()).");
        return ReasonerRegistry.getRDFSReasoner();
    }

    /**
     * Carrega dados de pregão de um arquivo Excel para o modelo base.
     * Inclui correções nos índices das colunas e adição da coluna Quantidade.
     */
    private void loadExcelData(String resourcePath) {
        String cleanPath = resourcePath.startsWith("/") ? resourcePath : "/" + resourcePath;
        logger.info(">> Iniciando carregamento Pregão de: {}", cleanPath);
        int rowsProcessed = 0;
        int errors = 0;
        InputStream excelFile = null;

        try {
            excelFile = Ontology.class.getResourceAsStream(cleanPath);
            if (excelFile == null) {
                logger.error("   Arquivo Excel '{}' não encontrado no classpath (src/main/resources). Pulando este arquivo.", cleanPath);
                return; // Não continua se o arquivo não existe
            }

            // Usa try-with-resources para garantir fechamento do Workbook
            try (Workbook workbook = new XSSFWorkbook(excelFile)) {
                // Assume que os dados estão na primeira planilha
                Sheet sheet = workbook.getSheetAt(0);
                if (sheet == null) {
                    logger.error("   Planilha 0 (primeira) não encontrada no arquivo Excel '{}'.", cleanPath);
                    return;
                }
                int lastRowNum = sheet.getLastRowNum();
                logger.info("   ... Processando Planilha '{}' (Última linha física no índice: {})", sheet.getSheetName(), lastRowNum);

                SimpleDateFormat rdfDateFormat = new SimpleDateFormat("yyyy-MM-dd"); // Formato padrão RDF para datas

                // Itera pelas linhas, começando da segunda linha (índice 1), assumindo que a primeira é cabeçalho
                for (int i = 1; i <= lastRowNum; i++) {
                    Row row = sheet.getRow(i);
                    if (row == null) {
                        logger.trace("   L{} Pulando: Linha física nula.", i + 1);
                        continue; // Pula linhas completamente nulas
                    }

                    // --- ÍNDICES DAS COLUNAS (Base 0) ---
                    // AJUSTE CONFORME A ESTRUTURA EXATA DA SUA PLANILHA
                    final int tickerColIdx = 4;       // Coluna E (Ticker)
                    final int dataColIdx = 2;       // Coluna C (Data)
                    final int openPriceColIdx = 8;  // Coluna I (Abertura)
                    final int highPriceColIdx = 9;  // Coluna J (Máximo)
                    final int lowPriceColIdx = 10;   // Coluna K (Mínimo)
                    final int closePriceColIdx = 12; // Coluna M (Fechamento) <- CORRIGIDO
                    final int quantidadeColIdx = 13; // Coluna N (Quantidade) <- ADICIONADO
                    final int volumeColIdx = 15;    // Coluna P (Volume)
                    // --- FIM ÍNDICES ---

                    // Validação mínima da célula do ticker antes de prosseguir
                    Cell tickerCell = row.getCell(tickerColIdx);
                    if (tickerCell == null || tickerCell.getCellType() == CellType.BLANK) {
                        logger.trace("   L{} Pulando: Célula do ticker (col {}) vazia ou nula.", i + 1, tickerColIdx + 1);
                        continue;
                    }

                    String ticker = null;
                    Date dataPregaoDate = null;

                    try {
                        // 1. Leitura dos valores das células usando métodos helper
                        ticker = getStringCellValue(tickerCell, "Ticker", i + 1);
                        dataPregaoDate = getDateCellValue(row.getCell(dataColIdx), "Data", i + 1, ticker); // Passa ticker para logs

                        // Validações Críticas Iniciais
                        if (ticker == null || ticker.trim().isEmpty() || !ticker.matches("^[A-Z]{4}\\d{1,2}$")) {
                            if (ticker != null && !ticker.trim().isEmpty()) logger.warn("   L{} Pulando: Ticker lido ('{}') não corresponde ao formato esperado AAAA(N)N.", i + 1, ticker);
                            else logger.warn("   L{} Pulando: Ticker nulo ou vazio na coluna {}.", i + 1, tickerColIdx + 1);
                            errors++; continue;
                        }
                        ticker = ticker.trim().toUpperCase(); // Normaliza APÓS validação de formato

                        if (dataPregaoDate == null) {
                            logger.warn("   L{} Pulando: Data do pregão inválida ou não encontrada para o ticker '{}' na coluna {}.", i + 1, ticker, dataColIdx + 1);
                            errors++; continue;
                        }

                        // Leitura dos dados numéricos
                        double precoAbertura = getNumericCellValue(row.getCell(openPriceColIdx), "PrecoAbertura", i + 1, ticker);
                        double precoMaximo = getNumericCellValue(row.getCell(highPriceColIdx), "PrecoMaximo", i + 1, ticker);
                        double precoMinimo = getNumericCellValue(row.getCell(lowPriceColIdx), "PrecoMinimo", i + 1, ticker);
                        double precoFechamento = getNumericCellValue(row.getCell(closePriceColIdx), "PrecoFechamento", i + 1, ticker); // Coluna M (12)
                        double quantidadeNeg = getNumericCellValue(row.getCell(quantidadeColIdx), "Quantidade", i + 1, ticker);       // Coluna N (13)
                        double volumeTotal = getNumericCellValue(row.getCell(volumeColIdx), "Volume", i + 1, ticker);             // Coluna P (15)

                        // Validação: Pula se TODOS os dados numéricos forem inválidos (NaN)
                        if (Double.isNaN(precoAbertura) && Double.isNaN(precoMaximo) && Double.isNaN(precoMinimo) &&
                                Double.isNaN(precoFechamento) && Double.isNaN(quantidadeNeg) && Double.isNaN(volumeTotal)) {
                            logger.warn("   L{} Pulando: Nenhum dado numérico válido (preços, qtd, volume) encontrado para ticker '{}' na data {}.", i + 1, ticker, dataPregaoDate);
                            errors++; continue;
                        }

                        // 2. --- Criação de Recursos e Triplas RDF ---

                        // Formatação e Geração de URIs
                        String dataFormatada = rdfDateFormat.format(dataPregaoDate); // Formato YYYY-MM-DD

                        // URI para o Valor Mobiliário (Ação/Ticker)
                        String valorMobiliarioURI = ONT_PREFIX + ticker;
                        // URI para a instância de Negociação (Contexto do Ticker no Pregão)
                        String negociadoURI = ONT_PREFIX + "Negociado_" + ticker + "_" + dataFormatada;
                        // URI para o Pregão (Contexto do Dia)
                        String pregaoURI = ONT_PREFIX + "Pregao_" + dataFormatada;

                        // Criação/Obtenção de Recursos no Modelo RDF (com tipos)
                        Resource valorMobiliario = baseModel.createResource(valorMobiliarioURI);
                        Resource negociado = baseModel.createResource(negociadoURI, getResource("Negociado_Em_Pregao")); // Adiciona tipo Negociado
                        Resource pregao = baseModel.createResource(pregaoURI, getResource("Pregao")); // Adiciona tipo Pregao

                        // 3. Adição de Propriedades (evitando duplicatas se possível)

                        // Adiciona tipo e ticker ao Valor Mobiliário (se ainda não tiver)
                        addPropertyIfNotExist(valorMobiliario, RDF.type, getResource("Valor_Mobiliario"));
                        addPropertyIfNotExist(valorMobiliario, getProperty("ticker"), ticker);

                        // Adiciona data ao Pregão (se ainda não tiver)
                        addPropertyIfNotExist(pregao, getProperty("ocorreEmData"), ResourceFactory.createTypedLiteral(dataFormatada, XSDDatatype.XSDdate));

                        // Adiciona propriedades de dados ao recurso 'negociado' (SOMENTE se o valor for válido)
                        addNumericPropertyIfValid(negociado, getProperty("precoAbertura"), precoAbertura);
                        addNumericPropertyIfValid(negociado, getProperty("precoMaximo"), precoMaximo);
                        addNumericPropertyIfValid(negociado, getProperty("precoMinimo"), precoMinimo);
                        addNumericPropertyIfValid(negociado, getProperty("precoFechamento"), precoFechamento); // <-- Coluna M (12)
                        addNumericPropertyIfValid(negociado, getProperty("quantidadeNegociada"), quantidadeNeg); // <-- Coluna N (13) - Verifique nome da propriedade!
                        addNumericPropertyIfValid(negociado, getProperty("volumeNegociacao"), volumeTotal);       // <-- Coluna P (15) - Verifique nome da propriedade!

                        // Adiciona relações entre os recursos (se ainda não existirem)
                        addPropertyIfNotExist(negociado, getProperty("negociadoDurante"), pregao);
                        addPropertyIfNotExist(valorMobiliario, getProperty("negociado"), negociado);

                        rowsProcessed++; // Incrementa contador de sucesso apenas se chegou até aqui

                    } catch (Exception e) {
                        // Captura erros durante o processamento de uma linha específica
                        logger.error("   Erro GERAL ao processar linha {} da planilha '{}' (Ticker: {}, Data lida: {}): {}",
                                i + 1, cleanPath, ticker != null ? ticker : "N/A", (dataPregaoDate != null ? dataPregaoDate : "N/A"), e.getMessage(), e);
                        errors++;
                    }
                } // Fim do loop for (linhas)

                logger.info("<< Pregão {} carregado. {} linhas da planilha lidas, {} triplas RDF geradas com sucesso, {} linhas com erros/puladas.",
                        cleanPath, sheet.getLastRowNum(), rowsProcessed, errors);

                // Avisos importantes se nada foi processado
                if (rowsProcessed == 0 && sheet.getLastRowNum() > 0) {
                    if (errors > 0) logger.error("!!!!!!!! NENHUMA LINHA PROCESSADA COM SUCESSO EM '{}'. VERIFIQUE OS ÍNDICES DAS COLUNAS, FORMATO DE DATAS/NÚMEROS E LOGS DE ERRO ACIMA !!!!!!!!!!", cleanPath);
                    else logger.warn("!!!!!!!! NENHUMA LINHA PROCESSADA COM SUCESSO EM '{}', MAS NÃO HOUVE ERROS REPORTADOS. VERIFICAR CRITÉRIOS DE VALIDAÇÃO (EX: FORMATO TICKER) E CONTEÚDO EXCEL !!!!!!!!!", cleanPath);
                }

            } // Fim do try-with-resources (workbook)

        } catch (IOException e) {
            logger.error("   Erro de I/O ao ABRIR ou LER arquivo Excel {}", cleanPath, e);
        } catch (InvalidPathException ipe) {
            logger.error("   Caminho inválido para o arquivo Excel {}", cleanPath, ipe);
        } catch (Exception e) {
            logger.error("   Erro inesperado ao processar Excel {}", cleanPath, e);
        } finally {
            if (excelFile != null) {
                try { excelFile.close(); } catch (IOException e) { logger.error("   Erro ao fechar InputStream do Excel {}", cleanPath, e); }
            }
        }
    } // Fim do método loadExcelData

    // --- Métodos Helper para Leitura de Células e Adição de Propriedades ---

    /** Lê o valor de uma célula como String, com tratamento de tipos comuns. */
    private String getStringCellValue(Cell cell, String colName, int rowNum) {
        if (cell == null || cell.getCellType() == CellType.BLANK) {
            //logger.trace("   L{} Col '{}': Célula nula ou vazia.", rowNum, colName);
            return null;
        }
        try {
            CellType cellType = cell.getCellType();
            // Trata fórmulas, pegando o resultado cacheado
            if (cellType == CellType.FORMULA) {
                cellType = cell.getCachedFormulaResultType();
            }

            switch (cellType) {
                case STRING:
                    String val = cell.getStringCellValue();
                    return (val != null) ? val.trim() : null; // Retorna null se for string vazia após trim
                case NUMERIC:
                    // Converte números inteiros para string sem ".0"
                    double numVal = cell.getNumericCellValue();
                    if (numVal == Math.floor(numVal) && !Double.isInfinite(numVal)) {
                        return String.valueOf((long) numVal);
                    } else {
                        return String.valueOf(numVal);
                    }
                case BOOLEAN:
                    return String.valueOf(cell.getBooleanCellValue());
                default:
                    logger.warn("   L{} Coluna '{}': Tipo de célula {} não esperado para String.", rowNum, colName, cellType);
                    return null;
            }
        } catch (Exception e) {
            logger.error("   L{} Coluna '{}': Erro inesperado ao ler valor como String: {}", rowNum, colName, e.getMessage());
            return null;
        }
    }

    /** Lê o valor de uma célula como Data, tentando múltiplos formatos. */
    private Date getDateCellValue(Cell cell, String colName, int rowNum, String tickerContext) {
        if (cell == null || cell.getCellType() == CellType.BLANK) {
            //logger.trace("   L{} Col '{}' (Ticker {}): Célula de data nula ou vazia.", rowNum, colName, tickerContext);
            return null;
        }
        try {
            CellType cellType = cell.getCellType();
            if (cellType == CellType.FORMULA) {
                cellType = cell.getCachedFormulaResultType();
            }

            if (cellType == CellType.NUMERIC) {
                if (DateUtil.isCellDateFormatted(cell)) {
                    Date dateValue = cell.getDateCellValue();
                    //logger.trace("   L{} '{}' ({}): Célula numérica formatada como data: {}", rowNum, colName, tickerContext, dateValue);
                    return dateValue;
                } else {
                    // Tenta interpretar como data numérica Excel ou YYYYMMDD
                    double numVal = cell.getNumericCellValue();
                    if (DateUtil.isValidExcelDate(numVal)) {
                        Date dateValue = DateUtil.getJavaDate(numVal);
                        //logger.trace("   L{} '{}' ({}): Numérico {} interpretado como Data Excel -> {}", rowNum, colName, tickerContext, numVal, dateValue);
                        return dateValue;
                    }
                    if (numVal > 20000000 && numVal < 22000000 && numVal == Math.floor(numVal)) { // Heurística YYYYMMDD
                        String dateStr = String.valueOf((long) numVal);
                        try { SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd"); fmt.setLenient(false); return fmt.parse(dateStr); }
                        catch (ParseException pe) { /* Ignora, tentará como string abaixo se falhar */ }
                    }
                    logger.warn("   L{} '{}' ({}): Célula numérica {} não é data formatada, nem data Excel válida, nem yyyyMMdd.", rowNum, colName, tickerContext, numVal);
                    // Tenta converter para string e parsear (fallback)
                    String numStr = (numVal == Math.floor(numVal)) ? String.valueOf((long)numVal) : String.valueOf(numVal);
                    return parseDateFromString(numStr, colName, rowNum, tickerContext);
                }
            } else if (cellType == CellType.STRING) {
                String dateStr = cell.getStringCellValue().trim();
                return parseDateFromString(dateStr, colName, rowNum, tickerContext);
            } else {
                logger.warn("   L{} '{}' ({}): Tipo de célula inesperado ({}) para Data.", rowNum, colName, tickerContext, cellType);
                return null;
            }
        } catch (Exception e) {
            logger.error("   L{} '{}' ({}): Erro INESPERADO ao ler Data da célula: {}", rowNum, colName, tickerContext, e.getMessage(), e);
            return null;
        }
    }

    /** Tenta parsear uma string de data usando formatos comuns. */
    private Date parseDateFromString(String dateStr, String colName, int rowNum, String tickerContext) {
        if (dateStr == null || dateStr.isEmpty()) {
            //logger.trace("   L{} '{}' ({}): String de data vazia.", rowNum, colName, tickerContext);
            return null;
        }
        // Formatos a tentar, do mais específico para o mais geral ou comum
        SimpleDateFormat[] formats = {
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
                new SimpleDateFormat("dd/MM/yyyy HH:mm:ss"),
                new SimpleDateFormat("yyyy-MM-dd"),
                new SimpleDateFormat("dd/MM/yyyy"),
                new SimpleDateFormat("yyyyMMdd"),
                new SimpleDateFormat("MM/dd/yy", Locale.US), // Formato americano comum
                new SimpleDateFormat("dd-MMM-yyyy", Locale.ENGLISH) // Formato comum com mês abreviado
        };
        for (SimpleDateFormat fmt : formats) {
            try {
                fmt.setLenient(false); // Não permite datas inválidas (ex: 31/02)
                Date parsed = fmt.parse(dateStr);
                //logger.trace("   L{} '{}' ({}): String '{}' parseada com formato '{}' -> {}", rowNum, colName, tickerContext, dateStr, fmt.toPattern(), parsed);
                return parsed;
            } catch (ParseException pe) {
                // Ignora e tenta o próximo formato
            }
        }
        logger.warn("   L{} '{}' ({}): Impossível parsear data da string '{}' com formatos conhecidos.", rowNum, colName, tickerContext, dateStr);
        return null;
    }


    /** Lê o valor de uma célula como double, com tratamento de erros e limpeza. */
    private double getNumericCellValue(Cell cell, String colName, int rowNum, String tickerContext) {
        if (cell == null || cell.getCellType() == CellType.BLANK) return Double.NaN;
        try {
            CellType cellType = cell.getCellType();
            if (cellType == CellType.FORMULA) {
                cellType = cell.getCachedFormulaResultType();
            }
            switch (cellType) {
                case NUMERIC:
                    // Verifica se não é uma data formatada como número
                    if (DateUtil.isCellDateFormatted(cell)){
                        logger.warn("   L{} '{}' ({}): Valor numérico esperado, mas célula está formatada como Data ({}). Retornando NaN.", rowNum, colName, tickerContext, cell.getDateCellValue());
                        return Double.NaN;
                    }
                    return cell.getNumericCellValue();
                case STRING:
                    String val = cell.getStringCellValue().trim();
                    // Trata strings comuns que representam ausência de valor
                    if (val.isEmpty() || val.equals("-") || val.equalsIgnoreCase("N/A") || val.equalsIgnoreCase("NaN")) return Double.NaN;
                    try {
                        // Limpeza: remove R$, %, espaços, usa . como decimal
                        val = val.replace("R$", "").replace("%", "").replace(".", "").replace(",", ".").trim();
                        return Double.parseDouble(val);
                    } catch (NumberFormatException e) {
                        logger.warn("   L{} '{}' ({}): Impossível converter string '{}' (após limpeza: '{}') para número.", rowNum, colName, tickerContext, cell.getStringCellValue(), val);
                        return Double.NaN;
                    }
                default:
                    logger.warn("   L{} '{}' ({}): Tipo de célula inesperado ({}) para valor Numérico.", rowNum, colName, tickerContext, cellType);
                    return Double.NaN;
            }
        } catch (Exception e) {
            logger.error("   L{} '{}' ({}): Erro INESPERADO ao ler valor Numérico: {}", rowNum, colName, tickerContext, e.getMessage());
            return Double.NaN;
        }
    }

    /** Helper para criar URI de Property usando o prefixo da ontologia. */
    private Property getProperty(String localName) {
        // Validação básica do nome local (opcional)
        if (localName == null || localName.trim().isEmpty() || localName.contains(" ") || localName.contains("#")) {
            logger.error("Nome local inválido para propriedade: '{}'. Usando fallback.", localName);
            // Retornar um valor padrão ou lançar exceção? Depende da criticidade.
            return ResourceFactory.createProperty(ONT_PREFIX + "propriedadeInvalida_" + System.currentTimeMillis()); // Evita NullPointerException
        }
        return ResourceFactory.createProperty(ONT_PREFIX + localName.trim());
    }

    /** Helper para criar URI de Resource (Classe) usando o prefixo da ontologia. */
    private Resource getResource(String localName) {
        // Validação básica do nome local (opcional)
        if (localName == null || localName.trim().isEmpty() || localName.contains(" ") || localName.contains("#")) {
            logger.error("Nome local inválido para recurso/classe: '{}'. Usando fallback.", localName);
            return ResourceFactory.createResource(ONT_PREFIX + "RecursoInvalido_" + System.currentTimeMillis());
        }
        return ResourceFactory.createResource(ONT_PREFIX + localName.trim());
    }

    /** Adiciona uma propriedade de objeto se ela ainda não existir para o sujeito/predicado. */
    private void addPropertyIfNotExist(Resource subject, Property predicate, RDFNode object) {
        if (subject != null && predicate != null && object != null && !baseModel.contains(subject, predicate, object)) {
            subject.addProperty(predicate, object);
            logger.trace("   Adicionada tripla: {} {} {}", subject.getLocalName(), predicate.getLocalName(), (object.isResource() ? object.asResource().getLocalName() : object.toString()));
        }
    }

    /** Adiciona uma propriedade literal (String) se ela ainda não existir. */
    private void addPropertyIfNotExist(Resource subject, Property predicate, String literalValue) {
        if (subject != null && predicate != null && literalValue != null && !literalValue.isEmpty() && !baseModel.contains(subject, predicate, literalValue)) {
            subject.addProperty(predicate, literalValue);
            logger.trace("   Adicionada tripla: {} {} \"{}\"", subject.getLocalName(), predicate.getLocalName(), literalValue);
        }
    }

    /** Adiciona uma propriedade literal com tipo de dado (ex: data) se ela ainda não existir. */
    private void addPropertyIfNotExist(Resource subject, Property predicate, Literal typedLiteral) {
        if (subject != null && predicate != null && typedLiteral != null && !baseModel.contains(subject, predicate, typedLiteral)) {
            subject.addProperty(predicate, typedLiteral);
            logger.trace("   Adicionada tripla: {} {} {}", subject.getLocalName(), predicate.getLocalName(), typedLiteral.toString());
        }
    }


    /** Adiciona uma propriedade numérica (double) se o valor for válido (não NaN). */
    private void addNumericPropertyIfValid(Resource subject, Property predicate, double value) {
        if (subject != null && predicate != null && !Double.isNaN(value)) {
            // Cria o literal tipado como xsd:double (ou xsd:decimal se preferir)
            Literal literal = baseModel.createTypedLiteral(value);
            // Adiciona a propriedade (poderia verificar duplicata, mas para dados numéricos geralmente não é necessário)
            subject.addProperty(predicate, literal);
            logger.trace("   Adicionada tripla numérica: {} {} {}", subject.getLocalName(), predicate.getLocalName(), literal.getLexicalForm());
        }
    }


    /** Salva o modelo inferido em arquivo Turtle. */
    private void saveInferredModel() {
        logger.info("--- Tentando salvar modelo RDF inferido em {}...", INFERENCE_OUTPUT_FILE);
        Path outputPath = null;
        try {
            // Resolve o caminho relativo à raiz do projeto (diretório onde o comando java é executado)
            Path projectDir = Paths.get(".").toAbsolutePath().normalize();
            outputPath = projectDir.resolve(INFERENCE_OUTPUT_FILE);
            logger.info("   Caminho absoluto para salvar: {}", outputPath);

            // Verifica se o modelo inferido existe e não está vazio
            if (infModel != null && infModel.size() > 0) {
                // Verifica se o diretório pai existe, tenta criar se não existir
                Path parentDir = outputPath.getParent();
                if (parentDir != null && !Files.exists(parentDir)) {
                    logger.warn("   Diretório pai {} não existe. Tentando criar...", parentDir);
                    try {
                        Files.createDirectories(parentDir);
                        logger.info("   Diretório pai criado com sucesso.");
                    } catch (IOException eCreate) {
                        logger.error("   Falha ao criar diretório pai {}. Não será possível salvar.", parentDir, eCreate);
                        return; // Não tenta salvar se não conseguiu criar o diretório
                    }
                }

                logger.info("   Salvando {} triplas inferidas...", infModel.size());
                // Usa try-with-resources para garantir o fechamento do OutputStream
                try (OutputStream fos = new BufferedOutputStream(new FileOutputStream(outputPath.toFile()))) {
                    RDFDataMgr.write(fos, infModel, Lang.TURTLE); // Salva em formato Turtle
                    logger.info("   Modelo RDF inferido salvo com sucesso em {}", outputPath);
                } catch (IOException | JenaException e) {
                    logger.error("   Erro ao ESCREVER o modelo RDF inferido no arquivo {}.", outputPath, e);
                }
            } else if (infModel != null && infModel.size() == 0) {
                logger.warn("   Modelo inferido está VAZIO. Nada para salvar em {}", outputPath);
            } else if (infModel != null && baseModel != null && infModel.size() < baseModel.size()) {
                logger.warn("   Modelo inferido ({}) é MENOR que o modelo base ({}). Isso é inesperado. Verifique o reasoner. Não será salvo.", infModel.size(), baseModel.size());
            }
            else {
                logger.error("   Modelo inferido é nulo. Não é possível salvar.");
            }
        } catch(InvalidPathException ipe) {
            logger.error("   Caminho de saída '{}' é inválido para o sistema de arquivos.", INFERENCE_OUTPUT_FILE, ipe);
        } catch (Exception e) {
            logger.error("   Erro inesperado durante a tentativa de salvar o modelo inferido.", e);
        }
    }

    /**
     * Executa uma consulta SPARQL SELECT no modelo inferido.
     * Retorna uma lista de strings com os valores da variável alvo.
     * Retorna null em caso de erro grave (sintaxe, execução).
     */
    public List<String> executeQuery(String sparqlQuery, String targetVariable) {
        // Bloqueio de leitura para garantir consistência do modelo durante a consulta
        lock.readLock().lock();
        try {
            // Verifica se o modelo inferido está pronto
            if (infModel == null) {
                logger.error("ERRO: Modelo de inferência não inicializado. Impossível executar consulta.");
                return null; // Retorna null indicando falha
            }

            logger.debug("Executando consulta SPARQL. Variável alvo: '{}'\n---\n{}\n---", targetVariable, sparqlQuery);
            List<String> results = new ArrayList<>();
            Query query;

            // 1. Parse da Query (validação de sintaxe)
            try {
                query = QueryFactory.create(sparqlQuery);
                logger.debug("   Query SPARQL parseada com sucesso.");
            } catch (QueryParseException e) {
                logger.error("   ERRO DE SINTAXE na query SPARQL: {}", e.getMessage());
                // Loga a linha e coluna do erro para facilitar depuração
                logger.error("   Detalhes do erro: Linha {}, Coluna {}", e.getLine(), e.getColumn());
                logger.error("   Query com erro:\n---\n{}\n---", sparqlQuery);
                return null; // Retorna null indicando falha no parse
            }

            // 2. Execução da Query (try-with-resources para garantir fechamento do QueryExecution)
            try (QueryExecution qexec = QueryExecutionFactory.create(query, infModel)) {
                ResultSet rs = qexec.execSelect();
                logger.debug("   Executando SELECT e iterando sobre os resultados...");

                // 3. Processamento dos Resultados
                while (rs.hasNext()) {
                    QuerySolution soln = rs.nextSolution();
                    RDFNode node = soln.get(targetVariable); // Obtém o nó RDF para a variável alvo

                    if (node != null) {
                        // Extrai o valor dependendo do tipo do nó (Literal ou Recurso/URI)
                        if (node.isLiteral()) {
                            String lexicalForm = node.asLiteral().getLexicalForm();
                            results.add(lexicalForm);
                            logger.trace("    Resultado encontrado (Literal): {}", lexicalForm);
                        } else if (node.isResource()) {
                            String uri = node.asResource().getURI();
                            results.add(uri); // Adiciona o URI completo
                            logger.trace("    Resultado encontrado (Resource): {}", uri);
                        } else {
                            // Caso raro: nó não é literal nem recurso
                            logger.warn("    Nó encontrado para a variável '{}' não é Literal nem Resource: {}", targetVariable, node);
                            results.add(node.toString()); // Adiciona representação em string como fallback
                        }
                    } else {
                        // A variável alvo não estava presente nesta linha de resultado (pode acontecer com OPTIONAL)
                        logger.warn("    Variável alvo '{}' não encontrada na solução atual: {}. Possivelmente devido a um OPTIONAL?", targetVariable, soln);
                        // Decide se quer adicionar um marcador (ex: null ou string vazia) ou simplesmente ignorar
                        // results.add(null); // Opção: adicionar null
                    }
                }
                logger.debug("   Iteração dos resultados concluída. {} resultado(s) encontrado(s) para a variável '{}'.", results.size(), targetVariable);

            } catch (Exception e) {
                // Captura erros durante a execução da query ou processamento dos resultados
                logger.error("   Erro durante a EXECUÇÃO da query SPARQL ou processamento dos resultados.", e);
                return null; // Retorna null indicando falha na execução
            }

            return results; // Retorna a lista de resultados (pode estar vazia)

        } finally {
            lock.readLock().unlock(); // Libera o bloqueio de leitura
        }
    }

} // Fim da classe Ontology