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
    private static final String RULES_FILE = "/regras.rules"; // Arquivo de regras (opcional)

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

            // Verifica se o modelo base está vazio ou muito pequeno após carregar tudo
            if (baseSizeBeforeInfer == 0) {
                logger.error("!!!!!!!!!!!!! MODELO BASE ESTÁ VAZIO APÓS CARREGAMENTO! VERIFIQUE OS ARQUIVOS .OWL, .TTL E O CARREGAMENTO DO EXCEL !!!!!!!!!!!!!");
            } else if (Files.exists(Paths.get("src/main/resources", BASE_DATA_FILE)) && baseModel.listStatements(null, RDF.type, getResource("Pregao")).toList().isEmpty()) {
                // Verifica especificamente se nenhum Pregão foi carregado do Excel (se o TTL base existir)
                logger.warn("Modelo base contém {} triplas, mas NENHUM recurso do tipo 'ont:Pregao' foi carregado das planilhas Excel. Verifique o método loadExcelData e o formato das datas/índices.", baseSizeBeforeInfer);
            }

            // 4. Configurar e Aplicar Raciocinador (Inferência)
            logger.info("--- Configurando Reasoner ---");
            Reasoner reasoner = getReasoner(); // Obtém o raciocinador (atualmente RDFS)
            logger.info("--- Criando modelo inferência ---");
            infModel = ModelFactory.createInfModel(reasoner, baseModel); // Cria o modelo com inferência
            long infSize = infModel.size();
            logger.info("--- Modelo inferência criado. Base:{}, Inferidas:{}, Total:{} ---",
                    baseSizeBeforeInfer, (infSize - baseSizeBeforeInfer), infSize);

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
     */
    private void loadRdfData(String resourcePath, Lang language, String description) {
        logger.info("   Tentando carregar {} de classpath:{}", description, resourcePath);
        InputStream in = null;
        try {
            in = Ontology.class.getResourceAsStream(resourcePath);
            if (in == null) {
                logger.warn("   Arquivo '{}' ({}) não encontrado no classpath.", resourcePath, description);
                if (BASE_DATA_FILE.equals(resourcePath)) {
                    logger.error("   !!!!!!!! ARQUIVO BASE TTL '{}' É ESSENCIAL E NÃO FOI ENCONTRADO! COLOQUE-O EM src/main/resources !!!!!!!!!!", resourcePath);
                }
                return;
            }
            RDFDataMgr.read(baseModel, in, language);
            logger.info("   {} carregado com sucesso.", description);
        } catch (JenaException e) {
            logger.error("   Erro de sintaxe ou Jena ao carregar {} de {}: {}", description, resourcePath, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("   Erro inesperado ao carregar {} de {}", description, resourcePath, e);
        } finally {
            if (in != null) {
                try { in.close(); } catch (IOException e) { logger.error("   Erro ao fechar InputStream para {}", resourcePath, e); }
            }
        }
    }

    /**
     * Obtém o raciocinador Jena (atualmente RDFS).
     */
    private Reasoner getReasoner() {
        logger.info("   Usando RDFS Reasoner padrão.");
        return ReasonerRegistry.getRDFSReasoner();
    }

    /**
     * Carrega dados de pregão de um arquivo Excel.
     * ÍNDICES CORRIGIDOS conforme especificação.
     */
    private void loadExcelData(String resourcePath) {
        logger.info(">> Iniciando carregamento Pregão: {}", resourcePath);
        int rowsProcessed = 0;
        int errors = 0;
        InputStream excelFile = null;

        try {
            excelFile = Ontology.class.getResourceAsStream(resourcePath);
            if (excelFile == null) {
                logger.error("   Arquivo Excel '{}' não encontrado no classpath. Coloque-o em src/main/resources.", resourcePath);
                return;
            }

            try (Workbook workbook = new XSSFWorkbook(excelFile)) {
                Sheet sheet = workbook.getSheetAt(0);
                if (sheet == null) {
                    logger.error("   Planilha 0 não encontrada no arquivo '{}'", resourcePath);
                    return;
                }
                logger.info("   ... Processando Planilha Pregão '{}' (Última linha física: {})", sheet.getSheetName(), sheet.getLastRowNum());

                SimpleDateFormat rdfDateFormat = new SimpleDateFormat("yyyy-MM-dd");

                for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                    Row row = sheet.getRow(i);

                    // --- ÍNDICES DAS COLUNAS CORRIGIDOS ---
                    int tickerColIdx = 4;       // Coluna E
                    int dataColIdx = 2;       // Coluna C
                    int openPriceColIdx = 8;  // Coluna I
                    int highPriceColIdx = 9;  // Coluna J
                    int lowPriceColIdx = 10;   // Coluna K
                    int closePriceColIdx = 13; // Coluna M
                    int volumeColIdx = 15;    // Coluna P
                    // --- FIM ÍNDICES ---

                    if (row == null || row.getCell(tickerColIdx) == null || row.getCell(tickerColIdx).getCellType() == CellType.BLANK) {
                        continue;
                    }

                    String ticker = null;
                    Date dataPregaoDate = null;

                    try {
                        ticker = getStringCellValue(row.getCell(tickerColIdx), "Ticker", i + 1);
                        dataPregaoDate = getDateCellValue(row.getCell(dataColIdx), "Data", i + 1, ticker);
                        double precoAbertura = getNumericCellValue(row.getCell(openPriceColIdx), "PrecoAbertura", i + 1, ticker);
                        double precoMaximo = getNumericCellValue(row.getCell(highPriceColIdx), "PrecoMaximo", i + 1, ticker);
                        double precoMinimo = getNumericCellValue(row.getCell(lowPriceColIdx), "PrecoMinimo", i + 1, ticker);
                        double precoFechamento = getNumericCellValue(row.getCell(closePriceColIdx), "PrecoFechamento", i + 1, ticker);
                        double volumeTotal = getNumericCellValue(row.getCell(volumeColIdx), "Volume", i + 1, ticker);

                        // Validação
                        if (ticker == null || ticker.trim().isEmpty() || !ticker.matches("^[A-Z0-9]{4,6}$")) {
                            if(ticker != null && !ticker.trim().isEmpty()) logger.warn("   L{} Pulando: Ticker inválido ('{}').", i + 1, ticker);
                            errors++; continue;
                        }
                        if (dataPregaoDate == null) { errors++; continue; } // Log já feito em getDateCellValue
                        if (Double.isNaN(precoAbertura) && Double.isNaN(precoMaximo) && Double.isNaN(precoMinimo) && Double.isNaN(precoFechamento) && Double.isNaN(volumeTotal)) {
                            logger.warn("   L{} Pulando: Nenhum dado numérico válido para ticker '{}' na data {}.", i + 1, ticker, dataPregaoDate);
                            errors++; continue;
                        }

                        // Formatação e URIs
                        String dataFormatada = rdfDateFormat.format(dataPregaoDate);
                        ticker = ticker.trim().toUpperCase();

                        String valorMobiliarioURI = ONT_PREFIX + ticker;
                        String negociadoURI = ONT_PREFIX + "Negociado_" + ticker + "_" + dataFormatada;
                        String pregaoURI = ONT_PREFIX + "Pregao_" + dataFormatada;

                        // Criação/Obtenção de Recursos
                        Resource valorMobiliario = baseModel.createResource(valorMobiliarioURI);
                        Resource negociado = baseModel.createResource(negociadoURI, getResource("Negociado_Em_Pregao"));
                        Resource pregao = baseModel.createResource(pregaoURI, getResource("Pregao"));

                        // Adição de Tipos (se necessário)
                        if (!valorMobiliario.hasProperty(RDF.type)) valorMobiliario.addProperty(RDF.type, getResource("Valor_Mobiliario"));
                        if (!pregao.hasProperty(getProperty("ocorreEmData"))) pregao.addProperty(getProperty("ocorreEmData"), dataFormatada, XSDDatatype.XSDdate);

                        // Adição de Propriedades de Dados (se não NaN)
                        if (!Double.isNaN(precoAbertura)) negociado.addProperty(getProperty("precoAbertura"), baseModel.createTypedLiteral(precoAbertura));
                        if (!Double.isNaN(precoMaximo)) negociado.addProperty(getProperty("precoMaximo"), baseModel.createTypedLiteral(precoMaximo));
                        if (!Double.isNaN(precoMinimo)) negociado.addProperty(getProperty("precoMinimo"), baseModel.createTypedLiteral(precoMinimo));
                        if (!Double.isNaN(precoFechamento)) negociado.addProperty(getProperty("precoFechamento"), baseModel.createTypedLiteral(precoFechamento));
                        // *** VERIFICAR NOME DA PROPRIEDADE DE VOLUME ***
                        if (!Double.isNaN(volumeTotal)) negociado.addProperty(getProperty("volumeNegociacao"), baseModel.createTypedLiteral(volumeTotal));

                        // Adição de Propriedades de Objeto (Relações)
                        negociado.addProperty(getProperty("negociadoDurante"), pregao);
                        valorMobiliario.addProperty(getProperty("negociado"), negociado);

                        rowsProcessed++;

                    } catch (Exception e) {
                        logger.error("   Erro GERAL ao processar linha {} da planilha '{}' (Ticker: {}, Data lida: {}): {}",
                                i + 1, resourcePath, ticker, (dataPregaoDate != null ? dataPregaoDate : "N/A"), e.getMessage(), e);
                        errors++;
                    }
                } // Fim do loop for (linhas)

                logger.info("<< Pregão {} carregado. {} linhas processadas com sucesso, {} linhas com erros/puladas.", resourcePath, rowsProcessed, errors);
                if (rowsProcessed == 0 && sheet.getLastRowNum() > 0) {
                    if (errors > 0) logger.error("!!!!!!!! NENHUMA LINHA PROCESSADA COM SUCESSO EM '{}'. VERIFIQUE OS ÍNDICES DAS COLUNAS, FORMATO DE DATAS/NÚMEROS !!!!!!!!!!", resourcePath);
                    else logger.warn("!!!!!!!! NENHUMA LINHA PROCESSADA COM SUCESSO EM '{}', MAS NÃO HOUVE ERROS. VERIFICAR CRITÉRIOS DE VALIDAÇÃO E CONTEÚDO EXCEL !!!!!!!!!", resourcePath);
                }

            } // Fim do try-with-resources (workbook)

        } catch (IOException e) { logger.error("   Erro de I/O ao ABRIR ou LER arquivo Excel {}", resourcePath, e);
        } catch (InvalidPathException ipe) { logger.error("   Caminho inválido para o arquivo Excel {}", resourcePath, ipe);
        } catch (Exception e) { logger.error("   Erro inesperado ao processar Excel {}", resourcePath, e);
        } finally {
            if (excelFile != null) { try { excelFile.close(); } catch (IOException e) { logger.error("   Erro ao fechar InputStream do Excel {}", resourcePath, e); } }
        }
    }

    /** Lê o valor de uma célula como String. */
    private String getStringCellValue(Cell cell, String colName, int rowNum) {
        if (cell == null) return null;
        try {
            CellType cellType = cell.getCellType(); if (cellType == CellType.FORMULA) cellType = cell.getCachedFormulaResultType();
            switch (cellType) {
                case STRING: String val = cell.getStringCellValue(); return (val != null) ? val.trim() : null;
                case NUMERIC:
                    if (DateUtil.isCellDateFormatted(cell)) { logger.warn("   L{} Coluna '{}': String esperada, Data encontrada ({}).", rowNum, colName, cell.getDateCellValue()); return null; }
                    double numVal = cell.getNumericCellValue(); return (numVal == Math.floor(numVal) && !Double.isInfinite(numVal)) ? String.valueOf((long) numVal) : String.valueOf(numVal);
                case BLANK: return null; default: logger.warn("   L{} Coluna '{}': Tipo {} não tratado p/ String.", rowNum, colName, cellType); return null;
            }
        } catch (Exception e) { logger.error("   L{} Coluna '{}': Erro ler String: {}", rowNum, colName, e.getMessage()); return null; }
    }

    /** Lê o valor de uma célula como Data, tentando formato yyyyMMdd e outros. */
    private Date getDateCellValue(Cell cell, String colName, int rowNum, String ticker) {
        if (cell == null) { logger.trace("   L{} Col '{}' (Ticker {}): Célula nula.", rowNum, colName, ticker); return null; }
        try {
            CellType cellType = cell.getCellType(); if (cellType == CellType.FORMULA) cellType = cell.getCachedFormulaResultType();
            if (cellType == CellType.NUMERIC) {
                if (DateUtil.isCellDateFormatted(cell)) { Date dateValue = cell.getDateCellValue(); logger.trace("   L{} '{}' ({}): Célula numérica formatada como data: {}", rowNum, colName, ticker, dateValue); return dateValue; }
                else {
                    double numVal = cell.getNumericCellValue();
                    if (numVal > 20000000 && numVal < 22000000 && numVal == Math.floor(numVal)) { // Heurística YYYYMMDD
                        String dateStr = String.valueOf((long) numVal);
                        try { SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd"); fmt.setLenient(false); Date parsed = fmt.parse(dateStr); logger.trace("   L{} '{}' ({}): Numérico {} parseado como yyyyMMdd -> {}", rowNum, colName, ticker, dateStr, parsed); return parsed; }
                        catch (ParseException pe) { logger.warn("   L{} '{}' ({}): Falha parsear numérico {} como yyyyMMdd.", rowNum, colName, ticker, dateStr); }
                    } logger.warn("   L{} '{}' ({}): Numérico {} não formatado como Data/yyyyMMdd.", rowNum, colName, ticker, numVal); return null;
                }
            } else if (cellType == CellType.STRING) {
                String dateStr = cell.getStringCellValue().trim(); if (dateStr.isEmpty()) { logger.trace("   L{} '{}' ({}): String vazia.", rowNum, colName, ticker); return null; }
                SimpleDateFormat[] formats = { new SimpleDateFormat("yyyyMMdd"), new SimpleDateFormat("yyyy-MM-dd"), new SimpleDateFormat("dd/MM/yyyy"), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"), new SimpleDateFormat("dd/MM/yyyy HH:mm:ss"), new SimpleDateFormat("MM/dd/yy", Locale.US), new SimpleDateFormat("dd-MMM-yyyy", Locale.ENGLISH) };
                for (SimpleDateFormat fmt : formats) {
                    try { fmt.setLenient(false); Date parsed = fmt.parse(dateStr); logger.trace("   L{} '{}' ({}): String '{}' parseada c/ formato '{}' -> {}", rowNum, colName, ticker, dateStr, fmt.toPattern(), parsed); return parsed; }
                    catch (ParseException pe) { /* Ignora */ }
                } logger.warn("   L{} '{}' ({}): Impossível parsear data da string: '{}'.", rowNum, colName, ticker, dateStr); return null;
            } else { logger.warn("   L{} '{}' ({}): Tipo célula inesperado ({}) p/ Data.", rowNum, colName, ticker, cellType); return null; }
        } catch (Exception e) { logger.error("   L{} '{}' ({}): Erro INESPERADO ler Data: {}", rowNum, colName, ticker, e.getMessage(), e); return null; }
    }

    /** Lê o valor de uma célula como double. */
    private double getNumericCellValue(Cell cell, String colName, int rowNum, String ticker) {
        if (cell == null) return Double.NaN;
        try {
            CellType cellType = cell.getCellType(); if (cellType == CellType.FORMULA) cellType = cell.getCachedFormulaResultType();
            switch (cellType) {
                case NUMERIC: if (DateUtil.isCellDateFormatted(cell)){ logger.warn("   L{} '{}' ({}): Numérico esperado, Data encontrada ({}).", rowNum, colName, ticker, cell.getDateCellValue()); return Double.NaN; } return cell.getNumericCellValue();
                case STRING: String val = cell.getStringCellValue().trim(); if (val.isEmpty() || val.equals("-") || val.equalsIgnoreCase("N/A") || val.equalsIgnoreCase("NaN")) return Double.NaN;
                    try { val = val.replace("R$", "").replace("%", "").replace(".", "").replace(",", ".").trim(); return Double.parseDouble(val); }
                    catch (NumberFormatException e) { logger.warn("   L{} '{}' ({}): Impossível converter string '{}' p/ número.", rowNum, colName, ticker, cell.getStringCellValue()); return Double.NaN; }
                case BLANK: return Double.NaN; default: logger.warn("   L{} '{}' ({}): Tipo célula inesperado ({}) p/ Número.", rowNum, colName, ticker, cellType); return Double.NaN;
            }
        } catch (Exception e) { logger.error("   L{} '{}' ({}): Erro ler Numérico: {}", rowNum, colName, ticker, e.getMessage()); return Double.NaN; }
    }

    /** Helper para criar Property. */
    private Property getProperty(String localName) { return baseModel.createProperty(ONT_PREFIX + localName); }
    /** Helper para criar Resource (Classe). */
    private Resource getResource(String localName) { return baseModel.createResource(ONT_PREFIX + localName); }

    /** Salva o modelo inferido. */
    private void saveInferredModel() {
        logger.info("--- Tentando salvar modelo RDF inferido em {}...", INFERENCE_OUTPUT_FILE); Path outputPath = null;
        try {
            Path projectDir = Paths.get(".").toAbsolutePath().normalize(); outputPath = projectDir.resolve(INFERENCE_OUTPUT_FILE); logger.info("   Salvando modelo inferido em: {}", outputPath);
            if (infModel != null && outputPath != null) {
                if (infModel.size() > 0 && infModel.size() >= baseModel.size()) {
                    logger.info("   Salvando {} triplas...", infModel.size());
                    try (OutputStream fos = new BufferedOutputStream(new FileOutputStream(outputPath.toFile()))) { RDFDataMgr.write(fos, infModel, Lang.TURTLE); logger.info("   Modelo RDF inferido salvo com sucesso."); }
                    catch (IOException | JenaException e) { logger.error("   Erro ao salvar modelo RDF inferido.", e); }
                } else { logger.warn("   Modelo inferido vazio ou <= base (Base: {}, Inf: {}). Não será salvo.", baseModel.size(), infModel.size()); }
            } else { logger.error("   Modelo inferido nulo ou caminho inválido."); }
        } catch (Exception e) { logger.error("   Erro inesperado ao salvar modelo inferido.", e); }
    }

    /** Executa uma consulta SPARQL SELECT no modelo inferido. */
    public List<String> executeQuery(String sparqlQuery, String targetVariable) {
        lock.readLock().lock();
        try {
            if (infModel == null) { logger.error("Modelo inferência não inicializado."); return null; }
            logger.debug("Exec query SPARQL. Var: '{}'\n---\n{}\n---", targetVariable, sparqlQuery); List<String> results = new ArrayList<>(); Query query;
            try { query = QueryFactory.create(sparqlQuery); logger.debug("   Query parseada."); }
            catch (QueryParseException e) { logger.error("   Erro sintaxe query: {}", e.getMessage()); logger.error("   Detalhes: L{}, C{}", e.getLine(), e.getColumn()); logger.error("   Query:\n---\n{}\n---", sparqlQuery); return null; }
            try (QueryExecution qexec = QueryExecutionFactory.create(query, infModel)) {
                ResultSet rs = qexec.execSelect(); logger.debug("   Iterando resultados...");
                while (rs.hasNext()) {
                    QuerySolution soln = rs.nextSolution(); RDFNode node = soln.get(targetVariable);
                    if (node != null) {
                        if (node.isLiteral()) results.add(node.asLiteral().getLexicalForm());
                        else if (node.isResource()) results.add(node.asResource().getURI());
                        else logger.warn("   Nó p/ '{}' não é Literal/Resource: {}", targetVariable, soln);
                    } else { logger.warn("   Var '{}' não encontrada na solução: {}", targetVariable, soln); }
                } logger.debug("   Consulta OK. {} resultado(s) p/ '{}'.", results.size(), targetVariable);
            } catch (Exception e) { logger.error("   Erro execução query SPARQL.", e); return null; }
            return results;
        } finally { lock.readLock().unlock(); }
    }
}