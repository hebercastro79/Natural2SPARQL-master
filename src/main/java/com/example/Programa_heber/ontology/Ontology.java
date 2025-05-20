package com.example.Programa_heber.ontology; // Ajuste o pacote se necessário

import jakarta.annotation.PostConstruct;
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

    // Prefixo da sua ontologia
    private static final String ONT_PREFIX = "https://dcm.ffclrp.usp.br/lssb/stock-market-ontology#";


    private static final String[] PREGAO_FILES = {
            "/datasets/dados_novos_anterior.xlsx",
            "/datasets/dados_novos_atual.xlsx"
    };
    private static final String SCHEMA_FILE = "/stock_market.owl";   // Na raiz de resources
    private static final String BASE_DATA_FILE = "/ontologiaB3.ttl"; // Na raiz de resources
    // ------------------------------------------------------------------------------------

    // Arquivo de saída para o modelo inferido (será salvo na raiz do projeto)
    private static final String INFERENCE_OUTPUT_FILE = "ontologiaB3_com_inferencia.ttl";

    @PostConstruct
    public void init() {
        logger.info(">>> INICIANDO Inicialização Ontology (@PostConstruct)...");
        lock.writeLock().lock(); // Bloqueio de escrita para inicialização segura
        try {
            baseModel = ModelFactory.createDefaultModel();
            baseModel.setNsPrefix("stock", ONT_PREFIX); // Usa 'stock' como prefixo para a ontologia (consistente com o TTL)
            baseModel.setNsPrefix("rdf", RDF.uri);
            baseModel.setNsPrefix("rdfs", RDFS.uri);
            baseModel.setNsPrefix("owl", OWL.NS);
            baseModel.setNsPrefix("xsd", XSDDatatype.XSD + "#");
            logger.info("   Modelo base criado e prefixos definidos.");

            // 1. Carregar Schema OWL
            loadRdfData(SCHEMA_FILE, Lang.RDFXML, "Schema OWL");

            // 2. Carregar Dados Base TTL
            loadRdfData(BASE_DATA_FILE, Lang.TURTLE, "Dados base TTL");

            // 3. Carregar Dados Dinâmicos das Planilhas de Pregão
            logger.info("--- Iniciando carregamento planilhas de pregão ---");
            for (String filePath : PREGAO_FILES) {
                loadExcelData(filePath); // Chama o método com o caminho corrigido
            }
            logger.info("--- Carregamento planilhas pregão concluído ---");

            long baseSizeBeforeInfer = baseModel.size();
            logger.info("Total triplas BASE (pós-load): {}", baseSizeBeforeInfer);

            validateBaseModelLoad(baseSizeBeforeInfer);

            // 4. Configurar e Aplicar Raciocinador (Inferência)
            logger.info("--- Configurando Reasoner ---");
            Reasoner reasoner = getReasoner();
            logger.info("--- Criando modelo inferência ---");
            infModel = ModelFactory.createInfModel(reasoner, baseModel);
            long infSize = infModel.size();
            long inferredCount = infSize - baseSizeBeforeInfer;
            logger.info("--- Modelo inferência criado. Base:{}, Inferidas:{}, Total:{} ---",
                    baseSizeBeforeInfer, (inferredCount < 0 ? 0 : inferredCount), infSize);

            // 5. Salvar modelo inferido (opcional)
            saveInferredModel();

            logger.info("<<< Ontology INICIALIZADA COM SUCESSO >>>");

        } catch (Exception e) {
            logger.error("!!!!!!!! FALHA GRAVE NA INICIALIZAÇÃO DA ONTOLOGY !!!!!!!!", e);
            baseModel = null;
            infModel = null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void loadRdfData(String resourcePath, Lang language, String description) {
        // Método mantido como estava
        String cleanPath = resourcePath.startsWith("/") ? resourcePath : "/" + resourcePath;
        logger.info("   Tentando carregar {} de classpath: {}", description, cleanPath);
        InputStream in = null;
        try {
            in = Ontology.class.getResourceAsStream(cleanPath);
            if (in == null) {
                logger.error("   !!!!!!!! ARQUIVO ESSENCIAL '{}' ({}) NÃO ENCONTRADO no classpath (src/main/resources) !!!!!!!!!", cleanPath, description);
                if (SCHEMA_FILE.equals(cleanPath) || BASE_DATA_FILE.equals(cleanPath)) { // Considera base TTL essencial também
                    throw new FileNotFoundException("Arquivo RDF essencial não encontrado: " + cleanPath);
                }
                return;
            }
            try (InputStream bis = new BufferedInputStream(in)) {
                RDFDataMgr.read(baseModel, bis, language);
            }
            logger.info("   {} '{}' carregado com sucesso.", description, cleanPath);
        } catch (FileNotFoundException fnfe) {
            throw new RuntimeException(fnfe);
        } catch (JenaException e) {
            logger.error("   Erro de SINTAXE RDF ou Jena ao carregar {} de {}: {}", description, cleanPath, e.getMessage());
            logger.debug("   Detalhes do erro Jena:", e);
            //throw new RuntimeException("Erro de sintaxe no arquivo RDF: " + cleanPath, e); // Descomente se quiser parar em erro de sintaxe
        } catch (IOException e) {
            logger.error("   Erro de I/O ao ler {} de {}", description, cleanPath, e);
        } catch (Exception e) {
            logger.error("   Erro INESPERADO ao carregar {} de {}", description, cleanPath, e);
        } finally {
            if (in != null) {
                try { in.close(); } catch (IOException e) { logger.error("   Erro ao fechar InputStream original para {}", cleanPath, e); }
            }
        }
    }

    private void validateBaseModelLoad(long baseModelSize) {
        // Método mantido como estava
        if (baseModelSize == 0) {
            logger.error("!!!!!!!!!!!!! MODELO BASE ESTÁ VAZIO APÓS CARREGAMENTO! NENHUM DADO FOI CARREGADO. VERIFIQUE OS ARQUIVOS .OWL, .TTL E O PROCESSO DE CARGA DO EXCEL !!!!!!!!!!!!!");
        } else {
            boolean hasPregacoes = false;
            try {
                // Ajusta para usar o prefixo correto se getResource foi modificado
                hasPregacoes = baseModel.listSubjectsWithProperty(RDF.type, getResource("Pregao")).hasNext();
            } catch (Exception e) {
                logger.error("Erro ao verificar a existência de Pregões no modelo", e);
            }

            if (!hasPregacoes && PREGAO_FILES.length > 0) { // Só avisa se deveria ter carregado pregões
                logger.warn("Modelo base contém {} triplas, mas NENHUM recurso do tipo '{}Pregao' foi encontrado. Verifique o método loadExcelData (caminhos, índices, formato de datas/números) e o conteúdo das planilhas Excel.", baseModelSize, ONT_PREFIX);
            } else {
                logger.info("   Validação pós-carga: Modelo base contém {} triplas {}.", baseModelSize, hasPregacoes ? "e instâncias de Pregão foram encontradas" : "(nenhuma instância de Pregão encontrada)");
            }
        }
    }

    private Reasoner getReasoner() {
        // Método mantido como estava
        logger.info("   Usando RDFS Reasoner padrão (ReasonerRegistry.getRDFSReasoner()).");
        return ReasonerRegistry.getRDFSReasoner();
    }

    // --- MÉTODO loadExcelData ---
    // Nenhuma alteração necessária aqui, pois ele recebe o caminho corrigido
    private void loadExcelData(String resourcePath) {
        String cleanPath = resourcePath.startsWith("/") ? resourcePath : "/" + resourcePath;
        logger.info(">> Iniciando carregamento Pregão de: {}", cleanPath);
        int rowsProcessed = 0;
        int errors = 0;
        int skippedTickerFormat = 0; // Contador específico para formato de ticker
        InputStream excelFile = null;

        try {
            excelFile = Ontology.class.getResourceAsStream(cleanPath);
            if (excelFile == null) {
                // Este log agora deve ocorrer apenas se o arquivo realmente não existir no local corrigido
                logger.error("   Arquivo Excel '{}' não encontrado no classpath (verificar src/main/resources{}). Pulando este arquivo.", cleanPath, resourcePath.substring(0,resourcePath.lastIndexOf('/')));
                return;
            }

            try (Workbook workbook = new XSSFWorkbook(excelFile)) {
                Sheet sheet = workbook.getSheetAt(0);
                if (sheet == null) {
                    logger.error("   Planilha 0 não encontrada no arquivo Excel '{}'.", cleanPath);
                    return;
                }
                int lastRowNum = sheet.getLastRowNum();
                logger.info("   ... Processando Planilha '{}' (Última linha física no índice: {})", sheet.getSheetName(), lastRowNum);

                SimpleDateFormat rdfDateFormat = new SimpleDateFormat("yyyy-MM-dd");

                for (int i = 1; i <= lastRowNum; i++) {
                    Row row = sheet.getRow(i);
                    if (row == null) continue;

                    final int tickerColIdx = 4;       // Coluna E
                    final int dataColIdx = 2;       // Coluna C
                    final int openPriceColIdx = 8;  // Coluna I
                    final int highPriceColIdx = 9;  // Coluna J
                    final int lowPriceColIdx = 10;   // Coluna K
                    final int closePriceColIdx = 12; // Coluna M
                    final int quantidadeColIdx = 13; // Coluna N <-- Propriedade: totalNegocios? ou quantidadeNegociada?
                    final int volumeColIdx = 15;    // Coluna P
                    final int tipoAcaoColIdx = 16;   // Coluna Q (ON/PN) <-- NOVA COLUNA

                    Cell tickerCell = row.getCell(tickerColIdx);
                    if (tickerCell == null || tickerCell.getCellType() == CellType.BLANK) continue;

                    String ticker = null;
                    Date dataPregaoDate = null;
                    String tipoAcaoStr = null; // Para armazenar ON/PN

                    try {
                        ticker = getStringCellValue(tickerCell, "Ticker", i + 1);
                        dataPregaoDate = getDateCellValue(row.getCell(dataColIdx), "Data", i + 1, ticker);
                        tipoAcaoStr = getStringCellValue(row.getCell(tipoAcaoColIdx), "TipoAcao", i+1); // Lê a coluna Q

                        // Validação formato Ticker (mantida por enquanto)
                        if (ticker == null || ticker.trim().isEmpty() || !ticker.matches("^[A-Z]{4}\\d{1,2}$")) {
                            skippedTickerFormat++;
                            if (ticker != null && !ticker.trim().isEmpty() && skippedTickerFormat <= 5) { // Loga os 5 primeiros avisos
                                logger.warn("   L{} Pulando: Ticker lido ('{}') não corresponde ao formato esperado AAAA(N)N.", i + 1, ticker);
                            } else if (skippedTickerFormat > 5 && skippedTickerFormat % 100 == 0) { // Loga a cada 100 depois
                                logger.warn("   (L{}...) {} tickers pulados devido a formato inválido até agora...", i + 1, skippedTickerFormat);
                            }
                            errors++; continue;
                        }
                        ticker = ticker.trim().toUpperCase();

                        if (dataPregaoDate == null) {
                            logger.warn("   L{} Pulando: Data inválida para ticker '{}'.", i + 1, ticker);
                            errors++; continue;
                        }

                        double precoAbertura = getNumericCellValue(row.getCell(openPriceColIdx), "PrecoAbertura", i + 1, ticker);
                        double precoMaximo = getNumericCellValue(row.getCell(highPriceColIdx), "PrecoMaximo", i + 1, ticker);
                        double precoMinimo = getNumericCellValue(row.getCell(lowPriceColIdx), "PrecoMinimo", i + 1, ticker);
                        double precoFechamento = getNumericCellValue(row.getCell(closePriceColIdx), "PrecoFechamento", i + 1, ticker);
                        // ** Mapeando Coluna N para totalNegocios (ajustar se semântica for outra) **
                        double totalNegocios = getNumericCellValue(row.getCell(quantidadeColIdx), "TotalNegocios", i + 1, ticker);
                        double volumeTotal = getNumericCellValue(row.getCell(volumeColIdx), "Volume", i + 1, ticker);

                        if (Double.isNaN(precoAbertura) && Double.isNaN(precoMaximo) && Double.isNaN(precoMinimo) &&
                                Double.isNaN(precoFechamento) && Double.isNaN(totalNegocios) && Double.isNaN(volumeTotal)) {
                            logger.warn("   L{} Pulando: Nenhum dado numérico válido para ticker '{}' na data {}.", i + 1, ticker, dataPregaoDate);
                            errors++; continue;
                        }

                        String dataFormatada = rdfDateFormat.format(dataPregaoDate);
                        String valorMobiliarioURI = ONT_PREFIX + ticker;
                        // ** Ajuste URI Negociado para incluir tipo de ação se existir? Ou adicionar tipo no ValorMobiliario **
                        String negociadoURI = ONT_PREFIX + "Negociado_" + ticker + "_" + dataFormatada;
                        String pregaoURI = ONT_PREFIX + "Pregao_" + dataFormatada;

                        // Usa getResource e getProperty com o prefixo 'stock' definido no modelo
                        Resource valorMobiliario = baseModel.createResource(valorMobiliarioURI);
                        Resource negociado = baseModel.createResource(negociadoURI, getResource("Negociado_Em_Pregao"));
                        Resource pregao = baseModel.createResource(pregaoURI, getResource("Pregao"));

                        // Adiciona tipo genérico Valor_Mobiliario OU tipo específico (ON/PN)
                        Resource tipoAcaoResource = getResource("Valor_Mobiliario"); // Padrão
                        if (tipoAcaoStr != null) {
                            if ("ON".equalsIgnoreCase(tipoAcaoStr.trim())) {
                                tipoAcaoResource = getResource("Ordinaria");
                            } else if ("PN".equalsIgnoreCase(tipoAcaoStr.trim())) {
                                tipoAcaoResource = getResource("Preferencial");
                            } // Adicionar mais 'else if' para outros tipos (UNIT, etc.) se necessário
                            else {
                                logger.trace("   L{} Tipo de ação '{}' não reconhecido para ticker {}. Usando tipo genérico.", i + 1, tipoAcaoStr, ticker);
                            }
                        }
                        addPropertyIfNotExist(valorMobiliario, RDF.type, tipoAcaoResource); // Adiciona o tipo específico ou genérico
                        addPropertyIfNotExist(valorMobiliario, getProperty("ticker"), ticker); // Adiciona ticker se não existir

                        addPropertyIfNotExist(pregao, getProperty("ocorreEmData"), ResourceFactory.createTypedLiteral(dataFormatada, XSDDatatype.XSDdate));

                        // Adiciona propriedades de dados ao recurso 'negociado'
                        addNumericPropertyIfValid(negociado, getProperty("precoAbertura"), precoAbertura);
                        addNumericPropertyIfValid(negociado, getProperty("precoMaximo"), precoMaximo);
                        addNumericPropertyIfValid(negociado, getProperty("precoMinimo"), precoMinimo);
                        addNumericPropertyIfValid(negociado, getProperty("precoFechamento"), precoFechamento);
                        addNumericPropertyIfValid(negociado, getProperty("totalNegocios"), totalNegocios); // Mapeado para totalNegocios
                        addNumericPropertyIfValid(negociado, getProperty("volumeNegociacao"), volumeTotal);

                        // Adiciona relações
                        addPropertyIfNotExist(negociado, getProperty("negociadoDurante"), pregao);
                        addPropertyIfNotExist(valorMobiliario, getProperty("negociado"), negociado);

                        rowsProcessed++;

                    } catch (Exception e) {
                        logger.error("   Erro GERAL ao processar linha {} da planilha '{}' (Ticker: {}, Data lida: {}): {}",
                                i + 1, cleanPath, ticker != null ? ticker : "N/A", (dataPregaoDate != null ? dataPregaoDate : "N/A"), e.getMessage(), e);
                        errors++;
                    }
                }

                // Log final por arquivo
                logger.info("<< Pregão {} carregado. {} linhas da planilha lidas, {} linhas processadas com sucesso, {} erros/puladas ({} formato ticker inválido).",
                        cleanPath, sheet.getLastRowNum(), rowsProcessed, errors, skippedTickerFormat);
                if (rowsProcessed == 0 && sheet.getLastRowNum() > 0) {
                    logger.error("!!!!!!!! NENHUMA LINHA PROCESSADA COM SUCESSO EM '{}'. VERIFICAR LOGS !!!!!!!!!!", cleanPath);
                }

            }

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
    }

    // --- Métodos Helper (getStringCellValue, getDateCellValue, etc.) ---
    // Mantidos como estavam, mas adicionada leitura da coluna TipoAcao e
    // ajuste no mapeamento da coluna de Quantidade para a propriedade totalNegocios.
    // ... (Colar aqui os métodos helper da sua versão anterior,
    //      certificando-se que getResource e getProperty usam ONT_PREFIX) ...

    /** Lê o valor de uma célula como String, com tratamento de tipos comuns. */
    private String getStringCellValue(Cell cell, String colName, int rowNum) {
        if (cell == null || cell.getCellType() == CellType.BLANK) return null;
        try {
            CellType cellType = cell.getCellType();
            if (cellType == CellType.FORMULA) cellType = cell.getCachedFormulaResultType();
            switch (cellType) {
                case STRING: String val = cell.getStringCellValue(); return (val != null) ? val.trim() : null;
                case NUMERIC: double numVal = cell.getNumericCellValue(); return (numVal == Math.floor(numVal) && !Double.isInfinite(numVal)) ? String.valueOf((long) numVal) : String.valueOf(numVal);
                case BOOLEAN: return String.valueOf(cell.getBooleanCellValue());
                default: logger.warn("   L{} Col '{}': Tipo {} não esperado para String.", rowNum, colName, cellType); return null;
            }
        } catch (Exception e) { logger.error("   L{} Col '{}': Erro ao ler String: {}", rowNum, colName, e.getMessage()); return null; }
    }

    /** Lê o valor de uma célula como Data, tentando múltiplos formatos. */
    private Date getDateCellValue(Cell cell, String colName, int rowNum, String tickerContext) {
        if (cell == null || cell.getCellType() == CellType.BLANK) return null;
        try {
            CellType cellType = cell.getCellType();
            if (cellType == CellType.FORMULA) cellType = cell.getCachedFormulaResultType();
            if (cellType == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) return cell.getDateCellValue();
            if (cellType == CellType.NUMERIC) {
                double numVal = cell.getNumericCellValue();
                if (DateUtil.isValidExcelDate(numVal)) return DateUtil.getJavaDate(numVal);
                if (numVal > 20000000 && numVal < 22000000 && numVal == Math.floor(numVal)) { // Heurística YYYYMMDD
                    try { SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd"); fmt.setLenient(false); return fmt.parse(String.valueOf((long) numVal)); } catch (ParseException pe) {}
                }
                return parseDateFromString((numVal == Math.floor(numVal)) ? String.valueOf((long)numVal) : String.valueOf(numVal), colName, rowNum, tickerContext);
            }
            if (cellType == CellType.STRING) return parseDateFromString(cell.getStringCellValue().trim(), colName, rowNum, tickerContext);
            logger.warn("   L{} '{}' ({}): Tipo {} inesperado para Data.", rowNum, colName, tickerContext, cellType); return null;
        } catch (Exception e) { logger.error("   L{} '{}' ({}): Erro ao ler Data: {}", rowNum, colName, tickerContext, e.getMessage(), e); return null; }
    }

    /** Tenta parsear uma string de data usando formatos comuns. */
    private Date parseDateFromString(String dateStr, String colName, int rowNum, String tickerContext) {
        if (dateStr == null || dateStr.isEmpty()) return null;
        SimpleDateFormat[] formats = {
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"), new SimpleDateFormat("dd/MM/yyyy HH:mm:ss"),
                new SimpleDateFormat("yyyy-MM-dd"), new SimpleDateFormat("dd/MM/yyyy"), new SimpleDateFormat("yyyyMMdd"),
                new SimpleDateFormat("MM/dd/yy", Locale.US), new SimpleDateFormat("dd-MMM-yyyy", Locale.ENGLISH)
        };
        for (SimpleDateFormat fmt : formats) {
            try { fmt.setLenient(false); return fmt.parse(dateStr); } catch (ParseException pe) {}
        }
        logger.warn("   L{} '{}' ({}): Impossível parsear data da string '{}'.", rowNum, colName, tickerContext, dateStr); return null;
    }

    /** Lê o valor de uma célula como double, com tratamento de erros e limpeza. */
    private double getNumericCellValue(Cell cell, String colName, int rowNum, String tickerContext) {
        if (cell == null || cell.getCellType() == CellType.BLANK) return Double.NaN;
        try {
            CellType cellType = cell.getCellType();
            if (cellType == CellType.FORMULA) cellType = cell.getCachedFormulaResultType();
            switch (cellType) {
                case NUMERIC: return DateUtil.isCellDateFormatted(cell) ? Double.NaN : cell.getNumericCellValue(); // Evita ler datas como números
                case STRING:
                    String val = cell.getStringCellValue().trim();
                    if (val.isEmpty() || val.equals("-") || val.equalsIgnoreCase("N/A")) return Double.NaN;
                    try { return Double.parseDouble(val.replace("R$", "").replace("%", "").replace(".", "").replace(",", ".").trim()); }
                    catch (NumberFormatException e) { logger.warn("   L{} '{}' ({}): String '{}' não é número.", rowNum, colName, tickerContext, cell.getStringCellValue()); return Double.NaN; }
                default: logger.warn("   L{} '{}' ({}): Tipo {} inesperado para Numérico.", rowNum, colName, tickerContext, cellType); return Double.NaN;
            }
        } catch (Exception e) { logger.error("   L{} '{}' ({}): Erro ao ler Numérico: {}", rowNum, colName, tickerContext, e.getMessage()); return Double.NaN; }
    }

    /** Helper para criar URI de Property usando o prefixo da ontologia. */
    private Property getProperty(String localName) {
        if (localName == null || localName.trim().isEmpty()) return null; // Retorna null se inválido
        return ResourceFactory.createProperty(ONT_PREFIX + localName.trim());
    }

    /** Helper para criar URI de Resource (Classe) usando o prefixo da ontologia. */
    private Resource getResource(String localName) {
        if (localName == null || localName.trim().isEmpty()) return null; // Retorna null se inválido
        return ResourceFactory.createResource(ONT_PREFIX + localName.trim());
    }

    /** Adiciona uma propriedade de objeto se ela ainda não existir. */
    private void addPropertyIfNotExist(Resource subject, Property predicate, RDFNode object) {
        if (subject != null && predicate != null && object != null && !baseModel.contains(subject, predicate, object)) {
            subject.addProperty(predicate, object);
        }
    }

    /** Adiciona uma propriedade literal (String) se ela ainda não existir. */
    private void addPropertyIfNotExist(Resource subject, Property predicate, String literalValue) {
        if (subject != null && predicate != null && literalValue != null && !literalValue.isEmpty() && !baseModel.contains(subject, predicate, literalValue)) {
            subject.addProperty(predicate, literalValue);
        }
    }

    /** Adiciona uma propriedade literal com tipo de dado se ela ainda não existir. */
    private void addPropertyIfNotExist(Resource subject, Property predicate, Literal typedLiteral) {
        if (subject != null && predicate != null && typedLiteral != null && !baseModel.contains(subject, predicate, typedLiteral)) {
            subject.addProperty(predicate, typedLiteral);
        }
    }

    /** Adiciona uma propriedade numérica (double) se o valor for válido (não NaN). */
    private void addNumericPropertyIfValid(Resource subject, Property predicate, double value) {
        if (subject != null && predicate != null && !Double.isNaN(value)) {
            Literal literal = baseModel.createTypedLiteral(value);
            subject.addProperty(predicate, literal);
        }
    }

    /** Salva o modelo inferido em arquivo Turtle. */
    private void saveInferredModel() {
        // Método mantido como estava
        logger.info("--- Tentando salvar modelo RDF inferido em {}...", INFERENCE_OUTPUT_FILE);
        Path outputPath = null;
        try {
            Path projectDir = Paths.get(".").toAbsolutePath().normalize();
            outputPath = projectDir.resolve(INFERENCE_OUTPUT_FILE);
            logger.info("   Caminho absoluto para salvar: {}", outputPath);
            if (infModel != null && infModel.size() > 0) {
                Path parentDir = outputPath.getParent();
                if (parentDir != null && !Files.exists(parentDir)) {
                    try { Files.createDirectories(parentDir); } catch (IOException eCreate) { logger.error("   Falha ao criar diretório pai {}.", parentDir, eCreate); return; }
                }
                logger.info("   Salvando {} triplas inferidas...", infModel.size());
                try (OutputStream fos = new BufferedOutputStream(new FileOutputStream(outputPath.toFile()))) {
                    RDFDataMgr.write(fos, infModel, Lang.TURTLE);
                    logger.info("   Modelo RDF inferido salvo com sucesso em {}", outputPath);
                } catch (IOException | JenaException e) { logger.error("   Erro ao ESCREVER o modelo RDF inferido no arquivo {}.", outputPath, e); }
            } else { logger.warn("   Modelo inferido nulo ou vazio. Nada para salvar."); }
        } catch(InvalidPathException ipe) { logger.error("   Caminho de saída '{}' é inválido.", INFERENCE_OUTPUT_FILE, ipe);
        } catch (Exception e) { logger.error("   Erro inesperado durante salvamento do modelo inferido.", e); }
    }

    /**
     * Executa uma consulta SPARQL SELECT no modelo inferido.
     * Retorna uma lista de strings com os valores da variável alvo.
     * Retorna null em caso de erro grave (sintaxe, execução).
     */
    public List<String> executeQuery(String sparqlQuery, String targetVariable) {
        // Método mantido como estava
        lock.readLock().lock();
        try {
            if (infModel == null) { logger.error("ERRO: Modelo de inferência não inicializado."); return null; }
            logger.debug("Executando consulta SPARQL. Variável alvo: '{}'\n---\n{}\n---", targetVariable, sparqlQuery);
            List<String> results = new ArrayList<>();
            Query query;
            try {
                query = QueryFactory.create(sparqlQuery);
            } catch (QueryParseException e) {
                logger.error("   ERRO DE SINTAXE na query SPARQL: {}", e.getMessage());
                logger.error("   Detalhes do erro: Linha {}, Coluna {}", e.getLine(), e.getColumn());
                logger.error("   Query com erro:\n---\n{}\n---", sparqlQuery);
                return null;
            }
            try (QueryExecution qexec = QueryExecutionFactory.create(query, infModel)) {
                ResultSet rs = qexec.execSelect();
                while (rs.hasNext()) {
                    QuerySolution soln = rs.nextSolution();
                    RDFNode node = soln.get(targetVariable);
                    if (node != null) {
                        if (node.isLiteral()) results.add(node.asLiteral().getLexicalForm());
                        else if (node.isResource()) results.add(node.asResource().getURI());
                        else results.add(node.toString());
                    } else {
                        logger.warn("    Variável alvo '{}' não encontrada na solução atual: {}", targetVariable, soln);
                    }
                }
                logger.debug("   Iteração concluída. {} resultado(s) encontrado(s) para '{}'.", results.size(), targetVariable);
            } catch (Exception e) { logger.error("   Erro durante a EXECUÇÃO da query SPARQL.", e); return null; }
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

} // Fim da classe Ontology