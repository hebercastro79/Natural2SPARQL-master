package com.example.Programa_heber.ontology;

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
import java.text.Normalizer; // Para normalizarTextoJava
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern; // Para normalizarTextoJava

@Component
public class Ontology {

    private static final Logger logger = LoggerFactory.getLogger(Ontology.class);

    private Model baseModel;
    private InfModel infModel;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private static final String ONT_PREFIX = "https://dcm.ffclrp.usp.br/lssb/stock-market-ontology#";

    private static final String[] PREGAO_FILES = {
            "/datasets/dados_novos_anterior.xlsx",
            "/datasets/dados_novos_atual.xlsx"
    };
    private static final String SCHEMA_FILE = "/stock_market.owl";
    private static final String BASE_DATA_FILE = "/ontologiaB3.ttl";
    // Ajuste este caminho para onde seu arquivo está em src/main/resources
    private static final String INFO_EMPRESAS_FILE = "/Templates/Informacoes_Empresas.xlsx";
    private static final String INFERENCE_OUTPUT_FILE = "ontologiaB3_com_inferencia.ttl";

    @PostConstruct
    public void init() {
        logger.info(">>> INICIANDO Inicialização Ontology (@PostConstruct)...");
        lock.writeLock().lock();
        try {
            baseModel = ModelFactory.createDefaultModel();
            baseModel.setNsPrefix("stock", ONT_PREFIX);
            baseModel.setNsPrefix("rdf", RDF.uri);
            baseModel.setNsPrefix("rdfs", RDFS.uri);
            baseModel.setNsPrefix("owl", OWL.NS);
            baseModel.setNsPrefix("xsd", XSDDatatype.XSD + "#");
            logger.info("   Modelo base criado e prefixos definidos.");

            loadRdfData(SCHEMA_FILE, Lang.RDFXML, "Schema OWL");
            loadRdfData(BASE_DATA_FILE, Lang.TURTLE, "Dados base TTL");

            logger.info("--- Iniciando carregamento Informações das Empresas ---");
            loadInformacoesEmpresas(INFO_EMPRESAS_FILE);
            logger.info("--- Carregamento Informações das Empresas concluído ---");

            logger.info("--- Iniciando carregamento planilhas de pregão ---");
            for (String filePath : PREGAO_FILES) {
                loadDadosPregaoExcel(filePath);
            }
            logger.info("--- Carregamento planilhas pregão concluído ---");

            long baseSizeBeforeInfer = baseModel.size();
            logger.info("Total triplas BASE (pós-load): {}", baseSizeBeforeInfer);
            validateBaseModelLoad(baseSizeBeforeInfer);

            logger.info("--- Configurando Reasoner ---");
            Reasoner reasoner = getReasoner();
            logger.info("--- Criando modelo inferência ---");
            infModel = ModelFactory.createInfModel(reasoner, baseModel);
            long infSize = infModel.size();
            long inferredCount = infSize - baseSizeBeforeInfer;
            logger.info("--- Modelo inferência criado. Base:{}, Inferidas:{}, Total:{} ---",
                    baseSizeBeforeInfer, Math.max(0, inferredCount), infSize);

            saveInferredModel();
            logger.info("<<< Ontology INICIALIZADA COM SUCESSO >>>");

        } catch (Exception e) {
            logger.error("!!!!!!!! FALHA GRAVE NA INICIALIZAÇÃO DA ONTOLOGY !!!!!!!!", e);
            baseModel = null; infModel = null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void loadRdfData(String resourcePath, Lang language, String description) {
        String cleanPath = resourcePath.startsWith("/") ? resourcePath : "/" + resourcePath;
        logger.info("   Tentando carregar {} de classpath: {}", description, cleanPath);
        try (InputStream in = Ontology.class.getResourceAsStream(cleanPath)) {
            if (in == null) {
                logger.error("   !!!!!!!! ARQUIVO ESSENCIAL '{}' ({}) NÃO ENCONTRADO no classpath !!!!!!!!!", cleanPath, description);
                throw new FileNotFoundException("Arquivo RDF essencial não encontrado: " + cleanPath);
            }
            try (InputStream bis = new BufferedInputStream(in)) {
                RDFDataMgr.read(baseModel, bis, language);
            }
            logger.info("   {} '{}' carregado com sucesso.", description, cleanPath);
        } catch (FileNotFoundException fnfe) {
            throw new RuntimeException("Falha ao carregar RDF essencial: " + cleanPath, fnfe);
        } catch (JenaException e) {
            logger.error("   Erro de SINTAXE RDF ou Jena ao carregar {} de {}: {}", description, cleanPath, e.getMessage());
        } catch (IOException e) {
            logger.error("   Erro de I/O ao ler {} de {}", description, cleanPath, e);
        } catch (Exception e) {
            logger.error("   Erro INESPERADO ao carregar {} de {}", description, cleanPath, e);
        }
    }

    private void loadInformacoesEmpresas(String resourcePath) {
        String cleanPath = resourcePath.startsWith("/") ? resourcePath : "/" + resourcePath;
        logger.info(">> Iniciando carregamento Informações Empresas de: {}", cleanPath);
        int rowsProcessed = 0; int errors = 0;

        try (InputStream excelFile = Ontology.class.getResourceAsStream(cleanPath)) {
            if (excelFile == null) {
                logger.error("   Arquivo Excel de Informações de Empresas '{}' não encontrado. Pulando.", cleanPath);
                return;
            }
            try (Workbook workbook = new XSSFWorkbook(excelFile)) {
                Sheet sheet = workbook.getSheetAt(0);
                if (sheet == null) { logger.error("   Planilha 0 não encontrada em '{}'.", cleanPath); return; }
                logger.info("   ... Processando Planilha '{}' (Última linha física: {})", sheet.getSheetName(), sheet.getLastRowNum());

                // *** AJUSTE ESTES ÍNDICES CONFORME SEU Informacoes_Empresas.xlsx ***
                // Empresa Capital Aberto - Coluna A (índice 0)
                // Codigo Negociacao - Coluna B (índice 1)
                // Setor_Atuacao - Coluna C (índice 2)
                // Setor_Atuacao1 - Coluna D (índice 3)
                // Setor_Atuacao2 - Coluna E (índice 4)
                // Setor_Atuacao3 - Coluna F (índice 5)
                final int nomeEmpresaColIdx = 0;
                final int codigoNegociacaoColIdx = 1;
                final int setorAtuacao3PrincipalColIdx = 5;

                for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                    Row row = sheet.getRow(i);
                    if (row == null) continue;

                    String nomeEmpresaPlanilha = getStringCellValue(row.getCell(nomeEmpresaColIdx), "NomeEmpresa", i + 1);
                    String tickerPlanilha = getStringCellValue(row.getCell(codigoNegociacaoColIdx), "CodigoNegociacao", i + 1);
                    String setorPrincipalPlanilha = getStringCellValue(row.getCell(setorAtuacao3PrincipalColIdx), "SetorAtuacao3Principal", i + 1);

                    if (nomeEmpresaPlanilha == null || nomeEmpresaPlanilha.trim().isEmpty() ||
                            tickerPlanilha == null || tickerPlanilha.trim().isEmpty() ||
                            setorPrincipalPlanilha == null || setorPrincipalPlanilha.trim().isEmpty()) {
                        errors++; continue;
                    }

                    nomeEmpresaPlanilha = nomeEmpresaPlanilha.trim();
                    tickerPlanilha = tickerPlanilha.trim().toUpperCase();
                    setorPrincipalPlanilha = setorPrincipalPlanilha.trim();

                    try {
                        Resource empresaResource = findOrCreateEmpresaResource(nomeEmpresaPlanilha, tickerPlanilha);
                        if(empresaResource == null) {
                            logger.warn("   L{} Pulando: Não foi possível obter/criar recurso para empresa '{}'.", i + 1, nomeEmpresaPlanilha);
                            errors++; continue;
                        }

                        addPropertyIfNotExist(empresaResource, RDF.type, getResource("Empresa_Capital_Aberto"));
                        addPropertyIfNotExist(empresaResource, RDF.type, getResource("Empresa"));
                        addPropertyIfNotExist(empresaResource, RDFS.label, baseModel.createLiteral(nomeEmpresaPlanilha, "pt"));

                        String setorUriNomeNormalizado = normalizarTextoJava(setorPrincipalPlanilha).replace(" ", "_").replaceAll("[^a-z0-9_]", "");
                        Resource setorUriResource = baseModel.createResource(ONT_PREFIX + "Setor_" + setorUriNomeNormalizado);
                        addPropertyIfNotExist(setorUriResource, RDF.type, getResource("Setor_Atuacao"));
                        addPropertyIfNotExist(setorUriResource, RDFS.label, baseModel.createLiteral(setorPrincipalPlanilha, "pt"));

                        addPropertyIfNotExist(empresaResource, getProperty("atuaEm"), setorUriResource);

                        Resource vmResource = findOrCreateValorMobiliarioResource(tickerPlanilha);
                        addPropertyIfNotExist(empresaResource, getProperty("temValorMobiliarioNegociado"), vmResource);

                        rowsProcessed++;
                    } catch (Exception e) {
                        logger.error("   Erro ao processar linha {} de Informacoes_Empresas (Empresa: {}): {}", i + 1, nomeEmpresaPlanilha, e.getMessage(), e);
                        errors++;
                    }
                }
                logger.info("<< Informações de Empresas {} carregado. {} linhas processadas, {} erros.", cleanPath, rowsProcessed, errors);
            }
        } catch (IOException e) {
            logger.error("   Erro de I/O ao ABRIR Informacoes_Empresas {}: {}", cleanPath, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("   Erro inesperado ao processar Informacoes_Empresas {}: {}", cleanPath, e.getMessage(), e);
        }
    }

    private Resource findOrCreateEmpresaResource(String nomeEmpresaPlanilha, String tickerPrincipalDaLinha) {
        String nomeBusca = nomeEmpresaPlanilha;
        StmtIterator iter = baseModel.listStatements(null, RDFS.label, baseModel.createLiteral(nomeBusca, "pt"));
        if (iter.hasNext()) {
            Resource r = iter.nextStatement().getSubject();
            if (r.isURIResource()) {
                logger.trace("Reutilizando Empresa pela label '{}': {}", nomeBusca, r.getURI());
                return r;
            }
        }
        String empresaUriStr = ONT_PREFIX + tickerPrincipalDaLinha.replaceAll("[^A-Za-z0-9]", "") + "_EmpresaEntidade";
        logger.trace("Criando/reutilizando Empresa com URI: {}", empresaUriStr);
        return baseModel.createResource(empresaUriStr);
    }

    // *** MÉTODO normalizarTextoJava CORRIGIDO ***
    private String normalizarTextoJava(String textoInput) {
        if (textoInput == null || textoInput.trim().isEmpty()) {
            return "";
        }
        String lower = textoInput.toLowerCase(Locale.ROOT);
        String nfdNormalizedString = Normalizer.normalize(lower, Normalizer.Form.NFD);
        Pattern patternDiacritics = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
        String semAcentos = patternDiacritics.matcher(nfdNormalizedString).replaceAll("");

        // Remove pontuação e caracteres não alfanuméricos, exceto espaço (que será substituído)
        String semPontuacaoProblematicas = semAcentos.replaceAll("[^a-z0-9\\s]", "");

        String comUnderscores = semPontuacaoProblematicas.trim().replaceAll("\\s+", "_");
        return comUnderscores;
    }

    private void loadDadosPregaoExcel(String resourcePath) {
        String cleanPath = resourcePath.startsWith("/") ? resourcePath : "/" + resourcePath;
        logger.info(">> Iniciando carregamento Pregão de: {}", cleanPath);
        int rowsProcessed = 0; int errors = 0; int skippedTickerFormat = 0;
        try (InputStream excelFile = Ontology.class.getResourceAsStream(cleanPath)) {
            if (excelFile == null) {
                logger.error("   Arquivo Excel de Pregão '{}' não encontrado. Pulando.", cleanPath); return;
            }
            try (Workbook workbook = new XSSFWorkbook(excelFile)) {
                Sheet sheet = workbook.getSheetAt(0);
                if (sheet == null) { logger.error("   Planilha 0 não encontrada em '{}'.", cleanPath); return; }
                logger.info("   ... Processando Planilha '{}' (Última linha física: {})", sheet.getSheetName(), sheet.getLastRowNum());

                SimpleDateFormat rdfDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                    Row row = sheet.getRow(i);
                    if (row == null) continue;

                    // *** AJUSTE ESTES ÍNDICES PARA SEU ARQUIVO DE DADOS DE PREGÃO ***
                    final int tickerColIdx = 3;      // Col D: CODNEG
                    final int dataColIdx = 1;        // Col B: DATPRG
                    final int openPriceColIdx = 7;   // Col H: PREABE
                    final int highPriceColIdx = 8;   // Col I: PREMAX
                    final int lowPriceColIdx = 9;    // Col J: PREMIN
                    final int closePriceColIdx = 11; // Col L: PREULT
                    final int totalNegociosColIdx = 13;
                    final int volumeColIdx = 15;

                    String ticker = null; Date dataPregaoDate = null;
                    try {
                        ticker = getStringCellValue(row.getCell(tickerColIdx), "Ticker", i + 1);
                        Cell dataCell = row.getCell(dataColIdx);
                        if (dataCell != null) {
                            if (dataCell.getCellType() == CellType.NUMERIC) {
                                double numDate = dataCell.getNumericCellValue();
                                if (String.valueOf((long)numDate).matches("^\\d{8}$")) {
                                    try {
                                        dataPregaoDate = new SimpleDateFormat("yyyyMMdd").parse(String.valueOf((long)numDate));
                                    } catch (ParseException e) {
                                        if (DateUtil.isCellDateFormatted(dataCell)) dataPregaoDate = dataCell.getDateCellValue();
                                    }
                                } else if (DateUtil.isCellDateFormatted(dataCell)) {
                                    dataPregaoDate = dataCell.getDateCellValue();
                                }
                            } else if (dataCell.getCellType() == CellType.STRING) {
                                dataPregaoDate = parseDateFromString(dataCell.getStringCellValue().trim(), "Data", i+1, ticker);
                            }
                        }

                        if (ticker == null || !ticker.matches("^[A-Z]{4}\\d{1,2}$")) {
                            skippedTickerFormat++;
                            if (skippedTickerFormat <= 10 || skippedTickerFormat % 50 == 0)
                                logger.warn("   L{} Pulando Ticker: '{}' (Formato inválido ou ausente)", i + 1, ticker);
                            errors++; continue;
                        }
                        ticker = ticker.trim().toUpperCase();
                        if (dataPregaoDate == null) {
                            if(errors < 10 || errors % 50 == 0) logger.warn("   L{} Pulando Ticker {}: Data inválida/ausente.", i+1, ticker);
                            errors++; continue;
                        }

                        double pAbe = getNumericCellValue(row.getCell(openPriceColIdx), "PrecoAbertura", i + 1, ticker);
                        double pMax = getNumericCellValue(row.getCell(highPriceColIdx), "PrecoMaximo", i + 1, ticker);
                        double pMin = getNumericCellValue(row.getCell(lowPriceColIdx), "PrecoMinimo", i + 1, ticker);
                        double pUlt = getNumericCellValue(row.getCell(closePriceColIdx), "PrecoFechamento", i + 1, ticker);
                        double totNeg = getNumericCellValue(row.getCell(totalNegociosColIdx), "TotalNegocios", i + 1, ticker);
                        double volTot = getNumericCellValue(row.getCell(volumeColIdx), "Volume", i + 1, ticker);

                        if (Double.isNaN(pUlt)) { errors++; continue; }

                        String dataFmt = rdfDateFormat.format(dataPregaoDate);
                        Resource valorMobiliarioResource = findOrCreateValorMobiliarioResource(ticker);

                        String negociadoURI = ONT_PREFIX + "Negociado_" + ticker + "_" + dataFmt.replace("-", "");
                        Resource negociadoResource = baseModel.createResource(negociadoURI, getResource("Negociado_Em_Pregao"));
                        Resource pregaoResource = baseModel.createResource(ONT_PREFIX + "Pregao_" + dataFmt.replace("-", ""), getResource("Pregao"));

                        addPropertyIfNotExist(pregaoResource, getProperty("ocorreEmData"), ResourceFactory.createTypedLiteral(dataFmt, XSDDatatype.XSDdate));
                        addPropertyIfNotExist(valorMobiliarioResource, getProperty("negociado"), negociadoResource);
                        addPropertyIfNotExist(negociadoResource, getProperty("negociadoDurante"), pregaoResource);

                        addNumericPropertyIfValid(negociadoResource, getProperty("precoAbertura"), pAbe);
                        addNumericPropertyIfValid(negociadoResource, getProperty("precoMaximo"), pMax);
                        addNumericPropertyIfValid(negociadoResource, getProperty("precoMinimo"), pMin);
                        addNumericPropertyIfValid(negociadoResource, getProperty("precoFechamento"), pUlt);
                        addNumericPropertyIfValid(negociadoResource, getProperty("totalNegocios"), totNeg);
                        addNumericPropertyIfValid(negociadoResource, getProperty("volumeNegociacao"), volTot);

                        rowsProcessed++;
                    } catch (Exception e) {
                        logger.error("   Erro GERAL ao processar linha {} da planilha de pregão '{}' (Ticker: {}): {}",
                                i + 1, cleanPath, (ticker != null ? ticker : "N/A"), e.getMessage());
                        errors++;
                    }
                }
                logger.info("<< Pregão {} carregado. {} linhas lidas, {} processadas, {} erros ({} formato ticker).",
                        cleanPath, sheet.getLastRowNum(), rowsProcessed, errors, skippedTickerFormat);
            }
        } catch (IOException e) {
            logger.error("   Erro de I/O ao ABRIR Excel de Pregão {}", cleanPath, e);
        } catch (Exception e) {
            logger.error("   Erro inesperado ao processar Excel de Pregão {}", cleanPath, e);
        }
    }

    private Resource findOrCreateValorMobiliarioResource(String ticker) {
        String vmUriStr = ONT_PREFIX + ticker;
        Resource vmResource = baseModel.getResource(vmUriStr);
        boolean existsWithType = baseModel.contains(vmResource, RDF.type, getResource("Valor_Mobiliario"));

        if (!existsWithType) {
            vmResource = baseModel.createResource(vmUriStr);
            addPropertyIfNotExist(vmResource, RDF.type, getResource("Valor_Mobiliario"));

            Resource codigoResource = baseModel.createResource(ONT_PREFIX + ticker + "_Codigo");
            addPropertyIfNotExist(codigoResource, RDF.type, getResource("Codigo_Negociacao"));
            addPropertyIfNotExist(codigoResource, getProperty("ticker"), baseModel.createLiteral(ticker));
            addPropertyIfNotExist(vmResource, getProperty("representadoPor"), codigoResource);
        }
        return vmResource;
    }

    private void validateBaseModelLoad(long baseModelSize) {
        if (baseModelSize == 0) {
            logger.error("MODELO BASE VAZIO! Verifique arquivos .OWL/.TTL e carga de Excel.");
        } else {
            boolean hasPregacoes = baseModel.listSubjectsWithProperty(RDF.type, getResource("Pregao")).hasNext();
            logger.info("   Validação: Modelo base {} triplas. Pregões encontrados: {}.", baseModelSize, hasPregacoes);
            if (!hasPregacoes && PREGAO_FILES.length > 0) {
                logger.warn("   NENHUM recurso do tipo '{}Pregao' encontrado. Verifique loadDadosPregaoExcel.", ONT_PREFIX);
            }
        }
    }

    private Reasoner getReasoner() {
        logger.info("   Usando RDFS Reasoner padrão.");
        return ReasonerRegistry.getRDFSReasoner();
    }

    private void saveInferredModel() {
        logger.info("--- Tentando salvar modelo RDF inferido em {}...", INFERENCE_OUTPUT_FILE);
        try {
            Path outputPath = Paths.get(".").toAbsolutePath().normalize().resolve(INFERENCE_OUTPUT_FILE);
            logger.info("   Caminho absoluto para salvar: {}", outputPath);
            if (infModel != null && infModel.size() > 0) {
                Path parentDir = outputPath.getParent();
                if (parentDir != null && !Files.exists(parentDir)) Files.createDirectories(parentDir);
                logger.info("   Salvando {} triplas inferidas...", infModel.size());
                try (OutputStream fos = new BufferedOutputStream(Files.newOutputStream(outputPath))) {
                    RDFDataMgr.write(fos, infModel, Lang.TURTLE);
                    logger.info("   Modelo RDF inferido salvo com sucesso em {}", outputPath);
                }
            } else { logger.warn("   Modelo inferido nulo ou vazio. Nada para salvar."); }
        } catch(Exception e) { logger.error("   Erro ao salvar modelo inferido.", e); }
    }

    public List<String> executeQuery(String sparqlQuery, String targetVariable) {
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
                        StringJoiner sj = new StringJoiner(", ", "( ", " )");
                        rs.getResultVars().forEach(varName -> {
                            RDFNode valNode = soln.get(varName);
                            if (valNode != null) sj.add("? " + varName + " = \"" + valNode.toString() + "\"");
                        });
                        logger.warn("    Variável alvo '{}' não encontrada na solução atual: {}", targetVariable, sj.toString());
                    }
                }
                logger.debug("   Iteração concluída. {} resultado(s) encontrado(s) para '{}'.", results.size(), targetVariable);
            } catch (Exception e) { logger.error("   Erro durante a EXECUÇÃO da query SPARQL.", e); return null; }
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    private String getStringCellValue(Cell cell, String colName, int rowNum) {
        if (cell == null) return null;
        CellType cellTypeToTest = cell.getCellType();
        if (cellTypeToTest == CellType.FORMULA) cellTypeToTest = cell.getCachedFormulaResultType();

        switch (cellTypeToTest) {
            case STRING: return cell.getStringCellValue().trim();
            case NUMERIC: return String.valueOf(cell.getNumericCellValue());
            case BOOLEAN: return String.valueOf(cell.getBooleanCellValue());
            case BLANK: return null;
            default: logger.warn("L{} Col '{}': Tipo {} não tratado para String.", rowNum, colName, cellTypeToTest); return null;
        }
    }

    private Date getDateCellValue(Cell cell, String colName, int rowNum, String tickerContext) {
        if (cell == null || cell.getCellType() == CellType.BLANK) return null;
        try {
            if (cell.getCellType() == CellType.NUMERIC) {
                if (DateUtil.isCellDateFormatted(cell)) return cell.getDateCellValue();
                double numVal = cell.getNumericCellValue();
                if (String.valueOf((long)numVal).matches("^\\d{8}$")) {
                    try { return new SimpleDateFormat("yyyyMMdd").parse(String.valueOf((long)numVal)); }
                    catch (ParseException pe) { /* Ignora e tenta outros formatos */ }
                }
            } else if (cell.getCellType() == CellType.STRING) {
                return parseDateFromString(cell.getStringCellValue().trim(), colName, rowNum, tickerContext);
            }
            logger.warn("   L{} '{}' ({}): Tipo {} não é data válida.", rowNum, colName, tickerContext, cell.getCellType());
            return null;
        } catch (Exception e) {
            logger.warn("   L{} '{}' ({}): Erro ao ler/parsear Data: {}", rowNum, colName, tickerContext, e.getMessage());
            return null;
        }
    }

    private Date parseDateFromString(String dateStr, String colName, int rowNum, String tickerContext) {
        if (dateStr == null || dateStr.isEmpty()) return null;
        SimpleDateFormat[] formats = {
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"), new SimpleDateFormat("dd/MM/yyyy HH:mm:ss"),
                new SimpleDateFormat("yyyy-MM-dd"), new SimpleDateFormat("dd/MM/yyyy"),
                new SimpleDateFormat("yyyyMMdd"), new SimpleDateFormat("MM/dd/yy", Locale.US)
        };
        for (SimpleDateFormat fmt : formats) {
            try { fmt.setLenient(false); return fmt.parse(dateStr); } catch (ParseException ignored) {}
        }
        logger.warn("   L{} '{}' ({}): Impossível parsear data da string '{}'.", rowNum, colName, tickerContext, dateStr); return null;
    }

    private double getNumericCellValue(Cell cell, String colName, int rowNum, String tickerContext) {
        if (cell == null || cell.getCellType() == CellType.BLANK) return Double.NaN;
        try {
            CellType cellType = cell.getCellType();
            if (cellType == CellType.FORMULA) cellType = cell.getCachedFormulaResultType();
            if (cellType == CellType.NUMERIC) {
                return cell.getNumericCellValue();
            } else if (cellType == CellType.STRING) {
                String val = cell.getStringCellValue().trim();
                if (val.isEmpty() || val.equals("-") || val.equalsIgnoreCase("N/A")) return Double.NaN;
                try { return Double.parseDouble(val.replace("R$", "").replace("%", "").replace(".", "").replace(",", ".").trim()); }
                catch (NumberFormatException e) { return Double.NaN; }
            }
            return Double.NaN;
        } catch (Exception e) { return Double.NaN; }
    }

    private Property getProperty(String localName) {
        if (localName == null || localName.trim().isEmpty()) {
            logger.error("Tentativa de criar Property com nome local nulo ou vazio.");
            return ResourceFactory.createProperty(ONT_PREFIX + "ERRO_NOME_PROPRIEDADE_VAZIO");
        }
        return ResourceFactory.createProperty(ONT_PREFIX + localName.trim());
    }
    private Resource getResource(String localName) {
        if (localName == null || localName.trim().isEmpty()) {
            logger.error("Tentativa de criar Resource com nome local nulo ou vazio.");
            return ResourceFactory.createResource(ONT_PREFIX + "ERRO_NOME_RECURSO_VAZIO");
        }
        return ResourceFactory.createResource(ONT_PREFIX + localName.trim());
    }

    private void addPropertyIfNotExist(Resource subject, Property predicate, RDFNode object) {
        if (subject != null && predicate != null && object != null) {
            if (!baseModel.contains(subject, predicate, object)) {
                baseModel.add(subject, predicate, object);
            }
        } else {
            logger.warn("Tentativa de adicionar tripla com sujeito, predicado ou objeto nulo: S:{} P:{} O:{}",
                    (subject != null ? subject.getURI() : "null"),
                    (predicate != null ? predicate.getURI() : "null"),
                    object);
        }
    }

    private void addNumericPropertyIfValid(Resource subject, Property predicate, double value) {
        if (subject != null && predicate != null && !Double.isNaN(value)) {
            baseModel.add(subject, predicate, baseModel.createTypedLiteral(value));
        }
    }
}