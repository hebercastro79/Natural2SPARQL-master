package com.example.Programa_heber.ontology; // Ajuste o package

import jakarta.annotation.PostConstruct;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.util.PrintUtil;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

// Imports POI
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

// Imports Java IO
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File; // <-- IMPORT ADICIONADO
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths; // <-- IMPORT ADICIONADO
import java.nio.file.Files; // Necessário para getScriptPathFromUrl se usado aqui

// Imports Java
import java.net.URL;
import java.net.URISyntaxException; // Necessário para Paths.get(url.toURI())
import java.net.URLDecoder;       // Necessário para fallback de path
import java.text.Normalizer;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.query.*;

@Component
public class Ontology {

    private static final Logger logger = LoggerFactory.getLogger(Ontology.class);

    private OntModel baseOntology;
    private InfModel inferenceModel;

    private static final String ONTOLOGY_PATH = "/stock_market.owl";
    private static final String RULES_PATH = "/regras.rules";
    private static final String BASE_URI = "https://dcm.ffclrp.usp.br/lssb/stock-market-ontology#";

    private static final String DADOS_ATUAL_PATH = "/dados_novos_atual.xlsx";
    private static final String DADOS_ANTERIOR_PATH = "/dados_novos_anterior.xlsx";

    private final Map<String, Resource> empresaUriCache = new ConcurrentHashMap<>();
    private final Map<String, Resource> codigoUriCache = new ConcurrentHashMap<>();

    // --- Constantes RDF ---
    private final Property RDF_TYPE = RDF.type;
    private final Property RDFS_LABEL = RDFS.label;
    private final Property NOME_EMPRESA = ResourceFactory.createProperty(BASE_URI + "nomeEmpresa");
    private final Property TEM_CODIGO = ResourceFactory.createProperty(BASE_URI + "temCodigo");
    private final Property TICKER_PROP = ResourceFactory.createProperty(BASE_URI + "ticker");
    private final Property REF_CODIGO = ResourceFactory.createProperty(BASE_URI + "refCodigo");
    private final Property PRECO_FECHAMENTO = ResourceFactory.createProperty(BASE_URI + "precoFechamento");
    private final Property PRECO_ABERTURA = ResourceFactory.createProperty(BASE_URI + "precoAbertura");
    private final Property PRECO_MAXIMO = ResourceFactory.createProperty(BASE_URI + "precoMaximo");
    private final Property PRECO_MINIMO = ResourceFactory.createProperty(BASE_URI + "precoMinimo");
    private final Property PRECO_MEDIO = ResourceFactory.createProperty(BASE_URI + "precoMedio");
    private final Property DATA_PREGAO = ResourceFactory.createProperty(BASE_URI + "dataPregao");
    private final Property TOTAL_NEGOCIOS = ResourceFactory.createProperty(BASE_URI + "totalNegocios");
    private final Property VOLUME_NEGOCIOS = ResourceFactory.createProperty(BASE_URI + "volumeNegociacao");

    private final Resource CLASS_EMPRESA = ResourceFactory.createResource(BASE_URI + "Empresa");
    private final Resource CLASS_CODIGO_NEGOCIACAO = ResourceFactory.createResource(BASE_URI + "CodigoNegociacao");
    private final Resource CLASS_COTACAO = ResourceFactory.createResource(BASE_URI + "Cotacao");

    // --- Formatters ---
    private static final DateTimeFormatter URI_DATE_FORMATTER_YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter INPUT_DATE_FORMATTER_SLASH = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final DateTimeFormatter ISO_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    public Ontology() {
        logger.debug("Construtor Ontology chamado.");
    }

    @PostConstruct
    public void initializeOntology() {
        logger.info(">>> INICIANDO Inicialização Ontology (@PostConstruct)...");
        try {
            this.baseOntology = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM);
            this.baseOntology.setNsPrefix("stock", BASE_URI);
            this.baseOntology.setNsPrefix("rdf", RDF.getURI());
            this.baseOntology.setNsPrefix("rdfs", RDFS.getURI());
            this.baseOntology.setNsPrefix("owl", OWL.getURI());
            this.baseOntology.setNsPrefix("xsd", XSD.getURI());
            logger.info("   Modelo base criado e prefixos definidos.");

            logger.info("   Tentando carregar schema OWL de: {}", ONTOLOGY_PATH);
            InputStream ontologyStream = getClass().getResourceAsStream(ONTOLOGY_PATH);
            if (ontologyStream != null) {
                try { RDFDataMgr.read(this.baseOntology, ontologyStream, Lang.RDFXML); logger.info("   Schema OWL carregado."); }
                finally { ontologyStream.close(); }
            } else { logger.error("!!! FATAL: ARQUIVO OWL BASE '{}' NÃO ENCONTRADO !!!", ONTOLOGY_PATH); throw new IOException("OWL base não encontrado"); }

            logger.info("--- Iniciando carregamento APENAS das planilhas de pregão ---");
            loadPregaoData(DADOS_ANTERIOR_PATH, this.baseOntology);
            loadPregaoData(DADOS_ATUAL_PATH, this.baseOntology);
            logger.info("--- Carregamento planilhas pregão concluído ---");
            logger.info("Total triplas BASE (pós-planilhas): {}", this.baseOntology.size());

            logger.info("--- Configurando Reasoner ---");
            Reasoner reasoner = ReasonerRegistry.getRDFSReasoner();
            InputStream rulesStream = getClass().getResourceAsStream(RULES_PATH);
            if (rulesStream != null) {
                try { List<Rule> rules = Rule.parseRules(Rule.rulesParserFromReader(new BufferedReader(new InputStreamReader(rulesStream, StandardCharsets.UTF_8)))); if (!rules.isEmpty()) { GenericRuleReasoner rr = new GenericRuleReasoner(rules); reasoner = rr; logger.info("   Reasoner Regras config com {} regras de {}", rules.size(), RULES_PATH); } else { logger.warn("   Arq regras '{}' vazio/inválido. Usando RDFS.", RULES_PATH); } }
                catch (Exception e) { logger.error("   Erro carregar/parsear regras '{}'. Usando RDFS. Erro: {}", RULES_PATH, e.getMessage()); }
                finally { try { rulesStream.close(); } catch (IOException e) { /* ignora */ } }
            } else { logger.warn("   Arq regras '{}' não encontrado. Usando RDFS.", RULES_PATH); }

            logger.info("--- Criando modelo inferência ---");
            this.inferenceModel = ModelFactory.createInfModel(reasoner, this.baseOntology);
            this.inferenceModel.prepare();
            long inferredCount = this.inferenceModel.size() - this.baseOntology.size();
            logger.info("--- Modelo inferência criado. Base:{}, Inferidas:{}, Total:{} ---", this.baseOntology.size(), inferredCount, this.inferenceModel.size());

            saveModelToFile(this.inferenceModel, "ontologiaB3_com_inferencia.ttl", Lang.TURTLE);

            PrintUtil.registerPrefix("stock", BASE_URI);
            logger.info("<<< Ontology INICIALIZADA COM SUCESSO >>>");

        } catch (Exception e) { logger.error("##### ERRO FATAL INICIALIZAÇÃO ONTOLOGIA #####", e); throw new RuntimeException("Falha inicialização Ontologia: " + e.getMessage(), e); }
    }

    // --- MÉTODO DE CARGA DE DADOS DE PREGÃO REFATORADO ---
    private void loadPregaoData(String resourcePath, Model targetModel) {
        logger.info(">> Iniciando carregamento Pregão: {}", resourcePath);
        InputStream excelInputStream = null; int processedRows = 0; int errorCount = 0;
        try {
            excelInputStream = getClass().getResourceAsStream(resourcePath);
            if (excelInputStream == null) { logger.error("!!!! PLANILHA NÃO ENCONTRADA: {} !!!!", resourcePath); return; }
            logger.debug("   Arquivo Pregão OK, lendo...");
            try (Workbook workbook = new XSSFWorkbook(excelInputStream)) {
                Sheet sheet = workbook.getSheetAt(0);
                if (sheet == null) { logger.error("   Planilha 0 não encontrada em {}.", resourcePath); return; }
                logger.info("   ... Processando Planilha Pregão '{}'", sheet.getSheetName());
                boolean isHeader = true;

                for (Row row : sheet) {
                    if (isHeader) { isHeader = false; continue; }
                    int rowNum = row.getRowNum() + 1; logger.trace("-- Proc Pregão L: {} --", rowNum);
                    try {
                        String nomeEmpresaOriginal = getCellValueAsString(row.getCell(6)); // G
                        String dataStr = getCellValueAsString(row.getCell(2));             // C
                        String tickerTmp = getCellValueAsString(row.getCell(4));             // E
                        Double precoAbertura = getCellValueAsDouble(row.getCell(8));    // I
                        Double precoMaximo = getCellValueAsDouble(row.getCell(9));     // J
                        Double precoMinimo = getCellValueAsDouble(row.getCell(10));    // K
                        Double precoMedio = getCellValueAsDouble(row.getCell(11));     // L
                        Double precoFechamento = getCellValueAsDouble(row.getCell(12));  // M
                        Long totalNegocios = getCellValueAsLong(row.getCell(13));     // N
                        Double volumeNegocios = getCellValueAsDouble(row.getCell(14));   // O

                        if (isEmpty(tickerTmp) || !isTickerValid(tickerTmp)) { logger.warn("   L{} Pregão: Ticker inválido/vazio ('{}').", rowNum, tickerTmp); continue; }
                        if (isEmpty(dataStr)) { logger.warn("   L{} Pregão: Data vazia (Ticker: {}).", rowNum, tickerTmp); continue; }
                        if (isEmpty(nomeEmpresaOriginal)) { logger.warn("   L{} Pregão: Nome empresa vazio (Ticker: {}). Usando ticker.", rowNum, tickerTmp); nomeEmpresaOriginal = tickerTmp; }

                        final String ticker = tickerTmp.trim().toUpperCase();
                        final String nomeEmpresaLimpo = nomeEmpresaOriginal.trim();
                        String nomeNormalizado = normalizeJava(nomeEmpresaLimpo);
                        if (nomeNormalizado == null) { logger.warn("   L{} Pregão: Nome norm nulo p/ '{}'. Usando ticker.", rowNum, nomeEmpresaLimpo); nomeNormalizado = ticker; }
                        final String nomeNormalizadoFinal = nomeNormalizado;

                        String empresaUriKey = "Empresa_" + nomeNormalizadoFinal;
                        Resource empresaResource = empresaUriCache.computeIfAbsent(empresaUriKey, k -> {
                            Resource newEmp = targetModel.createResource(BASE_URI + k);
                            addTripleIfNotExists(targetModel, newEmp, RDF_TYPE, CLASS_EMPRESA);
                            addTripleIfNotExists(targetModel, newEmp, RDFS_LABEL, targetModel.createLiteral(nomeEmpresaLimpo, "pt"));
                            logger.debug("   +++ Criada Empresa <...{}> label '{}'", k, nomeEmpresaLimpo);
                            return newEmp;
                        });
                        if (!targetModel.contains(empresaResource, RDFS_LABEL, targetModel.createLiteral(nomeEmpresaLimpo, "pt"))) {
                            targetModel.removeAll(empresaResource, RDFS_LABEL, null);
                            addTripleIfNotExists(targetModel, empresaResource, RDFS_LABEL, targetModel.createLiteral(nomeEmpresaLimpo, "pt"));
                        }

                        String codigoUriKey = "Codigo_" + ticker;
                        Resource codigoResource = codigoUriCache.computeIfAbsent(codigoUriKey, k -> {
                            Resource newCodigo = targetModel.createResource(BASE_URI + k);
                            addTripleIfNotExists(targetModel, newCodigo, RDF_TYPE, CLASS_CODIGO_NEGOCIACAO);
                            addTripleIfNotExists(targetModel, newCodigo, TICKER_PROP, targetModel.createTypedLiteral(ticker));
                            logger.debug("   +++ Criado Código <...{}> ticker '{}'", k, ticker);
                            return newCodigo;
                        });
                        addTripleIfNotExists(targetModel, empresaResource, TEM_CODIGO, codigoResource);

                        String dataYYYYMMDD = resolveDataToUriFormatYYYYMMDD(dataStr);
                        if (dataYYYYMMDD == null) { logger.warn("   L{} Pregão: Data inválida p/ URI cotação ('{}').", rowNum, dataStr); continue; }
                        String cotacaoUriKey = "Cotacao_" + ticker + "_" + dataYYYYMMDD;
                        Resource cotacaoResource = targetModel.createResource(BASE_URI + cotacaoUriKey);

                        addTripleIfNotExists(targetModel, cotacaoResource, RDF_TYPE, CLASS_COTACAO);
                        addTripleIfNotExists(targetModel, cotacaoResource, REF_CODIGO, codigoResource);
                        LocalDate dataDate = parseDateFromVariousFormats(dataStr);
                        if (dataDate != null) { addTripleIfNotExists(targetModel, cotacaoResource, DATA_PREGAO, targetModel.createTypedLiteral(dataDate.format(ISO_DATE_FORMATTER), XSD.date.getURI())); }
                        else { logger.warn("   L{} Pregão: Data '{}' inválida p/ literal cotação.", rowNum, dataStr); }

                        Optional.ofNullable(precoAbertura).ifPresent(v -> addTripleIfNotExists(targetModel, cotacaoResource, PRECO_ABERTURA, ResourceFactory.createTypedLiteral(v)));
                        Optional.ofNullable(precoMaximo).ifPresent(v -> addTripleIfNotExists(targetModel, cotacaoResource, PRECO_MAXIMO, ResourceFactory.createTypedLiteral(v)));
                        Optional.ofNullable(precoMinimo).ifPresent(v -> addTripleIfNotExists(targetModel, cotacaoResource, PRECO_MINIMO, ResourceFactory.createTypedLiteral(v)));
                        Optional.ofNullable(precoMedio).ifPresent(v -> addTripleIfNotExists(targetModel, cotacaoResource, PRECO_MEDIO, ResourceFactory.createTypedLiteral(v)));
                        Optional.ofNullable(precoFechamento).ifPresent(v -> addTripleIfNotExists(targetModel, cotacaoResource, PRECO_FECHAMENTO, ResourceFactory.createTypedLiteral(v)));
                        Optional.ofNullable(totalNegocios).ifPresent(v -> addTripleIfNotExists(targetModel, cotacaoResource, TOTAL_NEGOCIOS, ResourceFactory.createTypedLiteral(v)));
                        Optional.ofNullable(volumeNegocios).ifPresent(v -> addTripleIfNotExists(targetModel, cotacaoResource, VOLUME_NEGOCIOS, ResourceFactory.createTypedLiteral(v)));

                        processedRows++;
                    } catch (Exception e) { logger.error("   Erro proc L{} Pregão {}: {}", rowNum, resourcePath, e.getMessage(), e); errorCount++; }
                } // fim for row
            } // fim try-with-workbook
        } catch (IOException e) { logger.error("   Erro IO ler Pregão '{}'", resourcePath, e); }
        finally { if (excelInputStream != null) { try { excelInputStream.close(); } catch (IOException e) { /* Ignora */ } } logger.info("<< Pregão {} carregado. {} linhas ok, {} erros.", resourcePath, processedRows, errorCount); }
    }
    // --- FIM MÉTODO DE CARGA REFATORADO ---


    // --- Métodos Utilitários ---

    private boolean addTripleIfNotExists(Model m, Resource s, Property p, RDFNode o) {
        if (s == null || p == null || o == null) { logger.warn("Add Tripla Nula: S={}, P={}, O={}", s, p, o); return false; }
        if (!m.contains(s, p, o)) {
            m.add(s, p, o);
            logger.trace("   +++ Add Triple: S=<{}> P=<{}> O={}", s.isAnon()?"[]":s.getURI(), p.getURI(), o.toString().length()>100?o.toString().substring(0,97)+"...":o.toString());
            return true;
        } else {
            logger.trace("   --- Triple Exists: S=<{}> P=<{}> O={}", s.isAnon()?"[]":s.getURI(), p.getURI(), o.toString().length()>100?o.toString().substring(0,97)+"...":o.toString());
            return false;
        }
    }

    private String getCellValueAsString(Cell cell) {
        if (cell == null) return null; CellType ct = cell.getCellType();
        if(ct==CellType.FORMULA){try{ct=cell.getCachedFormulaResultType();}catch(Exception e){try{FormulaEvaluator ev=cell.getSheet().getWorkbook().getCreationHelper().createFormulaEvaluator();CellValue cv=ev.evaluate(cell);ct=cv.getCellType();}catch(Exception e2){logger.error("Erro eval formula {}: {}",cell.getAddress(),e2.getMessage());return null;}}}
        try {
            switch(ct){
                case STRING:return cell.getStringCellValue().trim();
                case NUMERIC:if(DateUtil.isCellDateFormatted(cell)){try{return cell.getDateCellValue().toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDate().format(ISO_DATE_FORMATTER);}catch(Exception e){double v=cell.getNumericCellValue();return(v==Math.floor(v)&&!Double.isInfinite(v))?String.valueOf((long)v):String.valueOf(v);}}else{double v=cell.getNumericCellValue();return(v==Math.floor(v)&&!Double.isInfinite(v))?String.valueOf((long)v):String.valueOf(v);}
                case BOOLEAN:return String.valueOf(cell.getBooleanCellValue());
                case BLANK:return null;
                case ERROR:return null; // Ignora células com erro
                default:return null;
            }
        }catch(Exception e){logger.error("Erro ler cel {}: {}",cell.getAddress(),e.getMessage());return null;}
    }

    private Optional<Double> getCellValueAsOptionalDouble(Cell cell) {
        String s=getCellValueAsString(cell); if(isEmpty(s)) return Optional.empty();
        try { return Optional.of(Double.parseDouble(s.replace(',','.').trim())); }
        catch (NumberFormatException e) { logger.trace("Falha conv Double:'{}'. Cell:{}", s, cell!=null?cell.getAddress():"N/A"); return Optional.empty(); }
    }

    private Double getCellValueAsDouble(Cell cell) { return getCellValueAsOptionalDouble(cell).orElse(null); }

    private Optional<Long> getCellValueAsOptionalLong(Cell cell) {
        String s=getCellValueAsString(cell); if(isEmpty(s)) return Optional.empty();
        try { return Optional.of(Double.valueOf(s.replace(',','.').trim()).longValue()); } // Converte Double para Long
        catch (NumberFormatException | NullPointerException e) { logger.trace("Falha conv Long:'{}'. Cell:{}", s, cell!=null?cell.getAddress():"N/A"); return Optional.empty(); }
    }

    private Long getCellValueAsLong(Cell cell) { return getCellValueAsOptionalLong(cell).orElse(null); }

    private LocalDate parseDateFromVariousFormats(String ds) {
        if(isEmpty(ds)) return null; String d=ds.trim();
        try {return LocalDate.parse(d,ISO_DATE_FORMATTER);} catch (DateTimeParseException e1){
            try {return LocalDate.parse(d,INPUT_DATE_FORMATTER_SLASH);} catch (DateTimeParseException e2){
                if(d.matches("^\\d{8}$")){try{return LocalDate.parse(d,DateTimeFormatter.BASIC_ISO_DATE);}catch(DateTimeParseException e3){}}
                String l=d.toLowerCase(); if(l.contains("hoje")||l.contains("hj")) return LocalDate.now(); if(l.contains("ontem")) return LocalDate.now().minusDays(1);
                logger.warn("Data '{}' não parseada.",d); return null;}}
    }

    private String resolveDataToUriFormatYYYYMMDD(String di) {
        LocalDate d=parseDateFromVariousFormats(di); if(d!=null) return d.format(URI_DATE_FORMATTER_YYYYMMDD);
        String l=isEmpty(di)?"":di.trim().toLowerCase(); if(l.contains("hoje")) return LocalDate.now().format(URI_DATE_FORMATTER_YYYYMMDD); if(l.contains("ontem")) return LocalDate.now().minusDays(1).format(URI_DATE_FORMATTER_YYYYMMDD);
        logger.warn("Data não resolvida p/ URI: '{}'",di); return null;
    }

    private boolean isEmpty(String s) { return s==null||s.trim().isEmpty(); }

    // Método createUriSafe corrigido
    private String createUriSafe(String input) {
        if (isEmpty(input)) return "id_" + UUID.randomUUID().toString().substring(0, 8); // Correto
        String normalized = Normalizer.normalize(input.trim(), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
        String sanitized = normalized.replaceAll("[^a-zA-Z0-9_\\-\\.]+", "_").replaceAll("\\s+", "_").replaceAll("_+", "_").replaceAll("^_|_$", "");
        if (sanitized.isEmpty()) return "gen_" + UUID.randomUUID().toString().substring(0, 8); // Correto
        if (!sanitized.isEmpty() && !Character.isLetter(sanitized.charAt(0)) && sanitized.charAt(0) != '_') { // Garante começar com letra ou _
            sanitized = "id_" + sanitized;
        }
        return sanitized;
    }

    private String normalizeJava(String text) {
        if (text == null) return null; String n = text.toUpperCase().trim();
        try { n = Normalizer.normalize(n, Normalizer.Form.NFD).replaceAll("\\p{M}", ""); }
        catch (Exception e) { logger.warn("Falha norm acentos Java: '{}'", text, e); }
        n = n.replaceAll("\\b(S\\.?A\\.?|S/?A|CIA\\.?|COMPANHIA|LTDA\\.?|ON|PN|N[12]|PREF\\.?|ORD\\.?|NM|ED|EJ|MA)\\b", "");
        n = n.replaceAll("[^\\p{Alnum}]+", ""); // Apenas alfanuméricos Unicode
        return n.isEmpty() ? null : n;
    }

    private boolean isTickerValid(String ticker){
        return ticker != null && ticker.matches("^[A-Z]{4}\\d{1,2}$");
    }

    // Método saveModelToFile corrigido com imports
    private void saveModelToFile(Model model, String filename, Lang lang) {
        logger.info("--- Tentando salvar modelo RDF em {}...", filename);
        String outputPath = filename; // Diretório atual por padrão
        try {
            URL classesUrl = Ontology.class.getProtectionDomain().getCodeSource().getLocation();
            if (classesUrl != null && "file".equals(classesUrl.getProtocol())) {
                File classesDir = Paths.get(classesUrl.toURI()).toFile(); // Usa Paths e File importados
                File projectDir = classesDir.getParentFile().getParentFile();
                if (projectDir != null && projectDir.isDirectory()) { // Verifica se projectDir não é nulo
                    outputPath = new File(projectDir, filename).getAbsolutePath(); // Usa File importado
                    logger.info("   Diretório do projeto detectado, salvando em: {}", outputPath);
                } else {
                    logger.warn("   Não foi possível determinar diretório do projeto, salvando no diretório atual: {}", new File(outputPath).getAbsolutePath());
                }
            } else {
                logger.warn("   Não foi possível obter localização das classes, salvando no diretório atual: {}", new File(outputPath).getAbsolutePath());
            }
        } catch(Exception e) {
            logger.warn("   Erro ao determinar diretório do projeto, salvando no diretório atual: {}", outputPath, e);
        }

        try (OutputStream fos = new FileOutputStream(outputPath)) {
            logger.info("   Salvando {} triplas...", model.size());
            RDFDataMgr.write(fos, model, lang);
            logger.info("   Modelo RDF salvo com sucesso em {}", outputPath);
        } catch (IOException e) {
            logger.error("##### ERRO AO SALVAR MODELO RDF EM {} #####", outputPath, e);
        }
    }

    // --- Métodos de Consulta ---
    public List<String> queryAndExtractList(Query query, String targetVariable) {
        List<String> results = new ArrayList<>(); Model modelToQuery = getModel();
        if (modelToQuery == null) { logger.error("Modelo NULO!"); return results; }
        logger.debug("Exec query no modelo '{}'. Var: '{}'", (modelToQuery==inferenceModel?"INF":"BASE"), targetVariable); logger.trace("Query:\n{}", query);
        try (QueryExecution qe = QueryExecutionFactory.create(query, modelToQuery)) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution soln = rs.nextSolution(); RDFNode node = soln.get(targetVariable);
                if (node!=null) { if(node.isLiteral()) results.add(node.asLiteral().getLexicalForm()); else if (node.isResource()) results.add(node.asResource().getURI()); else results.add(node.toString()); }
                else { logger.warn("Var '{}' nula na solução.", targetVariable); results.add(null); }
            }
        } catch (Exception e) { logger.error("Erro query SPARQL", e); logger.error("Query:\n{}", query); }
        logger.debug("Consulta concluída. {} resultado(s) p/ '{}'.", results.size(), targetVariable); return results;
    }

    public Model getModel() { if (inferenceModel != null) return inferenceModel; logger.warn("Modelo inferência NULO, usando base."); return baseOntology; }
    public String getBaseUri() { return BASE_URI; }
    public String getPredicateURI(String n) { return (isEmpty(n)) ? null : BASE_URI + n; }

} // Fim da classe Ontology