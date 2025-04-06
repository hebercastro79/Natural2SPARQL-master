package com.example.Programa_heber.ontology;

import jakarta.annotation.PostConstruct;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shared.JenaException;
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

// Imports Java
import java.net.URL;
import java.text.Normalizer;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.jena.query.*;

@Component
public class StockMarketOntology {

    private static final Logger logger = LoggerFactory.getLogger(StockMarketOntology.class);

    private OntModel baseOntology;
    private InfModel inferenceModel;

    private static final String ONTOLOGY_PATH = "/stock_market.owl";
    private static final String RULES_PATH = "/regras.rules";
    private static final String BASE_URI = "https://dcm.ffclrp.usp.br/lssb/stock-market-ontology#";

    private static final String DADOS_ATUAL_PATH = "/dados_novos_atual.xlsx";
    private static final String DADOS_ANTERIOR_PATH = "/dados_novos_anterior.xlsx";
    private static final String INFO_EMPRESAS_PATH = "/Informacoes_Empresas.xlsx";

    // --- Constantes RDF ---
    private final Property RDF_TYPE = RDF.type;
    private final Property TEM_VALOR_MOBILIARIO;
    private final Property NEGOCIADO_PROP;
    private final Property REPRESENTADO_POR;
    private final Property CODIGO_NEGOCIACAO_TICKER;

    private final Property NOME_EMPRESA_PROP = RDFS.label;
    private final Property PRECO_FECHAMENTO;
    private final Property PRECO_ABERTURA;
    private final Property PRECO_MAXIMO;
    private final Property PRECO_MINIMO;
    private final Property PRECO_MEDIO;
    private final Property NEGOCIADO_DURANTE;

    private final Property OCORRE_EM_DATA;
    private final Property TOTAL_NEGOCIOS;
    private final Property VOLUME_NEGOCIOS;
    private final Property ATUA_EM;
    private final Resource CLASS_EMPRESA;
    private final Resource CLASS_VALOR_MOBILIARIO_NEGOCIADO;
    private final Resource CLASS_VALOR_MOBILIARIO;
    private final Resource CLASS_NEGOCIADO_EM_PREGAO;
    private final Resource CLASS_PREGAO;
    private final Resource CLASS_CODIGO_NEGOCIACAO;
    private final Resource CLASS_ACAO_ORDINARIA;
    private final Resource CLASS_ACAO_PREFERENCIAL;
    private final Resource CLASS_SETOR_ATUACAO;

    // --- Formatters ---
    // Formato YYYYMMDD para partes da URI (Negociado, Pregão URI)
    private static final DateTimeFormatter URI_DATE_FORMATTER_YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");
    // Formato dd/MM/yyyy para parse de entrada
    private static final DateTimeFormatter INPUT_DATE_FORMATTER_SLASH = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    // Formato ISO YYYY-MM-DD para parse de entrada e para criar literais xsd:date
    private static final DateTimeFormatter ISO_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;


    public StockMarketOntology() {
        logger.debug("Construtor StockMarketOntology chamado.");
        // Inicializa propriedades e classes (ocorreEmData é Datatype)
        this.TEM_VALOR_MOBILIARIO = ResourceFactory.createProperty(BASE_URI + "temValorMobiliarioNegociado");
        this.NEGOCIADO_PROP = ResourceFactory.createProperty(BASE_URI + "negociado");
        this.REPRESENTADO_POR = ResourceFactory.createProperty(BASE_URI + "representadoPor");
        this.CODIGO_NEGOCIACAO_TICKER = ResourceFactory.createProperty(BASE_URI + "ticker");
        // NOME_EMPRESA_PROP já definido como RDFS.label acima
        this.PRECO_FECHAMENTO = ResourceFactory.createProperty(BASE_URI + "precoFechamento");
        this.PRECO_ABERTURA = ResourceFactory.createProperty(BASE_URI + "precoAbertura");
        this.PRECO_MAXIMO = ResourceFactory.createProperty(BASE_URI + "precoMaximo");
        this.PRECO_MINIMO = ResourceFactory.createProperty(BASE_URI + "precoMinimo");
        this.PRECO_MEDIO = ResourceFactory.createProperty(BASE_URI + "precoMedio");
        this.NEGOCIADO_DURANTE = ResourceFactory.createProperty(BASE_URI + "negociadoDurante");
        this.OCORRE_EM_DATA = ResourceFactory.createProperty(BASE_URI + "ocorreEmData");
        this.TOTAL_NEGOCIOS = ResourceFactory.createProperty(BASE_URI + "totalNegocios");
        this.VOLUME_NEGOCIOS = ResourceFactory.createProperty(BASE_URI + "volumeNegociacao");
        this.ATUA_EM = ResourceFactory.createProperty(BASE_URI + "atuaEm");
        this.CLASS_EMPRESA = ResourceFactory.createResource(BASE_URI + "Empresa_Capital_Aberto");
        this.CLASS_VALOR_MOBILIARIO_NEGOCIADO = ResourceFactory.createResource(BASE_URI + "Valor_Mobiliario_Negociado");
        this.CLASS_VALOR_MOBILIARIO = ResourceFactory.createResource(BASE_URI + "Valor_Mobiliario");
        this.CLASS_NEGOCIADO_EM_PREGAO = ResourceFactory.createResource(BASE_URI + "Negociado_Em_Pregao");
        this.CLASS_PREGAO = ResourceFactory.createResource(BASE_URI + "Pregao");
        this.CLASS_CODIGO_NEGOCIACAO = ResourceFactory.createResource(BASE_URI + "Codigo_Negociacao");
        this.CLASS_ACAO_ORDINARIA = ResourceFactory.createResource(BASE_URI + "Ordinaria");
        this.CLASS_ACAO_PREFERENCIAL = ResourceFactory.createResource(BASE_URI + "Preferencial");
        this.CLASS_SETOR_ATUACAO = ResourceFactory.createResource(BASE_URI + "Setor_Atuacao");
    }

    @PostConstruct
    public void initializeOntology() {
        logger.info(">>> INICIANDO Inicialização StockMarketOntology (@PostConstruct)...");
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
                RDFDataMgr.read(this.baseOntology, ontologyStream, Lang.RDFXML);
                logger.info("   Schema OWL carregado com sucesso de {}", ONTOLOGY_PATH);
                ontologyStream.close();
            } else {
                logger.error("!!! FATAL: ARQUIVO OWL NÃO ENCONTRADO: {} !!!", ONTOLOGY_PATH);
                throw new IOException("Arquivo OWL não encontrado: " + ONTOLOGY_PATH);
            }

            logger.info("--- Iniciando carregamento das planilhas ---");
            loadCompanyInfo(INFO_EMPRESAS_PATH, this.baseOntology);
            loadPregaoData(DADOS_ANTERIOR_PATH, this.baseOntology);
            loadPregaoData(DADOS_ATUAL_PATH, this.baseOntology);
            logger.info("--- Carregamento das planilhas concluído ---");
            logger.info("Total de triplas no modelo BASE (pós-planilhas): {}", this.baseOntology.size());

            // --- RESTAURADO: Configuração completa do Reasoner ---
            logger.info("--- Configurando Reasoner ---");
            Reasoner reasoner;
            URL rulesURL = null;
            try {
                // Tenta obter o recurso como URL
                rulesURL = getClass().getResource(RULES_PATH);
                if (rulesURL == null) {
                    // Se falhar (comum em JARs), tenta como InputStream e loga
                    InputStream rulesStream = getClass().getResourceAsStream(RULES_PATH);
                    if (rulesStream != null) {
                        logger.warn("   Arquivo de regras '{}' encontrado como InputStream, mas não como URL direta. Regras não serão carregadas por URL.", RULES_PATH);
                        rulesStream.close();
                    } else {
                        logger.warn("   Arquivo de regras '{}' não encontrado nem como URL nem como InputStream.", RULES_PATH);
                    }
                }
            } catch (Exception e) {
                logger.warn("   Erro ao obter URL do arquivo de regras '{}': {}", RULES_PATH, e.getMessage());
                rulesURL = null; // Garante que é nulo em caso de erro
            }

            if (rulesURL != null) {
                try {
                    logger.info("   Encontrado arquivo de regras via URL: {}", rulesURL.toExternalForm());
                    List<Rule> rules = Rule.rulesFromURL(rulesURL.toString());
                    if (rules.isEmpty()) {
                        logger.warn("   Arquivo de regras '{}' carregado via URL, mas não contém regras válidas. Usando RDFS Reasoner.", RULES_PATH);
                        reasoner = ReasonerRegistry.getRDFSReasoner();
                    } else {
                        GenericRuleReasoner ruleReasoner = new GenericRuleReasoner(rules);

                        reasoner = ruleReasoner;
                        logger.info("   Reasoner de Regras configurado com {} regras via URL.", rules.size());
                    }
                } catch (Exception e) {
                    logger.error("   Erro ao carregar ou configurar regras da URL '{}'. Usando RDFS Reasoner padrão.", rulesURL.toString(), e);
                    reasoner = ReasonerRegistry.getRDFSReasoner(); // Fallback para RDFS
                }
            } else {
                logger.warn("   Arquivo de regras '{}' não encontrado ou inacessível via URL. Usando RDFS Reasoner padrão.", RULES_PATH);
                reasoner = ReasonerRegistry.getRDFSReasoner();
            }
            // --- Fim da configuração do Reasoner ---

            logger.info("--- Criando modelo de inferência ---");
            this.inferenceModel = ModelFactory.createInfModel(reasoner, this.baseOntology); // Usa o reasoner configurado
            // Forçar uma computação inicial pode ser útil para alguns reasoners
            this.inferenceModel.prepare();
            long inferredCount = this.inferenceModel.size() - this.baseOntology.size();
            logger.info("--- Modelo de inferência criado e preparado. Base:{}, Inferidas:{}, Total:{} ---", this.baseOntology.size(), inferredCount, this.inferenceModel.size());

            // --- RESTAURADO: Salvar modelo final para depuração ---
            logger.info("--- Tentando salvar modelo RDF final (com inferências) em arquivo... ---");
            Model modelToSave = this.inferenceModel;
            String outputFilename = "ontologiaB3.ttl";
            try (OutputStream fos = new FileOutputStream(outputFilename)) {
                logger.info("   Salvando modelo RDF ({} triplas) em: {}", modelToSave.size(), outputFilename);
                RDFDataMgr.write(fos, modelToSave, Lang.TURTLE);
                logger.info("   Modelo RDF salvo com sucesso em {}", outputFilename);
            } catch (IOException e) {
                logger.error("##### ERRO AO SALVAR MODELO RDF FINAL EM {} #####", outputFilename, e);
            }
            // --- Fim do bloco de salvamento ---

            PrintUtil.registerPrefix("stock", BASE_URI);
            logger.info("<<< StockMarketOntology INICIALIZADA COM SUCESSO >>>");

        } catch (Exception e) {
            logger.error("##### ERRO FATAL INICIALIZAÇÃO ONTOLOGIA #####", e);

            throw new RuntimeException("Falha grave na inicialização da Ontologia: " + e.getMessage(), e);
        }
    }


    private void loadCompanyInfo(String resourcePath, Model targetModel) {
        logger.info(">> Iniciando carregamento Info Empresas: {}", resourcePath);
        InputStream excelInputStream = null;
        int processedRows = 0;
        int errorCount = 0;
        try {
            excelInputStream = getClass().getResourceAsStream(resourcePath);
            if (excelInputStream == null) {
                logger.error("!!!! PLANILHA INFO EMPRESAS NÃO ENCONTRADA: {} !!!!", resourcePath);
                return;
            }
            logger.debug("   Arquivo Info Empresas OK, lendo...");
            try (Workbook workbook = new XSSFWorkbook(excelInputStream)) {
                Sheet sheet = workbook.getSheetAt(0);
                if (sheet == null) {
                    logger.error("   Planilha 0 na Info Empresas não encontrada em {}.", resourcePath);
                    return;
                }
                logger.info("   ... Processando Planilha Info Empresas '{}'", sheet.getSheetName());
                boolean isHeader = true;


                for (Row row : sheet) {
                    if (isHeader) {
                        isHeader = false;
                        logger.trace("   Pulando header."); // Trace
                        continue;
                    }
                    int rowNum = row.getRowNum() + 1;
                    logger.trace("-- Proc Info Row: {} --", rowNum); // Trace
                    try {
                        String nomeEmpresa = getCellValueAsString(row.getCell(0));
                        String codigoTicker = getCellValueAsString(row.getCell(1));
                        String setorNome = getCellValueAsString(row.getCell(2));
                        logger.trace("   L{} Raw Data: Nome='{}', Ticker='{}', Setor='{}'", rowNum, nomeEmpresa, codigoTicker, setorNome); // Trace

                        if (isEmpty(codigoTicker)) {
                            logger.warn("   L{} InfoEmpresas: Ticker vazio. Linha ignorada.", rowNum);
                            continue;
                        }
                        if (isEmpty(nomeEmpresa)) {
                            logger.warn("   L{} InfoEmpresas: Nome da empresa vazio (Ticker: {}). Usando Ticker como fallback para Label.", rowNum, codigoTicker);
                            nomeEmpresa = codigoTicker; // Fallback para garantir label
                        }

                        // URIs
                        String empresaUriPart = createUriSafe(codigoTicker); // Usa Ticker como ID
                        Resource empresaResource = targetModel.createResource(BASE_URI + empresaUriPart);
                        String codigoUriPart = createUriSafe(codigoTicker + "_Code");
                        Resource codigoResource = targetModel.createResource(BASE_URI + codigoUriPart);
                        Resource setorResource = null;
                        if (!isEmpty(setorNome)) {
                            String setorUriPart = createUriSafe(setorNome);
                            setorResource = targetModel.createResource(BASE_URI + setorUriPart);
                        }
                        logger.trace("   L{} URIs: Empresa=<...{}>, Codigo=<...{}>, Setor=<...{}>", rowNum, empresaUriPart, codigoUriPart, (setorResource!=null?setorResource.getLocalName():"N/A")); // Trace


                        // Add Triples
                        addTripleIfNotExists(targetModel, empresaResource, RDF_TYPE, CLASS_EMPRESA);
                        // CORREÇÃO: Usar apenas RDFS.label para nome
                        Literal nomeLit = targetModel.createLiteral(nomeEmpresa);
                        addTripleIfNotExists(targetModel, empresaResource, RDFS.label, nomeLit); // Adiciona rdfs:label

                        addTripleIfNotExists(targetModel, codigoResource, RDF_TYPE, CLASS_CODIGO_NEGOCIACAO);
                        Literal tickerLit = targetModel.createTypedLiteral(codigoTicker); // Ticker é string (xsd:string por padrão)
                        addTripleIfNotExists(targetModel, codigoResource, CODIGO_NEGOCIACAO_TICKER, tickerLit);

                        addTripleIfNotExists(targetModel, empresaResource, REPRESENTADO_POR, codigoResource); // Empresa representadaPor Codigo

                        if (setorResource != null && !isEmpty(setorNome)) {
                            addTripleIfNotExists(targetModel, setorResource, RDF_TYPE, CLASS_SETOR_ATUACAO);
                            addTripleIfNotExists(targetModel, setorResource, RDFS.label, targetModel.createLiteral(setorNome, "pt")); // Label para o setor
                            addTripleIfNotExists(targetModel, empresaResource, ATUA_EM, setorResource); // Empresa atuaEm Setor
                        }
                        processedRows++;
                    } catch (Exception e) {
                        logger.error("   Erro ao processar linha {} da Planilha Info Empresas: {}", rowNum, e.getMessage(), e);
                        errorCount++; // Incrementa contador de erro
                    }
                } // fim for row
            } // Fim try-with-workbook
        } catch (IOException e) {
            logger.error("   Erro fatal de IO ao ler InfoEmpresas '{}'", resourcePath, e);
        } finally {
            if (excelInputStream != null) {
                try { excelInputStream.close(); } catch (IOException e) { /* Ignora erro no close */ }
            }

            logger.info("<< Info Empresas carregado de {}. {} linhas processadas com sucesso, {} erros.", resourcePath, processedRows, errorCount);
        }
    }


    private void loadPregaoData(String resourcePath, Model targetModel) {
        logger.info(">> Iniciando carregamento Pregão: {}", resourcePath);
        InputStream excelInputStream = null;
        int processedRows = 0;
        int errorCount = 0;
        try {
            excelInputStream = getClass().getResourceAsStream(resourcePath);
            if (excelInputStream == null) {
                logger.error("!!!! PLANILHA PREGÃO NÃO ENCONTRADA: {} !!!!", resourcePath);
                return;
            }
            logger.debug("   Arquivo Pregão OK, lendo...");
            try (Workbook workbook = new XSSFWorkbook(excelInputStream)) {
                Sheet sheet = workbook.getSheetAt(0);
                if (sheet == null) {
                    logger.error("   Planilha 0 na Planilha Pregão não encontrada em {}.", resourcePath);
                    return;
                }
                logger.info("   ... Processando Planilha Pregão '{}'", sheet.getSheetName());
                boolean isHeader = true;
                // counters já declarados fora

                for (Row row : sheet) {
                    if (isHeader) {
                        isHeader = false;
                        logger.trace("   Pulando header."); // Trace
                        continue;
                    }
                    int rowNum = row.getRowNum() + 1;
                    logger.trace("-- Proc Pregão Row: {} --", rowNum); // Trace
                    try {
                        // Extração de Dados
                        String dataStr = getCellValueAsString(row.getCell(2)); // C
                        String ticker = getCellValueAsString(row.getCell(4)); // E
                        Double precoAbertura = getCellValueAsDouble(row.getCell(8)); // I
                        Double precoMaximo = getCellValueAsDouble(row.getCell(9)); // J
                        Double precoMinimo = getCellValueAsDouble(row.getCell(10)); // K
                        Double precoMedio = getCellValueAsDouble(row.getCell(11)); // L
                        Double precoFechamento = getCellValueAsDouble(row.getCell(12)); // M
                        Long totalNegocios = getCellValueAsLong(row.getCell(13)); // N
                        Double volumeNegocios = getCellValueAsDouble(row.getCell(14)); // O
                        String tipoAcaoStr = getCellValueAsString(row.getCell(15)); // P
                        logger.trace("   L{} Raw Data: Data='{}', Ticker='{}', Tipo='{}', Fech='{}'", rowNum, dataStr, ticker, tipoAcaoStr, precoFechamento); // Trace

                        if (isEmpty(ticker) || isEmpty(dataStr)) {
                            logger.warn("   L{} Pregão: Ticker ('{}') ou Data ('{}') vazio. Linha ignorada.", rowNum, ticker, dataStr);
                            continue;
                        }

                        // --- Resolução e Criação de URIs ---
                        String empresaUriPart = createUriSafe(ticker);
                        Resource empresaResource = targetModel.createResource(BASE_URI + empresaUriPart);


                        String dataUriPartYYYYMMDD = resolveDataToUriFormatYYYYMMDD(dataStr); // Retorna YYYYMMDD ou null
                        if (dataUriPartYYYYMMDD == null) {
                            logger.warn("   L{} Pregão: Formato de data inválido para URI '{}'. Linha ignorada.", rowNum, dataStr);
                            continue;
                        }
                        // Cria URI do Pregão com prefixo
                        String pregaoUriPart = "Pregao_" + dataUriPartYYYYMMDD;
                        Resource pregaoResource = targetModel.createResource(BASE_URI + pregaoUriPart);
                        logger.trace("   L{} Pregão Res: <...{}>", rowNum, pregaoUriPart); // Trace

                        // Parse da data para o literal xsd:date
                        LocalDate dataDate = parseDateFromVariousFormats(dataStr);
                        Literal dataLiteral = null;
                        if (dataDate != null) {
                            dataLiteral = targetModel.createTypedLiteral(dataDate.format(ISO_DATE_FORMATTER), XSD.date.getURI());
                            logger.trace("   L{} Data Literal: '{}'^^xsd:date", rowNum, dataLiteral.getLexicalForm()); // Trace
                        } else {
                            logger.warn("   L{} Pregão: Não foi possível parsear data '{}' para criar literal xsd:date. Propriedade 'ocorreEmData' não será adicionada.", rowNum, dataStr);
                        }

                        String tipoAcaoUriPart = getTipoAcaoUriPart(tipoAcaoStr);
                        String vmIdSuffix = (tipoAcaoUriPart != null ? tipoAcaoUriPart : "TipoDesconhecido");
                        String vmId = createUriSafe(ticker + "_" + vmIdSuffix);
                        Resource valorMobiliarioResource = targetModel.createResource(BASE_URI + vmId);
                        logger.trace("   L{} VM Res: <...{}>", rowNum, vmId); // Trace

                        // Usa dataUriPartYYYYMMDD para URI do Negociado
                        String negociadoUriPart = createUriSafe(ticker + "_" + dataUriPartYYYYMMDD + "_Negociado");
                        Resource negociadoResource = targetModel.createResource(BASE_URI + negociadoUriPart);
                        logger.trace("   L{} Negociado Res: <...{}>", rowNum, negociadoUriPart); // Trace

                        String codigoUriPart = createUriSafe(ticker + "_Code");
                        Resource codigoResource = targetModel.getResource(BASE_URI + codigoUriPart); // Pega o recurso existente
                        if (!targetModel.containsResource(codigoResource)) {
                            logger.warn("   L{} Pregão: Código <{}> (ticker '{}') não encontrado. Criando stub.", rowNum, codigoResource.getURI(), ticker);
                            addTripleIfNotExists(targetModel, codigoResource, RDF_TYPE, CLASS_CODIGO_NEGOCIACAO);
                            addTripleIfNotExists(targetModel, codigoResource, CODIGO_NEGOCIACAO_TICKER, targetModel.createTypedLiteral(ticker));
                            if(targetModel.containsResource(empresaResource)){ addTripleIfNotExists(targetModel, empresaResource, REPRESENTADO_POR, codigoResource); }
                        }
                        logger.trace("   L{} Código Res (get): <...{}>", rowNum, codigoUriPart); // Trace


                        // --- Adição de Triplas ---

                        // 1. Tipos e Estrutura
                        addTripleIfNotExists(targetModel, empresaResource, RDF_TYPE, CLASS_EMPRESA);
                        if (addTripleIfNotExists(targetModel, pregaoResource, RDF_TYPE, CLASS_PREGAO)) {
                            // CORREÇÃO: Adiciona o literal xsd:date se disponível
                            if (dataLiteral != null) {
                                addTripleIfNotExists(targetModel, pregaoResource, OCORRE_EM_DATA, dataLiteral);
                            }

                        } else {

                            if (dataLiteral != null && !targetModel.contains(pregaoResource, OCORRE_EM_DATA)) { // Verifica se a propriedade já existe (qualquer valor)
                                // Remove triplas antigas incorretas (se houver) antes de adicionar a correta
                                targetModel.removeAll(pregaoResource, OCORRE_EM_DATA, null);
                                addTripleIfNotExists(targetModel, pregaoResource, OCORRE_EM_DATA, dataLiteral);
                                logger.debug("   L{} Atualizando/Adicionando data literal ao Pregao existente <{}>", rowNum, pregaoResource.getURI());
                            }
                        }

                        if (addTripleIfNotExists(targetModel, valorMobiliarioResource, RDF_TYPE, CLASS_VALOR_MOBILIARIO_NEGOCIADO)) {
                            addTripleIfNotExists(targetModel, valorMobiliarioResource, RDF_TYPE, CLASS_VALOR_MOBILIARIO);
                            addTripleIfNotExists(targetModel, empresaResource, TEM_VALOR_MOBILIARIO, valorMobiliarioResource);
                            addTripleIfNotExists(targetModel, valorMobiliarioResource, REPRESENTADO_POR, codigoResource);
                            if ("Ordinaria".equals(tipoAcaoUriPart)) addTripleIfNotExists(targetModel, valorMobiliarioResource, RDF_TYPE, CLASS_ACAO_ORDINARIA);
                            else if ("Preferencial".equals(tipoAcaoUriPart)) addTripleIfNotExists(targetModel, valorMobiliarioResource, RDF_TYPE, CLASS_ACAO_PREFERENCIAL);
                        }

                        addTripleIfNotExists(targetModel, negociadoResource, RDF_TYPE, CLASS_NEGOCIADO_EM_PREGAO);
                        addTripleIfNotExists(targetModel, valorMobiliarioResource, NEGOCIADO_PROP, negociadoResource);
                        addTripleIfNotExists(targetModel, negociadoResource, NEGOCIADO_DURANTE, pregaoResource);

                        // 3. Propriedades de Dados (Literais) - Usando Optional
                        Optional.ofNullable(precoAbertura).ifPresent(v -> addTripleIfNotExists(targetModel, negociadoResource, PRECO_ABERTURA, ResourceFactory.createTypedLiteral(v)));
                        Optional.ofNullable(precoMaximo).ifPresent(v -> addTripleIfNotExists(targetModel, negociadoResource, PRECO_MAXIMO, ResourceFactory.createTypedLiteral(v)));
                        Optional.ofNullable(precoMinimo).ifPresent(v -> addTripleIfNotExists(targetModel, negociadoResource, PRECO_MINIMO, ResourceFactory.createTypedLiteral(v)));
                        Optional.ofNullable(precoMedio).ifPresent(v -> addTripleIfNotExists(targetModel, negociadoResource, PRECO_MEDIO, ResourceFactory.createTypedLiteral(v)));
                        Optional.ofNullable(precoFechamento).ifPresent(v -> addTripleIfNotExists(targetModel, negociadoResource, PRECO_FECHAMENTO, ResourceFactory.createTypedLiteral(v)));
                        Optional.ofNullable(totalNegocios).ifPresent(v -> addTripleIfNotExists(targetModel, negociadoResource, TOTAL_NEGOCIOS, ResourceFactory.createTypedLiteral(v)));
                        Optional.ofNullable(volumeNegocios).ifPresent(v -> addTripleIfNotExists(targetModel, negociadoResource, VOLUME_NEGOCIOS, ResourceFactory.createTypedLiteral(v)));

                        processedRows++;
                    } catch (Exception e) {
                        logger.error("   Erro ao processar linha {} da Planilha Pregão {}: {}", rowNum, resourcePath, e.getMessage(), e);
                        errorCount++;
                    }
                } // fim for row
            } // fim try-with-workbook
        } catch (IOException e) {
            logger.error("   Erro fatal de IO ao ler Planilha Pregão '{}'", resourcePath, e);
        } finally {
            if (excelInputStream != null) {
                try { excelInputStream.close(); } catch (IOException e) { /* Ignora */ }
            }
            logger.info("<< Pregão {} carregado. {} linhas processadas com sucesso, {} erros.", resourcePath, processedRows, errorCount);
        }
    }

    // --- Métodos Utilitários ---

    /** Adiciona S-P-O se não existir. Logs Nulos e se adicionou/existia. */
    private boolean addTripleIfNotExists(Model m, Resource s, Property p, RDFNode o) {
        if (s == null || p == null || o == null) {
            logger.warn("Tentativa Add Tripla Nula: S={}, P={}, O={}", s, p, o);
            return false;
        }
        // Assume single-thread init, sem lock explícito
        if (!m.contains(s, p, o)) {
            m.add(s, p, o);
            // Log detalhado em TRACE
            logger.trace("   +++ Add Triple: S=<{}> P=<{}> O={}",
                    s.isAnon()?"[]":s.getURI(),
                    p.getURI(),
                    o.toString().length()>100?o.toString().substring(0,97)+"...":o.toString());
            return true;
        } else {
            logger.trace("   --- Triple Exists: S=<{}> P=<{}> O={}",
                    s.isAnon()?"[]":s.getURI(),
                    p.getURI(),
                    o.toString().length()>100?o.toString().substring(0,97)+"...":o.toString());
            return false;
        }
    }

    /** Obtém valor da célula como String, tratando tipos e datas (para ISO). */
    private String getCellValueAsString(Cell cell) {
        if (cell == null) return null;
        CellType cellType = cell.getCellType();
        if (cellType == CellType.FORMULA) {
            try { cellType = cell.getCachedFormulaResultType(); }
            catch (Exception e) { // Captura exceções mais genéricas de fórmulas
                logger.warn("Erro lendo formula cacheada em {}: {}. Tentando avaliar...", cell.getAddress(), e.getMessage());
                try{
                    // Tenta obter o avaliador do Workbook
                    FormulaEvaluator evaluator = cell.getSheet().getWorkbook().getCreationHelper().createFormulaEvaluator();
                    CellValue cellValue = evaluator.evaluate(cell);
                    cellType = cellValue.getCellType(); // Usa o tipo do resultado avaliado

                } catch (Exception e2){
                    logger.error("Erro avaliando formula em {}: {}", cell.getAddress(), e2.getMessage());
                    return null; // Retorna null se a avaliação falhar
                }
            }
        }
        try {
            switch (cellType) {
                case STRING:
                    return cell.getStringCellValue().trim();
                case NUMERIC:
                    if (DateUtil.isCellDateFormatted(cell)) {
                        try {

                            return cell.getDateCellValue().toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDate().format(ISO_DATE_FORMATTER);
                        } catch (Exception e) {

                            logger.warn("Falha ao formatar data da célula numérica '{}', tentando ler como número.", cell.getAddress(), e);
                            double value = cell.getNumericCellValue();
                            if (value == Math.floor(value) && !Double.isInfinite(value)) return String.valueOf((long) value);
                            else return String.valueOf(value);
                        }
                    } else {
                        double value = cell.getNumericCellValue();
                        if (value == Math.floor(value) && !Double.isInfinite(value)) return String.valueOf((long) value);
                        else return String.valueOf(value);
                    }
                case BOOLEAN: return String.valueOf(cell.getBooleanCellValue());
                case BLANK: return null;
                case ERROR: logger.warn("Célula {} contém erro: {}", cell.getAddress(), cell.getErrorCellValue()); return null;
                default: return null;
            }
        } catch (Exception e) {
            logger.error("Erro inesperado ao ler célula {}: {}", cell.getAddress(), e.getMessage(), e);
            return null;
        }
    }

    /** Obtém valor como Optional<Double>, tratando vírgula. */
    private Optional<Double> getCellValueAsOptionalDouble(Cell cell) {
        String valueStr = getCellValueAsString(cell);
        if (isEmpty(valueStr)) return Optional.empty();
        try {
            // Remove possíveis espaços extras antes de parsear
            return Optional.of(Double.parseDouble(valueStr.replace(',', '.').trim()));
        } catch (NumberFormatException e) {
            logger.trace("Fail conv Dbl:'{}'. Cell:{}", valueStr, cell!=null?cell.getAddress():"N/A");
            return Optional.empty();
        }
    }
    /** Retorna Double ou null. */
    private Double getCellValueAsDouble(Cell cell) { return getCellValueAsOptionalDouble(cell).orElse(null); }

    /** Obtém valor como Optional<Long>, tratando vírgula/ponto. */
    private Optional<Long> getCellValueAsOptionalLong(Cell cell) {
        String valueStr = getCellValueAsString(cell);
        if (isEmpty(valueStr)) return Optional.empty();
        try {
            // Remove possíveis espaços extras
            return Optional.of(Double.valueOf(valueStr.replace(',', '.').trim()).longValue());
        } catch (NumberFormatException | NullPointerException e) { // Adiciona NPE
            logger.trace("Fail conv Long:'{}'. Cell:{}", valueStr, cell!=null?cell.getAddress():"N/A");
            return Optional.empty();
        }
    }
    /** Retorna Long ou null. */
    private Long getCellValueAsLong(Cell cell) { return getCellValueAsOptionalLong(cell).orElse(null); }

    /**
     * Tenta parsear data de String (ISO, dd/MM/yyyy, ou YYYYMMDD).
     * @return LocalDate ou null se falhar.
     */
    private LocalDate parseDateFromVariousFormats(String dateString) {
        if (isEmpty(dateString)) return null;
        String trimmedData = dateString.trim();
        try {
            // Tenta ISO YYYY-MM-DD primeiro
            return LocalDate.parse(trimmedData, ISO_DATE_FORMATTER);
        } catch (DateTimeParseException e1) {
            try {
                // Tenta dd/MM/yyyy
                return LocalDate.parse(trimmedData, INPUT_DATE_FORMATTER_SLASH);
            } catch (DateTimeParseException e2) {
                // Tenta YYYYMMDD (BASIC_ISO_DATE)
                if (trimmedData.matches("^\\d{8}$")) {
                    try {
                        return LocalDate.parse(trimmedData, DateTimeFormatter.BASIC_ISO_DATE);
                    } catch (DateTimeParseException e3) {

                    }
                }
                logger.warn("Não foi possível parsear data '{}' com formatos suportados (ISO, dd/MM/yyyy, YYYYMMDD).", trimmedData);
                return null;
            }
        }
    }

    /**
     * Resolve data para formato YYYYMMDD (para URIs), tentando parsear primeiro.
     * @return String YYYYMMDD ou null.
     */
    private String resolveDataToUriFormatYYYYMMDD(String dataInput) {
        LocalDate parsedDate = parseDateFromVariousFormats(dataInput); // Usa o parser robusto
        if (parsedDate != null) {
            return parsedDate.format(URI_DATE_FORMATTER_YYYYMMDD); // Formata para URI
        } else {
            // Fallback para "hoje"/"ontem" se o parse falhou
            String lowerData = isEmpty(dataInput) ? "" : dataInput.trim().toLowerCase();
            if (lowerData.contains("hoje")) return LocalDate.now().format(URI_DATE_FORMATTER_YYYYMMDD);
            if (lowerData.contains("ontem")) return LocalDate.now().minusDays(1).format(URI_DATE_FORMATTER_YYYYMMDD);

            logger.warn("Data não reconhecida para formato URI YYYYMMDD: '{}'", dataInput);
            return null;
        }
    }

    /** Mapeia ON/PN para parte da URI ("Ordinaria", "Preferencial"). */
    private String getTipoAcaoUriPart(String tipoAcaoInput) {
        if (tipoAcaoInput == null) return null;
        String upperTrimmed = tipoAcaoInput.toUpperCase().trim();
        if ("ON".equals(upperTrimmed)) return "Ordinaria";
        if (upperTrimmed.startsWith("PN")) return "Preferencial"; // PN, PNA, PNB...
        logger.trace("Tipo ação não mapeado: '{}'", tipoAcaoInput); // Trace
        return null;
    }

    /** Verifica se string é nula/vazia. */
    private boolean isEmpty(String s) { return s == null || s.trim().isEmpty(); }

    /** Cria string segura para URI (letras, números, _, -, .). Evita começar com não-letra. */
    private String createUriSafe(String input) {
        if (isEmpty(input)) return "id_" + UUID.randomUUID().toString();
        String sanitized = input.trim()
                .replaceAll("\\s+", "_") // Espaço para underscore
                .replaceAll("[^a-zA-Z0-9_\\-\\.]", ""); // Permite letras, numeros, _, -, . (remove outros)
        sanitized = Normalizer.normalize(sanitized, Normalizer.Form.NFD)
                .replaceAll("[^\\p{ASCII}]", ""); // Remove acentos/não-ASCII
        // Re-aplica filtro para garantir que caracteres problemáticos de NFD foram removidos
        sanitized = sanitized.replaceAll("[^a-zA-Z0-9_\\-\\.]", "");
        // Garante que começa com letra (prefixo 'z_' se necessário)
        if (!sanitized.isEmpty() && !Character.isLetter(sanitized.charAt(0))) {
            sanitized = "z_" + sanitized;
        }
        // Garante que não ficou vazia após a sanitização
        if (isEmpty(sanitized)) return "id_" + UUID.randomUUID().toString();
        return sanitized;
    }


    // --- Métodos de Consulta ---
    public List<String> queryAndExtractList(Query query, String targetVariable) {
        List<String> results = new ArrayList<>();
        // Usa getModel() para garantir que temos um modelo (base ou inferência)
        Model modelToQuery = getModel();
        if (modelToQuery == null) {
            logger.error("Modelo base e de inferência NULOS! Não pode executar query.");
            return results;
        }
        logger.debug("Executando query SPARQL no modelo '{}'. Target Var: '{}'", (modelToQuery == inferenceModel ? "INFERENCIA" : "BASE"), targetVariable);
        logger.trace("Query:\n{}", query); // Log query em trace

        try (QueryExecution qe = QueryExecutionFactory.create(query, modelToQuery)) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution soln = rs.nextSolution();
                RDFNode node = soln.get(targetVariable);
                if (node != null) {
                    if (node.isLiteral()) results.add(node.asLiteral().getLexicalForm());
                    else if (node.isResource()) results.add(node.asResource().getURI());
                    else results.add(node.toString());
                } else {
                    logger.warn("Variável '{}' nula na solução da query.", targetVariable);
                    results.add(null); // Adiciona null para manter correspondência se necessário
                }
            }
        } catch (QueryParseException qpe) {
            logger.error("Erro de PARSE na consulta SPARQL!", qpe);
            logger.error("Query com erro:\n{}", query);
        } catch (Exception e) {
            logger.error("Erro INESPERADO ao executar consulta SPARQL", e);
            logger.error("Query:\n{}", query);
        }
        logger.debug("Execução da consulta concluída. {} resultado(s) para '{}'.", results.size(), targetVariable);
        return results;
    }

    /** Retorna o modelo de inferência (preferencial) ou o base. */
    public Model getModel() {
        if (inferenceModel != null) {
            return inferenceModel;
        } else {
            logger.warn("Modelo de inferência é nulo, retornando modelo base.");
            return baseOntology; // Retorna o base se a inferência falhou
        }
    }

    /** Retorna URI base. */
    public String getBaseUri() { return BASE_URI; }

    /** Constrói URI de predicado. */
    public String getPredicateURI(String localName) { return (isEmpty(localName)) ? null : BASE_URI + localName; }

} // Fim da classe StockMarketOntology