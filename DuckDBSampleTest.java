package com.gilead.testscripts.module1;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

public class DuckDBSampleTest {

    // ======== Configuration ========
    private static final String SHEET_NAME = null;                 // null => first sheet
    private static final String EXCEL_DATE_HEADER = "Sampling date";
    private static final String EXCEL_DEVICE_ID_HEADER = "Device sample ID";

    // CSV headers
    private static final String CSV_DATE_COL   = "created_on";
    private static final String CSV_ID_COL     = "device_sample_id";
    private static final String CSV_ENTITY_COL = "entity";

    // Display format for DATE report
    private static final String DISPLAY_DATE_FMT = "%m/%d/%Y";

    // CSV date formats (robust parsing)
    private static final String[] CSV_STRPTIME_FORMATS = new String[] {
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f"
    };

    // Excel text formats to try
    private static final DateTimeFormatter[] EXCEL_TEXT_FORMATS = new DateTimeFormatter[] {
        DateTimeFormatter.ofPattern("d/M/yy H:mm").withResolverStyle(java.time.format.ResolverStyle.SMART),
        DateTimeFormatter.ofPattern("d/M/yyyy H:mm").withResolverStyle(java.time.format.ResolverStyle.SMART),
        DateTimeFormatter.ofPattern("d/M/yy").withResolverStyle(java.time.format.ResolverStyle.SMART),
        DateTimeFormatter.ofPattern("d/M/yyyy").withResolverStyle(java.time.format.ResolverStyle.SMART),
        DateTimeFormatter.ofPattern("ddMMyyyy").withResolverStyle(java.time.format.ResolverStyle.SMART)
    };

    // Matching toggles
    private static final boolean CASE_INSENSITIVE_ID_MATCH     = true;
    private static final boolean CASE_INSENSITIVE_ENTITY_MATCH = true;

    // Configuration for batch size
    private static final int BATCH_SIZE = 500;

    // Device-Entity mapping configuration (loaded from JSON)
    private static DeviceEntityMappingConfigSimplified mappingConfig;

    // Column split validation configuration (loaded from JSON)
    private static ColumnSplitValidationConfig splitValidationConfig;

    // ===== SHARED STATE FOR TESTNG EXECUTION =====
    private static Connection conn;
    private static XSSFWorkbook wb;
    private static Path excelPath;
    private static Path csvPath;
    private static String csvFile;

    // *** Output directory: configurable (defaults to user home + GVista path) ***
    private static final Path FIXED_OUTPUT_DIR = resolveOutputDir();

    private static Path resolveOutputDir() {
        String envPath = System.getenv("GVISTA_OUTPUT_DIR");
        if (envPath != null && !envPath.isBlank()) {
            return Paths.get(envPath);
        }
        return Paths.get(
            System.getProperty("user.home"),
            "GVista", "projectTests", "src", "test", "resources", "externalFiles"
        );
    }

    // =====================================================
    // TESTNG SUPPORT METHODS (Called from DuckDBValidationTestNG)
    // =====================================================

    /**
     * Initialize test environment (called once before all tests)
     */
    public static void initializeTestEnvironment() throws Exception {
        System.out.println("\n📋 Initializing test environment...");
        System.out.println("Working dir: " + System.getProperty("user.dir"));
        System.out.println("Output dir: " + FIXED_OUTPUT_DIR.toAbsolutePath());
        ReportingUtils.updateLog("Initializing test environment");

        // Load device-entity mapping configuration
        try {
            mappingConfig = DeviceEntityMappingConfigSimplified.load();
            mappingConfig.validate();
            System.out.println("✓ Device mapping config loaded");
            System.out.println(mappingConfig.getMappingsSummary());
        } catch (Exception e) {
            System.err.println("ERROR loading mapping configuration: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        // Load column split validation configuration
        try {
            splitValidationConfig = ColumnSplitValidationConfig.load();
            splitValidationConfig.validate();
            System.out.println("✓ Column split validation config loaded");
            System.out.println(splitValidationConfig.getValidationsSummary());
        } catch (Exception e) {
            System.err.println("ERROR loading split validation configuration: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        // Resolve files
        excelPath = resolveResource("/testDataFiles/CCPD013_example_from_SDC.xlsx",
                "src/test/resources/testDataFiles/CCPD013_example_from_SDC.xlsx");
        csvPath   = resolveResource("/externalFiles/BioHT_parsed_cc.csv",
                "src/test/resources/externalFiles/BioHT_parsed_cc.csv");

        System.out.println("Excel path : " + excelPath + " | exists? " + Files.exists(excelPath));
        System.out.println("CSV   path : " + csvPath   + " | exists? " + Files.exists(csvPath));

        if (!Files.exists(excelPath)) {
            throw new FileNotFoundException("Excel file not found: " + excelPath.toAbsolutePath());
        }
        if (!Files.exists(csvPath)) {
            throw new FileNotFoundException("CSV file not found: " + csvPath.toAbsolutePath());
        }

        csvFile = escapeSqlString(csvPath.toString().replace("\\", "/"));

        // Create output directory
        Files.createDirectories(FIXED_OUTPUT_DIR);

        // Initialize Database Connection
        conn = DriverManager.getConnection("jdbc:duckdb:");

        // Initialize Workbook
        wb = new XSSFWorkbook();

        // Show DuckDB version
        try (Statement s = conn.createStatement();
             ResultSet v = s.executeQuery("PRAGMA version")) {
            if (v.next()) {
                System.out.println("DuckDB version: " + v.getString("library_version") +
                                   " | source_id: " + v.getString("source_id"));
            }
        }

        // Create temp tables
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS excel_ts;");
            s.execute("CREATE TEMP TABLE excel_ts(unified_ts TIMESTAMP);");
            s.execute("DROP TABLE IF EXISTS excel_ids;");
            s.execute("CREATE TEMP TABLE excel_ids(excel_id VARCHAR);");
            s.execute("DROP TABLE IF EXISTS csv_data;");
            s.execute("CREATE TEMP TABLE csv_data AS SELECT * FROM read_csv_auto('" + csvFile + "');");
        }

        // Ingest Excel data
        int excelDateRows = ingestExcelDateTimes(conn, excelPath, SHEET_NAME, EXCEL_DATE_HEADER);
        System.out.println("Inserted into excel_ts: " + excelDateRows + " rows");

        int excelIdRows = ingestExcelIds(conn, excelPath, SHEET_NAME, EXCEL_DEVICE_ID_HEADER);
        System.out.println("Inserted into excel_ids: " + excelIdRows + " rows");

        System.out.println("✓ Test environment initialized\n");
        ReportingUtils.updateLog("Test environment initialized", true);
    }

    /**
     * Cleanup test environment (called once after all tests)
     */
    public static void cleanupTestEnvironment() throws Exception {
        System.out.println("\n📊 Saving results...");
        ReportingUtils.updateLog("Beginning cleanup and save of results");

        Path outFile = FIXED_OUTPUT_DIR.resolve("validation_results.xlsx");
        saveExcelReport(outFile, wb);

        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
        if (wb != null) {
            wb.close();
        }

        System.out.println("✓ Results saved to: " + outFile.toAbsolutePath());
        System.out.println("✓ Test environment cleaned up\n");
        ReportingUtils.updateLog("Test environment cleaned up; results saved to: " + outFile.toAbsolutePath(), true);
    }

    // =====================================================
    // TEST EXECUTION METHODS (Called from DuckDBValidationTestNG)
    // =====================================================

    /**
     * TEST 1: Validate DATE matching between Excel and CSV
     */
    public static void executeTestDateValidation() throws Exception {
        ReportingUtils.updateLog("START: Date validation");
        try {
            System.out.println("\n🔵 TEST 1: DATE VALIDATION");
            System.out.println("─────────────────────────────────────");

            long excelTotalRows     = scalarLong(conn, "SELECT COUNT(*) FROM excel_ts");
            long excelUsedRows      = scalarLong(conn, "SELECT COUNT(*) FROM excel_ts WHERE unified_ts IS NOT NULL");
            long excelDistinctDates = scalarLong(conn, "SELECT COUNT(DISTINCT CAST(unified_ts AS DATE)) FROM excel_ts WHERE unified_ts IS NOT NULL");

            String csvDateExpr = buildCsvDateExpr(CSV_DATE_COL);
            long csvTotalRows       = scalarLong(conn, "SELECT COUNT(*) FROM csv_data");
            long csvUsedRows        = scalarLong(conn, "SELECT COUNT(*) FROM (SELECT " + csvDateExpr + " AS d FROM csv_data) t WHERE d IS NOT NULL");
            long csvDistinctDates   = scalarLong(conn, "SELECT COUNT(DISTINCT d) FROM (SELECT " + csvDateExpr + " AS d FROM csv_data) t WHERE d IS NOT NULL");

            System.out.println("Excel rows: total=" + excelTotalRows + ", used=" + excelUsedRows + ", distinct_dates=" + excelDistinctDates);
            System.out.println("CSV   rows: total=" + csvTotalRows   + ", used=" + csvUsedRows   + ", distinct_dates=" + csvDistinctDates);

            String dateMatchSql = buildDateOnlyMatchReportSQL(csvDateExpr);

            executeSqlAndPrintAll(conn, "DATE-ONLY MATCH REPORT (ascending; format " + DISPLAY_DATE_FMT + ")", dateMatchSql);
            writeQueryToSheetWithHighlight(conn, wb, "Date Match", dateMatchSql, SheetType.DATE);
            writeCountsSheet(wb, "Date Counts", new String[][]{
                {"excel_total_rows", String.valueOf(excelTotalRows)},
                {"excel_used_rows", String.valueOf(excelUsedRows)},
                {"excel_distinct_dates", String.valueOf(excelDistinctDates)},
                {"csv_total_rows", String.valueOf(csvTotalRows)},
                {"csv_used_rows", String.valueOf(csvUsedRows)},
                {"csv_distinct_dates", String.valueOf(csvDistinctDates)},
            });

            System.out.println("✓ Date validation complete");
            ReportingUtils.updateLog("END: Date validation - PASS", true);
        } catch (Exception e) {
            ReportingUtils.updateLog("END: Date validation - FAIL: " + e.getMessage(), false);
            throw e;
        }
    }

    /**
     * TEST 2: Validate DEVICE ID matching between Excel and CSV
     */
    public static void executeTestDeviceIdValidation() throws Exception {
        ReportingUtils.updateLog("START: Device ID validation");
        try {
            System.out.println("\n🟢 TEST 2: DEVICE ID VALIDATION");
            System.out.println("─────────────────────────────────────");

            long excelIdTotal    = scalarLong(conn, "SELECT COUNT(*) FROM excel_ids");
            long excelIdUsed     = scalarLong(conn, "SELECT COUNT(*) FROM excel_ids WHERE NULLIF(TRIM(excel_id), '') IS NOT NULL");
            long excelIdDistinct = scalarLong(conn,
                "SELECT COUNT(DISTINCT " + (CASE_INSENSITIVE_ID_MATCH ? "UPPER" : "") +
                "(NULLIF(TRIM(excel_id), ''))) FROM excel_ids WHERE NULLIF(TRIM(excel_id), '') IS NOT NULL");

            long csvIdTotal      = scalarLong(conn, "SELECT COUNT(*) FROM csv_data");
            long csvIdUsed       = scalarLong(conn,
                "SELECT COUNT(*) FROM (SELECT NULLIF(TRIM(CAST(" + CSV_ID_COL + " AS VARCHAR)), '') AS id FROM csv_data) t WHERE id IS NOT NULL");
            long csvIdDistinct   = scalarLong(conn,
                "SELECT COUNT(DISTINCT " + (CASE_INSENSITIVE_ID_MATCH ? "UPPER" : "") +
                "(id)) FROM (SELECT NULLIF(TRIM(CAST(" + CSV_ID_COL + " AS VARCHAR)), '') AS id FROM csv_data) t WHERE id IS NOT NULL");

            System.out.println("Excel IDs: total=" + excelIdTotal + ", used=" + excelIdUsed + ", distinct_ids=" + excelIdDistinct);
            System.out.println("CSV   IDs: total=" + csvIdTotal   + ", used=" + csvIdUsed   + ", distinct_ids=" + csvIdDistinct);

            String deviceIdMatchSql = buildDeviceIdMatchReportSQL(CASE_INSENSITIVE_ID_MATCH);

            executeSqlAndPrintAll(conn, "DEVICE ID MATCH REPORT (ascending; exact, trimmed)", deviceIdMatchSql);
            writeQueryToSheetWithHighlight(conn, wb, "Device ID Match", deviceIdMatchSql, SheetType.ID);
            writeCountsSheet(wb, "ID Counts", new String[][]{
            {"excel_id_total", String.valueOf(excelIdTotal)},
            {"excel_id_used", String.valueOf(excelIdUsed)},
            {"excel_id_distinct", String.valueOf(excelIdDistinct)},
            {"csv_id_total", String.valueOf(csvIdTotal)},
            {"csv_id_used", String.valueOf(csvIdUsed)},
            {"csv_id_distinct", String.valueOf(csvIdDistinct)},
            });

            System.out.println("✓ Device ID validation complete");
            ReportingUtils.updateLog("END: Device ID validation - PASS", true);
        } catch (Exception e) {
            ReportingUtils.updateLog("END: Device ID validation - FAIL: " + e.getMessage(), false);
            throw e;
        }
    }

    /**
     * TEST 3: Validate DEVICE to ENTITY transformation
     */
    public static void executeTestDeviceEntityValidation() throws Exception {
        ReportingUtils.updateLog("START: Device→Entity validation");
        try {
            System.out.println("\n🟠 TEST 3: DEVICE→ENTITY VALIDATION");
            System.out.println("─────────────────────────────────────");

            String deviceEntityCountsSql = buildDeviceEntityCountsSql(CASE_INSENSITIVE_ENTITY_MATCH);
            String deviceEntityReportSql = buildDeviceEntityValidationSql(CASE_INSENSITIVE_ENTITY_MATCH);

            executeSqlAndPrintAll(conn, "DEVICE → ENTITY VALIDATION — COUNTS", deviceEntityCountsSql);
            executeSqlAndPrintAll(conn, "DEVICE → ENTITY VALIDATION — FULL REPORT", deviceEntityReportSql);
            writeQueryToSheetWithHighlight(conn, wb, "Device→Entity Report", deviceEntityReportSql, SheetType.ENTITY);

            Map<String,String> entityCounts = fetchCountsRow(conn, deviceEntityCountsSql);
            if (entityCounts.isEmpty()) {
                System.err.println("Warning: No entity counts returned. Using defaults.");
                entityCounts = getDefaultEntityCounts();
            }

            writeCountsSheet(wb, "Entity Counts", new String[][]{
                {"total_rows", entityCounts.getOrDefault("total_rows","0")},
                {"rule_rows", entityCounts.getOrDefault("rule_rows","0")},
                {"pass_rows", entityCounts.getOrDefault("pass_rows","0")},
                {"fail_rows", entityCounts.getOrDefault("fail_rows","0")},
                {"missing_entity_rows", entityCounts.getOrDefault("missing_entity_rows","0")},
                {"ignored_rows", entityCounts.getOrDefault("ignored_rows","0")}
            });

            System.out.println("✓ Device→Entity validation complete");
            ReportingUtils.updateLog("END: Device→Entity validation - PASS", true);
        } catch (Exception e) {
            ReportingUtils.updateLog("END: Device→Entity validation - FAIL: " + e.getMessage(), false);
            throw e;
        }
    }

    /**
     * TEST 4: Validate COLUMN SPLITS (15 columns: Raw + LG)
     */
    public static void executeTestColumnSplitValidation() throws Exception {
        ReportingUtils.updateLog("START: Column split validation");
        try {
            System.out.println("\n🟡 TEST 4: COLUMN SPLIT VALIDATION");
            System.out.println("─────────────────────────────────────");

            String splitsExcelPath = excelPath.toString().replace("\\", "/");
            String splitsCsvPath = csvPath.toString().replace("\\", "/");
            String columnSplitValidationSql = splitValidationConfig.buildValidationSql(splitsExcelPath, splitsCsvPath);

            executeSqlAndPrintAll(conn, "COLUMN SPLIT VALIDATION REPORT", columnSplitValidationSql);
            writeQueryToSheetWithHighlight(conn, wb, "Column Split Validation", columnSplitValidationSql, SheetType.ENTITY);

            System.out.println("✓ Column split validation complete");
            ReportingUtils.updateLog("END: Column split validation - PASS", true);
        } catch (Exception e) {
            ReportingUtils.updateLog("END: Column split validation - FAIL: " + e.getMessage(), false);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Working dir: " + System.getProperty("user.dir"));
        System.out.println("Output dir: " + FIXED_OUTPUT_DIR.toAbsolutePath());

        // Load device-entity mapping configuration
        try {
            mappingConfig = DeviceEntityMappingConfigSimplified.load();
            mappingConfig.validate();
            System.out.println("✓ Device mapping config loaded");
            System.out.println(mappingConfig.getMappingsSummary());
        } catch (Exception e) {
            System.err.println("ERROR loading mapping configuration: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        // Load column split validation configuration
        try {
            splitValidationConfig = ColumnSplitValidationConfig.load();
            splitValidationConfig.validate();
            System.out.println("✓ Column split validation config loaded");
            System.out.println(splitValidationConfig.getValidationsSummary());
        } catch (Exception e) {
            System.err.println("ERROR loading split validation configuration: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        // Resolve files from classpath first (target/test-classes), else from project tree
        Path excelPath = resolveResource("/testDataFiles/CCPD013_example_from_SDC.xlsx",
                "src/test/resources/testDataFiles/CCPD013_example_from_SDC.xlsx");
        Path csvPath   = resolveResource("/externalFiles/BioHT_parsed_cc.csv",
                "src/test/resources/externalFiles/BioHT_parsed_cc.csv");

        System.out.println("Excel path : " + excelPath + " | exists? " + Files.exists(excelPath));
        System.out.println("CSV   path : " + csvPath   + " | exists? " + Files.exists(csvPath));

        // Validate files exist
        if (!Files.exists(excelPath)) {
            throw new FileNotFoundException("Excel file not found: " + excelPath.toAbsolutePath());
        }
        if (!Files.exists(csvPath)) {
            throw new FileNotFoundException("CSV file not found: " + csvPath.toAbsolutePath());
        }

        String csvFile = escapeSqlString(csvPath.toString().replace("\\", "/"));

        // Ensure FIXED output folder exists
        Files.createDirectories(FIXED_OUTPUT_DIR);
        Path outFile = FIXED_OUTPUT_DIR.resolve("validation_results.xlsx");

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             XSSFWorkbook wb = new XSSFWorkbook()) {

            // 0) Version
            try (Statement s = conn.createStatement();
                 ResultSet v = s.executeQuery("PRAGMA version")) {
                if (v.next()) {
                    System.out.println("DuckDB version: " + v.getString("library_version") +
                                       " | source_id: " + v.getString("source_id"));
                }
            }

            // Create temp tables for Excel ingestion
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS excel_ts;");
                s.execute("CREATE TEMP TABLE excel_ts(unified_ts TIMESTAMP);");

                s.execute("DROP TABLE IF EXISTS excel_ids;");
                s.execute("CREATE TEMP TABLE excel_ids(excel_id VARCHAR);");

                // Create persistent CSV temp table to avoid multiple reads
                s.execute("DROP TABLE IF EXISTS csv_data;");
                s.execute("CREATE TEMP TABLE csv_data AS SELECT * FROM read_csv_auto('" + csvFile + "');");
            }

            // Ingest Excel
            int excelDateRows = ingestExcelDateTimes(conn, excelPath, SHEET_NAME, EXCEL_DATE_HEADER);
            System.out.println("Inserted into excel_ts: " + excelDateRows + " rows");

            int excelIdRows = ingestExcelIds(conn, excelPath, SHEET_NAME, EXCEL_DEVICE_ID_HEADER);
            System.out.println("Inserted into excel_ids: " + excelIdRows + " rows");

            // ===== A) DATE-ONLY MATCH =====
            long excelTotalRows     = scalarLong(conn, "SELECT COUNT(*) FROM excel_ts");
            long excelUsedRows      = scalarLong(conn, "SELECT COUNT(*) FROM excel_ts WHERE unified_ts IS NOT NULL");
            long excelDistinctDates = scalarLong(conn, "SELECT COUNT(DISTINCT CAST(unified_ts AS DATE)) FROM excel_ts WHERE unified_ts IS NOT NULL");

            String csvDateExpr = buildCsvDateExpr(CSV_DATE_COL);
            long csvTotalRows       = scalarLong(conn, "SELECT COUNT(*) FROM csv_data");
            long csvUsedRows        = scalarLong(conn, "SELECT COUNT(*) FROM (SELECT " + csvDateExpr + " AS d FROM csv_data) t WHERE d IS NOT NULL");
            long csvDistinctDates   = scalarLong(conn, "SELECT COUNT(DISTINCT d) FROM (SELECT " + csvDateExpr + " AS d FROM csv_data) t WHERE d IS NOT NULL");

            System.out.println("\n--- DATE ROW COUNTS ---");
            System.out.println("Excel rows: total=" + excelTotalRows + ", used=" + excelUsedRows + ", distinct_dates=" + excelDistinctDates);
            System.out.println("CSV   rows: total=" + csvTotalRows   + ", used=" + csvUsedRows   + ", distinct_dates=" + csvDistinctDates);

            String dateMatchSql = buildDateOnlyMatchReportSQL(csvDateExpr);

            // Console
            executeSqlAndPrintAll(conn, "DATE-ONLY MATCH REPORT (ascending; format " + DISPLAY_DATE_FMT + ")", dateMatchSql);
            // Excel export
            writeQueryToSheetWithHighlight(conn, wb, "Date Match", dateMatchSql, SheetType.DATE);

            // Metrics (key/value)
            writeCountsSheet(wb, "Date Counts", new String[][]{
                {"excel_total_rows", String.valueOf(excelTotalRows)},
                {"excel_used_rows", String.valueOf(excelUsedRows)},
                {"excel_distinct_dates", String.valueOf(excelDistinctDates)},
                {"csv_total_rows", String.valueOf(csvTotalRows)},
                {"csv_used_rows", String.valueOf(csvUsedRows)},
                {"csv_distinct_dates", String.valueOf(csvDistinctDates)},
            });

            // ===== B) DEVICE ID MATCH =====
            long excelIdTotal    = scalarLong(conn, "SELECT COUNT(*) FROM excel_ids");
            long excelIdUsed     = scalarLong(conn, "SELECT COUNT(*) FROM excel_ids WHERE NULLIF(TRIM(excel_id), '') IS NOT NULL");
            long excelIdDistinct = scalarLong(conn,
                    "SELECT COUNT(DISTINCT " + (CASE_INSENSITIVE_ID_MATCH ? "UPPER" : "") +
                    "(NULLIF(TRIM(excel_id), ''))) FROM excel_ids WHERE NULLIF(TRIM(excel_id), '') IS NOT NULL");

            long csvIdTotal      = scalarLong(conn, "SELECT COUNT(*) FROM csv_data");
            long csvIdUsed       = scalarLong(conn,
                    "SELECT COUNT(*) FROM (SELECT NULLIF(TRIM(CAST(" + CSV_ID_COL + " AS VARCHAR)), '') AS id FROM csv_data) t WHERE id IS NOT NULL");
            long csvIdDistinct   = scalarLong(conn,
                    "SELECT COUNT(DISTINCT " + (CASE_INSENSITIVE_ID_MATCH ? "UPPER" : "") +
                    "(id)) FROM (SELECT NULLIF(TRIM(CAST(" + CSV_ID_COL + " AS VARCHAR)), '') AS id FROM csv_data) t WHERE id IS NOT NULL");

            System.out.println("\n--- DEVICE ID ROW COUNTS ---");
            System.out.println("Excel IDs: total=" + excelIdTotal + ", used=" + excelIdUsed + ", distinct_ids=" + excelIdDistinct);
            System.out.println("CSV   IDs: total=" + csvIdTotal   + ", used=" + csvIdUsed   + ", distinct_ids=" + csvIdDistinct);

            String deviceIdMatchSql = buildDeviceIdMatchReportSQL(CASE_INSENSITIVE_ID_MATCH);

            executeSqlAndPrintAll(conn, "DEVICE ID MATCH REPORT (ascending; exact, trimmed)", deviceIdMatchSql);
            writeQueryToSheetWithHighlight(conn, wb, "Device ID Match", deviceIdMatchSql, SheetType.ID);

            writeCountsSheet(wb, "ID Counts", new String[][]{
                {"excel_id_total", String.valueOf(excelIdTotal)},
                {"excel_id_used", String.valueOf(excelIdUsed)},
                {"excel_id_distinct", String.valueOf(excelIdDistinct)},
                {"csv_id_total", String.valueOf(csvIdTotal)},
                {"csv_id_used", String.valueOf(csvIdUsed)},
                {"csv_id_distinct", String.valueOf(csvIdDistinct)},
            });

            // ===== C) DEVICE → ENTITY (CSV only) =====
            String deviceEntityCountsSql = buildDeviceEntityCountsSql(CASE_INSENSITIVE_ENTITY_MATCH);
            String deviceEntityReportSql = buildDeviceEntityValidationSql(CASE_INSENSITIVE_ENTITY_MATCH);

            executeSqlAndPrintAll(conn, "DEVICE → ENTITY VALIDATION — COUNTS", deviceEntityCountsSql);
            executeSqlAndPrintAll(conn, "DEVICE → ENTITY VALIDATION — FULL REPORT", deviceEntityReportSql);

            writeQueryToSheetWithHighlight(conn, wb, "Device→Entity Report", deviceEntityReportSql, SheetType.ENTITY);

            // Consistent key/value metrics for Entity Counts
            Map<String,String> entityCounts = fetchCountsRow(conn, deviceEntityCountsSql);
            if (entityCounts.isEmpty()) {
                System.err.println("Warning: No entity counts returned. Using defaults.");
                entityCounts = getDefaultEntityCounts();
            }

            writeCountsSheet(wb, "Entity Counts", new String[][]{
                {"total_rows", entityCounts.getOrDefault("total_rows","0")},
                {"rule_rows", entityCounts.getOrDefault("rule_rows","0")},
                {"pass_rows", entityCounts.getOrDefault("pass_rows","0")},
                {"fail_rows", entityCounts.getOrDefault("fail_rows","0")},
                {"missing_entity_rows", entityCounts.getOrDefault("missing_entity_rows","0")},
                {"ignored_rows", entityCounts.getOrDefault("ignored_rows","0")}
            });

            // ===== D) COLUMN SPLIT VALIDATION (15 columns) =====
            System.out.println("\n--- COLUMN SPLIT VALIDATIONS ---");
            String splitsExcelPath = excelPath.toString().replace("\\", "/");
            String splitsCsvPath = csvPath.toString().replace("\\", "/");
            String columnSplitValidationSql = splitValidationConfig.buildValidationSql(splitsExcelPath, splitsCsvPath);

            executeSqlAndPrintAll(conn, "COLUMN SPLIT VALIDATION REPORT", columnSplitValidationSql);
            writeQueryToSheetWithHighlight(conn, wb, "Column Split Validation", columnSplitValidationSql, SheetType.ENTITY);

            // ===== Save Excel to the fixed folder =====
            saveExcelReport(outFile, wb);

        } catch (Exception e) {
            System.err.println("ERROR in DuckDB validation: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    // =========================
    // Helper Methods (NEW)
    // =========================

    /**
     * Escape single quotes in SQL strings to prevent injection.
     * SQL standard: replace ' with ''
     */
    private static String escapeSqlString(String s) {
        return s.replace("'", "''");
    }

    /**
     * Build the reusable "expected_up" expression for entity validation.
     * Handles case sensitivity and normalizes device IDs to expected format.
     * USES EXTERNAL JSON CONFIGURATION - rules can be updated without recompilation!
     */
    private static String buildExpectedUpExpression(boolean caseInsensitive) {
        if (mappingConfig == null) {
            throw new IllegalStateException("Mapping configuration not loaded. Call DeviceEntityMappingConfigSimplified.load() first.");
        }
        return mappingConfig.buildSqlExpression("device_id", caseInsensitive);
    }

    /**
     * Get default entity counts when no results are returned.
     */
    private static Map<String, String> getDefaultEntityCounts() {
        return Map.ofEntries(
            Map.entry("total_rows", "0"),
            Map.entry("rule_rows", "0"),
            Map.entry("pass_rows", "0"),
            Map.entry("fail_rows", "0"),
            Map.entry("missing_entity_rows", "0"),
            Map.entry("ignored_rows", "0")
        );
    }

    /**
     * Save Excel report with proper error handling and file locking considerations.
     */
    private static void saveExcelReport(Path outFile, XSSFWorkbook wb) throws IOException {
        try {
            System.out.println("\nAttempting to write Excel report to: " + outFile.toAbsolutePath());
            Files.createDirectories(outFile.getParent());

            // If the file is open in Excel, delete will fail => show error
            try {
                Files.deleteIfExists(outFile);
            } catch (IOException delEx) {
                System.err.println("Warning: Could not delete existing file (it may be open): " + outFile);
            }

            try (FileOutputStream fos = new FileOutputStream(outFile.toFile())) {
                wb.write(fos);
                fos.flush();
            }

            System.out.println("Saved Excel report: " + outFile.toAbsolutePath());
            System.out.println(summarySheets(wb));

        } catch (Exception ioEx) {
            System.err.println("ERROR writing Excel report to " + outFile.toAbsolutePath());
            throw new IOException("Failed to save Excel report", ioEx);
        }
    }

    // =========================
    // SQL Builders (IMPROVED)
    // =========================

    private static String buildCsvDateExpr(String colName) {
        StringBuilder coalesce = new StringBuilder("COALESCE(");
        boolean first = true;
        for (String fmt : CSV_STRPTIME_FORMATS) {
            if (!first) coalesce.append(", ");
            coalesce.append("CAST(TRY_STRPTIME(CAST(")
                    .append(colName)
                    .append(" AS VARCHAR), '")
                    .append(fmt)
                    .append("') AS DATE)");
            first = false;
        }
        coalesce.append(", CAST(CAST(").append(colName).append(" AS TIMESTAMP) AS DATE))");
        return coalesce.toString();
    }

    /**
     * Date-only: headers are SQL names: "Measurement date" and "created_on"
     * Uses csv_data temp table instead of re-reading CSV.
     */
    private static String buildDateOnlyMatchReportSQL(String csvDateExpr) {
        return """
               WITH excel_dates AS (
                 SELECT DISTINCT CAST(unified_ts AS DATE) AS d
                 FROM excel_ts
                 WHERE unified_ts IS NOT NULL
               ),
               csv_dates AS (
                 SELECT DISTINCT %s AS d
                 FROM csv_data
                 WHERE %s IS NOT NULL
               )
               SELECT
                 strftime('%s', e.d) AS "Measurement date",
                 strftime('%s', c.d) AS "created_on",
                 CASE
                   WHEN e.d IS NOT NULL AND c.d IS NOT NULL THEN 'Date present in both xlsx and csv and it matches'
                   WHEN e.d IS NOT NULL AND c.d IS NULL THEN 'Present in xlsx only'
                   WHEN e.d IS NULL AND c.d IS NOT NULL THEN 'Present in csv only'
                   ELSE 'Unknown'
                 END AS result
               FROM excel_dates e
               FULL OUTER JOIN csv_dates c
                 ON e.d = c.d
               ORDER BY COALESCE(e.d, c.d)
               """.formatted(csvDateExpr, CSV_DATE_COL, DISPLAY_DATE_FMT, DISPLAY_DATE_FMT);
    }

    /**
     * Device ID: headers use SQL names: "device_sample_id_excel", "device_sample_id_csv"
     * Uses csv_data temp table instead of re-reading CSV.
     */
    private static String buildDeviceIdMatchReportSQL(boolean caseInsensitive) {
        String excelIdRaw = "NULLIF(TRIM(excel_id), '')";
        String csvIdRaw   = "NULLIF(TRIM(CAST(" + CSV_ID_COL + " AS VARCHAR)), '')";
        String excelKey   = caseInsensitive ? "UPPER(" + excelIdRaw + ")" : excelIdRaw;
        String csvKey     = caseInsensitive ? "UPPER(" + csvIdRaw + ")"   : csvIdRaw;

        return """
               WITH excel_ids_norm AS (
                 SELECT DISTINCT %1$s AS id_key, %2$s AS id_raw
                 FROM excel_ids
                 WHERE %2$s IS NOT NULL
               ),
               csv_ids_norm AS (
                 SELECT DISTINCT %3$s AS id_key, %4$s AS id_raw
                 FROM csv_data
                 WHERE %4$s IS NOT NULL
               )
               SELECT
                 e.id_raw AS "device_sample_id_excel",
                 c.id_raw AS "device_sample_id_csv",
                 CASE
                   WHEN e.id_key IS NOT NULL AND c.id_key IS NOT NULL THEN 'ID present in both xlsx and csv and it matches'
                   WHEN e.id_key IS NOT NULL AND c.id_key IS NULL THEN 'Present in xlsx only'
                   WHEN e.id_key IS NULL AND c.id_key IS NOT NULL THEN 'Present in csv only'
                   ELSE 'Unknown'
                 END AS result
               FROM excel_ids_norm e
               FULL OUTER JOIN csv_ids_norm c
                 ON e.id_key = c.id_key
               ORDER BY COALESCE(e.id_raw, c.id_raw)
               """.formatted(excelKey, excelIdRaw, csvKey, csvIdRaw);
    }

    /**
     * Device → Entity SELF-VALIDATION (CSV only)
     * Uses extracted buildExpectedUpExpression() which loads rules from JSON.
     * This ensures validation matches transformation rules defined in device-entity-mappings.json
     */
    private static String buildDeviceEntityValidationSql(boolean caseInsensitive) {
        String up = caseInsensitive ? "UPPER(%s)" : "%s";
        String entityUp     = String.format(up, "entity_id");  // Full entity value
        String expectedUp   = buildExpectedUpExpression(caseInsensitive);

        return """
               WITH src AS (
                 SELECT
                   NULLIF(TRIM(CAST(%1$s AS VARCHAR)), '') AS device_id,
                   NULLIF(TRIM(CAST(%2$s AS VARCHAR)), '') AS entity_id
                 FROM csv_data
               ),
               val AS (
                 SELECT
                   device_id,
                   entity_id,
                   %3$s AS expected_up,
                   %4$s AS entity_up
                 FROM src
               ),
               final AS (
                 SELECT
                   device_id,
                   entity_id,
                   CASE
                     WHEN expected_up IS NULL THEN 'IGNORED (no rule)'
                     WHEN entity_up IS NULL THEN 'MISSING ENTITY'
                     WHEN expected_up = entity_up THEN 'PASS'
                     ELSE 'FAIL'
                   END AS status
                 FROM val
               )
               SELECT
                 device_id AS "device_sample_id",
                 entity_id AS "entity_id",
                 status
               FROM final
               ORDER BY COALESCE(entity_id, device_id)
               """
               .formatted(
                   CSV_ID_COL,
                   CSV_ENTITY_COL,
                   expectedUp,
                   entityUp
               );
    }

    /**
     * Entity counts aggregation.
     * Uses extracted buildExpectedUpExpression() which loads rules from JSON.
     * This ensures counts are based on the same rules as validation.
     */
    private static String buildDeviceEntityCountsSql(boolean caseInsensitive) {
        String up = caseInsensitive ? "UPPER(%s)" : "%s";
        String entityUp     = String.format(up, "entity_id");  // Full entity value
        String expectedUp   = buildExpectedUpExpression(caseInsensitive);

        return """
               WITH src AS (
                 SELECT
                   NULLIF(TRIM(CAST(%1$s AS VARCHAR)), '') AS device_id,
                   NULLIF(TRIM(CAST(%2$s AS VARCHAR)), '') AS entity_id
                 FROM csv_data
               ),
               val AS (
                 SELECT
                   %3$s AS expected_up,
                   %4$s AS entity_up
                 FROM src
               ),
               agg AS (
                 SELECT
                   COUNT(*) AS total_rows,
                   SUM(CASE WHEN expected_up IS NOT NULL THEN 1 ELSE 0 END) AS rule_rows,
                   SUM(CASE WHEN expected_up IS NOT NULL AND expected_up = entity_up THEN 1 ELSE 0 END) AS pass_rows,
                   SUM(CASE WHEN expected_up IS NOT NULL AND expected_up <> entity_up AND entity_up IS NOT NULL THEN 1 ELSE 0 END) AS fail_rows,
                   SUM(CASE WHEN expected_up IS NOT NULL AND entity_up IS NULL THEN 1 ELSE 0 END) AS missing_entity_rows,
                   SUM(CASE WHEN expected_up IS NULL THEN 1 ELSE 0 END) AS ignored_rows
                 FROM val
               )
               SELECT * FROM agg
               """
               .formatted(
                   CSV_ID_COL,
                   CSV_ENTITY_COL,
                   expectedUp,
                   entityUp
               );
    }

    // =========================
    // Excel Export + Helpers
    // =========================

    private enum SheetType { DATE, ID, ENTITY }

    private static void writeQueryToSheetWithHighlight(Connection conn, XSSFWorkbook wb, String sheetName, String sql, SheetType type) throws SQLException {
        Sheet sheet = wb.createSheet(sheetName);

        // Styles
        CellStyle headerStyle = wb.createCellStyle();
        Font headerFont = wb.createFont();
        headerFont.setBold(true);
        headerStyle.setFont(headerFont);

        // Red (errors / data missing)
        CellStyle redFill = wb.createCellStyle();
        redFill.setFillForegroundColor(IndexedColors.RED.getIndex());
        redFill.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        Font white = wb.createFont();
        white.setColor(IndexedColors.WHITE.getIndex());
        redFill.setFont(white);

        // Yellow (IGNORED)
        CellStyle yellowFill = wb.createCellStyle();
        yellowFill.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.getIndex());
        yellowFill.setFillPattern(FillPatternType.SOLID_FOREGROUND);

        // Green (PASS)
        CellStyle greenFill = wb.createCellStyle();
        greenFill.setFillForegroundColor(IndexedColors.BRIGHT_GREEN.getIndex());
        greenFill.setFillPattern(FillPatternType.SOLID_FOREGROUND);

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();

            // Header
            Row header = sheet.createRow(0);
            Map<String, Integer> labelToIndex = new HashMap<>();
            for (int i = 1; i <= cols; i++) {
                String label = md.getColumnLabel(i);
                Cell c = header.createCell(i - 1);
                c.setCellValue(label);
                c.setCellStyle(headerStyle);
                labelToIndex.put(label.toLowerCase(Locale.ROOT), i); // 1-based index
            }

            // Indices per sheet
            Integer idxResult      = labelToIndex.get("result");         // DATE/ID sheets
            Integer idxStatus      = labelToIndex.get("status");         // ENTITY sheet
            Integer idxExcelDate   = labelToIndex.get("measurement date");
            Integer idxCsvDate     = labelToIndex.get("created_on");
            Integer idxExcelId     = labelToIndex.get("device_sample_id_excel");
            Integer idxCsvId       = labelToIndex.get("device_sample_id_csv");
            Integer idxEntityId    = labelToIndex.get("entity_id");

            int rowNum = 1;
            while (rs.next()) {
                Row r = sheet.createRow(rowNum++);

                // Write values
                for (int i = 1; i <= cols; i++) {
                    Object val = rs.getObject(i);
                    Cell c = r.createCell(i - 1);
                    if (val == null) c.setBlank();
                    else c.setCellValue(String.valueOf(val));
                }

                // Decide coloring
                switch (type) {
                    case DATE -> {
                        String result = (idxResult == null) ? null : safeString(rs.getObject(idxResult));
                        if (equalsAny(result, "Date present in both xlsx and csv and it matches")) {
                            applyCellStyle(r, idxResult, greenFill);
                        } else if (equalsAny(result, "Present in xlsx only", "Present in csv only", "FAIL", "MISSING ENTITY")) {
                            applyCellStyle(r, idxResult, redFill);
                        }
                        if (idxExcelDate != null && rs.getObject(idxExcelDate) == null) applyCellStyle(r, idxExcelDate, redFill);
                        if (idxCsvDate   != null && rs.getObject(idxCsvDate)   == null) applyCellStyle(r, idxCsvDate, redFill);
                    }
                    case ID -> {
                        String result = (idxResult == null) ? null : safeString(rs.getObject(idxResult));
                        if (equalsAny(result, "ID present in both xlsx and csv and it matches")) {
                            applyCellStyle(r, idxResult, greenFill);
                        } else if (equalsAny(result, "Present in xlsx only", "Present in csv only", "FAIL", "MISSING ENTITY")) {
                            applyCellStyle(r, idxResult, redFill);
                        }
                        if (idxExcelId != null && rs.getObject(idxExcelId) == null) applyCellStyle(r, idxExcelId, redFill);
                        if (idxCsvId   != null && rs.getObject(idxCsvId)   == null) applyCellStyle(r, idxCsvId, redFill);
                    }
                    case ENTITY -> {
                        String status = (idxStatus == null) ? null : safeString(rs.getObject(idxStatus));
                        if (equalsAny(status, "PASS")) applyCellStyle(r, idxStatus, greenFill);
                        if (equalsAny(status, "IGNORED (no rule)")) applyCellStyle(r, idxStatus, yellowFill);
                        if (equalsAny(status, "FAIL", "MISSING ENTITY")) applyCellStyle(r, idxStatus, redFill);
                        if (equalsAny(status, "MISSING ENTITY") && idxEntityId != null) applyCellStyle(r, idxEntityId, redFill);
                    }
                }
            }

            for (int i = 0; i < cols; i++) sheet.autoSizeColumn(i);
        }
    }

    /**
     * Helper to safely apply cell style when column index exists.
     */
    private static void applyCellStyle(Row r, Integer colIdx, CellStyle style) {
        if (colIdx != null) {
            Cell c = r.getCell(colIdx - 1);
            if (c != null) {
                c.setCellStyle(style);
            }
        }
    }

    private static String summarySheets(XSSFWorkbook wb) {
        StringBuilder sb = new StringBuilder("Workbook sheets summary:");
        for (int i = 0; i < wb.getNumberOfSheets(); i++) {
            Sheet s = wb.getSheetAt(i);
            sb.append("\n  - ").append(s.getSheetName())
              .append(" (rows incl. header: ").append(s.getLastRowNum() + 1).append(")");
        }
        return sb.toString();
    }

    private static void writeCountsSheet(XSSFWorkbook wb, String sheetName, String[][] kvPairs) {
        Sheet sheet = wb.createSheet(sheetName);

        CellStyle headerStyle = wb.createCellStyle();
        Font headerFont = wb.createFont();
        headerFont.setBold(true);
        headerStyle.setFont(headerFont);

        Row h = sheet.createRow(0);
        Cell c1 = h.createCell(0); c1.setCellValue("metric"); c1.setCellStyle(headerStyle);
        Cell c2 = h.createCell(1); c2.setCellValue("value");  c2.setCellStyle(headerStyle);

        int row = 1;
        for (String[] kv : kvPairs) {
            Row r = sheet.createRow(row++);
            r.createCell(0).setCellValue(kv[0]);
            r.createCell(1).setCellValue(kv[1]);
        }
        sheet.autoSizeColumn(0);
        sheet.autoSizeColumn(1);
    }

    private static Map<String,String> fetchCountsRow(Connection conn, String countsSql) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(countsSql)) {
            Map<String,String> map = new LinkedHashMap<>();
            if (rs.next()) {
                ResultSetMetaData md = rs.getMetaData();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    String col = md.getColumnLabel(i);
                    String val = String.valueOf(rs.getObject(i));
                    map.put(col, val);
                }
            }
            return map;
        }
    }

    private static boolean equalsAny(String s, String... arr) {
        if (s == null) return false;
        for (String t : arr) if (s.equalsIgnoreCase(t)) return true;
        return false;
    }

    private static String safeString(Object o) { return (o == null) ? null : String.valueOf(o); }

    // =========================
    // Console Printer
    // =========================

    private static void executeSqlAndPrintAll(Connection conn, String title, String sql) throws SQLException {
        System.out.println("\n=== " + title + " ===");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();

            StringBuilder header = new StringBuilder();
            for (int i = 1; i <= cols; i++) {
                if (i > 1) header.append(" | ");
                header.append(md.getColumnLabel(i));
            }
            System.out.println(header);
            System.out.println("-".repeat(Math.max(120, header.length())));

            int printed = 0;
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) row.append(" | ");
                    Object val = rs.getObject(i);
                    row.append(val);
                }
                System.out.println(row);
                printed++;
            }
            System.out.println("(rows printed: " + printed + ")");
        }
    }

    private static long scalarLong(Connection conn, String sql) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    // =========================
    // Excel Ingestion (Apache POI)
    // =========================

    private static Path resolveResource(String classpathLocation, String fallbackRelative) throws Exception {
        URL url = DuckDBSampleTest.class.getResource(classpathLocation);
        if (url != null) return Paths.get(url.toURI());
        return Paths.get(fallbackRelative).toAbsolutePath();
    }

    /**
     * Insert Excel date/time values into excel_ts(unified_ts TIMESTAMP).
     */
    private static int ingestExcelDateTimes(Connection conn,
                                            Path excelPath,
                                            String sheetName,
                                            String headerName) throws Exception {
        Objects.requireNonNull(headerName, "headerName");
        int count = 0;

        try (InputStream in = Files.newInputStream(excelPath);
             Workbook wb = WorkbookFactory.create(in)) {

            Sheet sheet = getSheet(wb, sheetName);
            int colIdx  = findHeaderColumnIndex(sheet, headerName);
            if (colIdx < 0) throw new IllegalStateException("Header '" + headerName + "' not found in sheet: " + sheet.getSheetName());

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO excel_ts(unified_ts) VALUES (?)")) {
                DataFormatter fmt = new DataFormatter();

                for (int r = sheet.getFirstRowNum() + 1; r <= sheet.getLastRowNum(); r++) {
                    Row row = sheet.getRow(r);
                    if (row == null) continue;

                    Cell cell = row.getCell(colIdx, MissingCellPolicy.RETURN_BLANK_AS_NULL);
                    if (cell == null) continue;

                    LocalDateTime ldt = extractLocalDateTimeFromCell(cell, fmt);
                    if (ldt != null) {
                        ps.setTimestamp(1, Timestamp.valueOf(ldt));
                        ps.addBatch();
                        count++;
                        if (count % BATCH_SIZE == 0) ps.executeBatch();
                    }
                }
                ps.executeBatch();
            }
        }
        return count;
    }

    /**
     * Insert Excel device ids into excel_ids(excel_id VARCHAR).
     */
    private static int ingestExcelIds(Connection conn,
                                      Path excelPath,
                                      String sheetName,
                                      String headerName) throws Exception {
        Objects.requireNonNull(headerName, "headerName");
        int count = 0;

        try (InputStream in = Files.newInputStream(excelPath);
             Workbook wb = WorkbookFactory.create(in)) {

            Sheet sheet = getSheet(wb, sheetName);
            int colIdx  = findHeaderColumnIndex(sheet, headerName);
            if (colIdx < 0) throw new IllegalStateException("Header '" + headerName + "' not found in sheet: " + sheet.getSheetName());

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO excel_ids(excel_id) VALUES (?)")) {
                DataFormatter fmt = new DataFormatter();

                for (int r = sheet.getFirstRowNum() + 1; r <= sheet.getLastRowNum(); r++) {
                    Row row = sheet.getRow(r);
                    if (row == null) continue;

                    Cell cell = row.getCell(colIdx, MissingCellPolicy.RETURN_BLANK_AS_NULL);
                    if (cell == null) continue;

                    String id = fmt.formatCellValue(cell);
                    if (id != null) id = id.trim();
                    if (id != null && !id.isEmpty()) {
                        ps.setString(1, id);
                        ps.addBatch();
                        count++;
                        if (count % BATCH_SIZE == 0) ps.executeBatch();
                    }
                }
                ps.executeBatch();
            }
        }
        return count;
    }

    private static Sheet getSheet(Workbook wb, String sheetName) {
        Sheet sheet;
        if (sheetName != null) {
            sheet = wb.getSheet(sheetName);
            if (sheet == null) throw new IllegalArgumentException("Sheet '" + sheetName + "' not found");
        } else {
            sheet = wb.getNumberOfSheets() > 0 ? wb.getSheetAt(0) : null;
            if (sheet == null) throw new IllegalStateException("No sheets in workbook");
        }
        return sheet;
    }

    private static int findHeaderColumnIndex(Sheet sheet, String headerName) {
        Row header = sheet.getRow(sheet.getFirstRowNum());
        if (header == null) throw new IllegalStateException("Header row not found: " + sheet.getSheetName());

        for (int c = header.getFirstCellNum(); c < header.getLastCellNum(); c++) {
            Cell cell = header.getCell(c, MissingCellPolicy.RETURN_BLANK_AS_NULL);
            String text = (cell == null) ? "" : cell.toString().trim();
            if (headerName.equalsIgnoreCase(text)) {
                return c;
            }
        }
        return -1;
    }

    private static LocalDateTime extractLocalDateTimeFromCell(Cell cell, DataFormatter fmt) {
        if (cell.getCellType() == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
            java.util.Date d = cell.getDateCellValue();
            return d.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        } else if (cell.getCellType() == CellType.NUMERIC) {
            double serial = cell.getNumericCellValue();
            java.util.Date d = DateUtil.getJavaDate(serial, false);
            return d.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        } else if (cell.getCellType() == CellType.STRING) {
            return parseExcelTextToLocalDateTime(cell.getStringCellValue());
        } else {
            return parseExcelTextToLocalDateTime(fmt.formatCellValue(cell));
        }
    }

    private static LocalDateTime parseExcelTextToLocalDateTime(String s) {
        if (s == null || s.isBlank()) return null;
        s = s.trim();

        for (DateTimeFormatter f : EXCEL_TEXT_FORMATS) {
            try {
                return LocalDateTime.parse(s, f);
            } catch (DateTimeParseException ignore) {
                try {
                    LocalDate d = LocalDate.parse(s, f);
                    return d.atStartOfDay();
                } catch (DateTimeParseException ignore2) {
                    // try next format
                }
            }
        }
        return null;
    }
}
