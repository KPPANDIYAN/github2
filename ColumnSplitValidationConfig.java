package com.gilead.testscripts.module1;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Configuration loader for column split validation.
 * Handles validation of columns that split from Excel into 2 CSV columns:
 * - Raw column: should match Excel exactly
 * - LG column: should contain only special characters (non-digits)
 */
public class ColumnSplitValidationConfig {

    public static class SplitColumnRule {
        public String excelColumn;
        public String rawColumn;
        public String lgColumn;
        public String description;

        @Override
        public String toString() {
            return String.format("%s (%s, %s)", excelColumn, rawColumn, lgColumn);
        }
    }

    public static class ValidationConfig {
        public List<SplitColumnRule> columnSplitValidations;

        public List<SplitColumnRule> getRules() {
            return columnSplitValidations != null ? columnSplitValidations : new ArrayList<>();
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper();
    private ValidationConfig config;

    /**
     * Load configuration from JSON file.
     * Tries multiple locations: current dir, classpath, src/test/resources/, workspace root
     */
    public static ColumnSplitValidationConfig load() throws IOException {
        ColumnSplitValidationConfig cfg = new ColumnSplitValidationConfig();
        cfg.loadFromFile();
        return cfg;
    }

    private void loadFromFile() throws IOException {
        String filename = "column-split-validations.json";

        // Try 1: current working directory
        Path path = Paths.get(filename);
        if (Files.exists(path)) {
            config = mapper.readValue(Files.readAllBytes(path), ValidationConfig.class);
            System.out.println("✓ Loaded column split validation config from: " + path.toAbsolutePath());
            return;
        }

        // Try 2: classpath resource
        try {
            var url = getClass().getClassLoader().getResource(filename);
            if (url != null) {
                config = mapper.readValue(url, ValidationConfig.class);
                System.out.println("✓ Loaded column split validation config from classpath: " + filename);
                return;
            }
        } catch (Exception e) {
            // Ignore, try next location
        }

        // Try 3: src/test/resources/
        path = Paths.get("src", "test", "resources", filename);
        if (Files.exists(path)) {
            config = mapper.readValue(Files.readAllBytes(path), ValidationConfig.class);
            System.out.println("✓ Loaded column split validation config from: " + path.toAbsolutePath());
            return;
        }

        // Try 4: workspace root
        path = Paths.get(System.getProperty("user.dir"), filename);
        if (Files.exists(path)) {
            config = mapper.readValue(Files.readAllBytes(path), ValidationConfig.class);
            System.out.println("✓ Loaded column split validation config from: " + path.toAbsolutePath());
            return;
        }

        throw new IOException("Could not find " + filename + " in any known location. " +
                "Place it in: current dir, classpath, or src/test/resources/");
    }

    /**
     * Get all split column rules
     */
    public List<SplitColumnRule> getRules() {
        return config.getRules();
    }

    /**
     * Validate that config is loaded correctly
     */
    public void validate() throws IOException {
        List<SplitColumnRule> rules = getRules();
        if (rules == null || rules.isEmpty()) {
            throw new IOException("No column split validations defined in config");
        }
        for (SplitColumnRule rule : rules) {
            if (rule.excelColumn == null || rule.excelColumn.isBlank()) {
                throw new IOException("Invalid rule: excelColumn is required");
            }
            if (rule.rawColumn == null || rule.rawColumn.isBlank()) {
                throw new IOException("Invalid rule for " + rule.excelColumn + ": rawColumn is required");
            }
            if (rule.lgColumn == null || rule.lgColumn.isBlank()) {
                throw new IOException("Invalid rule for " + rule.excelColumn + ": lgColumn is required");
            }
        }
    }

    /**
     * Get summary of all validation rules
     */
    public String getValidationsSummary() {
        StringBuilder sb = new StringBuilder("Column Split Validations:\n");
        List<SplitColumnRule> rules = getRules();

        if (rules.isEmpty()) {
            sb.append("  (No validations defined)\n");
            return sb.toString();
        }

        for (int i = 0; i < rules.size(); i++) {
            SplitColumnRule rule = rules.get(i);
            sb.append("  [").append(i + 1).append("] ")
                    .append(rule.excelColumn).append(" → ")
                    .append(rule.rawColumn).append(" + ")
                    .append(rule.lgColumn).append("\n");
        }
        return sb.toString();
    }

    /**
     * Build comprehensive SQL for all split column validations.
     * Validates 3 conditions for each column:
     * 1. Raw column must match Excel exactly
     * 2. LG column must contain only special characters (non-digits) from Excel
     * 3. If Excel is empty, CSV columns should be empty; if CSV has data, flag as ADDITIONAL_VALUE
     */
    public String buildValidationSql(String excelFilePath, String csvFilePath) {
        List<SplitColumnRule> rules = getRules();

        // Build UNION of validation queries for all split columns
        StringBuilder sqlBuilder = new StringBuilder();

        for (int i = 0; i < rules.size(); i++) {
            SplitColumnRule rule = rules.get(i);

            if (i > 0) {
                sqlBuilder.append("\nUNION ALL\n");
            }

            sqlBuilder.append(buildSingleColumnValidationSql(rule, excelFilePath, csvFilePath));
        }

        return sqlBuilder.toString();
    }

    /**
     * Build SQL for a single split column validation
     */
    private String buildSingleColumnValidationSql(SplitColumnRule rule, String excelFilePath, String csvFilePath) {
        return String.format("""
                WITH data AS (
                  SELECT
                    ROW_NUMBER() OVER () AS row_num,
                    CAST(excel."%s" AS VARCHAR) AS excel_value,
                    CAST(csv."%s" AS VARCHAR) AS csv_raw,
                    CAST(csv."%s" AS VARCHAR) AS csv_lg
                  FROM read_excel('%s') excel
                  CROSS JOIN read_csv_auto('%s') csv
                ),
                validation AS (
                  SELECT
                    row_num,
                    '%s' AS column_name,
                    excel_value,
                    csv_raw,
                    csv_lg,
                    CASE
                      -- Case 1: Excel is empty but CSV has data (ADDITIONAL_VALUE)
                      WHEN (excel_value IS NULL OR TRIM(excel_value) = '')
                        AND (csv_raw IS NOT NULL AND TRIM(csv_raw) != '')
                      THEN 'ADDITIONAL_VALUE'
                      
                      -- Case 2: Raw must always match Excel exactly
                      WHEN (excel_value IS NOT NULL AND TRIM(excel_value) != '')
                        AND TRIM(excel_value) != TRIM(csv_raw)
                      THEN 'RAW_MISMATCH'
                      
                      -- Case 3: LG validation depends on special character presence
                      WHEN (excel_value IS NOT NULL AND TRIM(excel_value) != '')
                      THEN
                        CASE
                          -- If Excel contains special chars (anything other than digits/dots)
                          WHEN regexp_like(TRIM(excel_value), '[^0-9.]')
                          THEN
                            -- LG should contain the ENTIRE value (not just special chars)
                            CASE
                              WHEN TRIM(excel_value) != COALESCE(TRIM(csv_lg), '')
                              THEN 'LG_MISMATCH'
                              ELSE 'PASS'
                            END
                          
                          -- If Excel contains ONLY digits/dots (no special chars)
                          ELSE
                            -- LG should be EMPTY/BLANK
                            CASE
                              WHEN COALESCE(TRIM(csv_lg), '') != ''
                              THEN 'LG_MISMATCH'
                              ELSE 'PASS'
                            END
                        END
                      
                      -- Case 4: Both Excel and CSV are empty
                      ELSE 'PASS'
                    END AS status
                  FROM data
                  WHERE row_num = row_num  -- Join on row number
                )
                SELECT
                  row_num,
                  column_name,
                  excel_value,
                  csv_raw,
                  csv_lg,
                  status
                FROM validation
                """,
                rule.excelColumn,
                rule.rawColumn,
                rule.lgColumn,
                excelFilePath,
                csvFilePath,
                rule.excelColumn
        );
    }


}
