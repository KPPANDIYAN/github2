package com.gilead.testscripts.module1;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Simplified Device-to-Entity mapping configuration loader.
 * Uses simple prefix → replacement mappings from JSON.
 * Easy to extend: just add new prefix/replacement pairs to JSON!
 */
public class DeviceEntityMappingConfigSimplified {

    public static class DeviceMapping {
        public String prefix;
        public String replacement;

        @Override
        public String toString() {
            return String.format("%s → %s", prefix, replacement);
        }
    }

    public static class MappingConfig {
        public List<DeviceMapping> deviceMappings;

        public List<DeviceMapping> getMappings() {
            return deviceMappings != null ? deviceMappings : new ArrayList<>();
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper();
    private MappingConfig config;

    /**
     * Load configuration from JSON file.
     * Tries multiple locations: current dir, classpath, src/test/resources/, workspace root
     */
    public static DeviceEntityMappingConfigSimplified load() throws IOException {
        DeviceEntityMappingConfigSimplified cfg = new DeviceEntityMappingConfigSimplified();
        cfg.loadFromFile();
        return cfg;
    }

    private void loadFromFile() throws IOException {
        String filename = "device-entity-mappings-simplified.json";

        // Try 1: current working directory
        Path path = Paths.get(filename);
        if (Files.exists(path)) {
            config = mapper.readValue(Files.readAllBytes(path), MappingConfig.class);
            System.out.println("✓ Loaded mapping config from: " + path.toAbsolutePath());
            return;
        }

        // Try 2: classpath resource
        try {
            var url = getClass().getClassLoader().getResource(filename);
            if (url != null) {
                config = mapper.readValue(url, MappingConfig.class);
                System.out.println("✓ Loaded mapping config from classpath: " + filename);
                return;
            }
        } catch (Exception e) {
            // Ignore, try next location
        }

        // Try 3: src/test/resources/
        path = Paths.get("src", "test", "resources", filename);
        if (Files.exists(path)) {
            config = mapper.readValue(Files.readAllBytes(path), MappingConfig.class);
            System.out.println("✓ Loaded mapping config from: " + path.toAbsolutePath());
            return;
        }

        // Try 4: workspace root
        path = Paths.get(System.getProperty("user.dir"), filename);
        if (Files.exists(path)) {
            config = mapper.readValue(Files.readAllBytes(path), MappingConfig.class);
            System.out.println("✓ Loaded mapping config from: " + path.toAbsolutePath());
            return;
        }

        throw new IOException("Could not find " + filename + " in any known location. " +
                "Place it in: current dir, classpath, or src/test/resources/");
    }

    /**
     * Get all device mappings
     */
    public List<DeviceMapping> getMappings() {
        return config.getMappings();
    }

    /**
     * Build SQL CASE expression from simple prefix → replacement mappings.
     * 
     * Logic:
     * 1. Extract first token from device_id (up to space, dash, or underscore)
     * 2. Check each prefix in order
     * 3. If prefix matches, return replacement
     * 4. Else return NULL
     */
    public String buildSqlExpression(String deviceIdColumn, boolean caseInsensitive) {
        List<DeviceMapping> mappings = getMappings();
        
        StringBuilder sql = new StringBuilder("CASE ");

        // Handle NULL case first
        sql.append("WHEN ").append(deviceIdColumn).append(" IS NULL THEN NULL ");

        // Add WHEN clause for each mapping
        // Key: Extract prefix, check if matches, then concat with suffix
        // Example: "767010" with prefix "767" → "CCSMP" || "010" = "CCSMP010"
        for (DeviceMapping mapping : mappings) {
            int prefixLen = mapping.prefix.length();
            String prefix = caseInsensitive ? mapping.prefix.toUpperCase() : mapping.prefix;
            String prefixCheck = caseInsensitive 
                ? "UPPER(SUBSTRING(" + deviceIdColumn + ", 1, " + prefixLen + "))"
                : "SUBSTRING(" + deviceIdColumn + ", 1, " + prefixLen + ")";
            String suffixExtract = "SUBSTRING(" + deviceIdColumn + ", " + (prefixLen + 1) + ")";
            
            sql.append("WHEN LENGTH(").append(deviceIdColumn).append(") >= ")
                    .append(prefixLen)
                    .append(" AND ").append(prefixCheck).append(" = '")
                    .append(prefix).append("' THEN '")
                    .append(mapping.replacement).append("' || ").append(suffixExtract).append(" ");
        }

        // Default case: no mapping found
        sql.append("ELSE NULL END");

        return sql.toString();
    }

    /**
     * Generate a human-readable summary of mappings
     */
    public String getMappingsSummary() {
        StringBuilder sb = new StringBuilder("Device-to-Entity Mappings:\n");
        List<DeviceMapping> mappings = getMappings();
        
        if (mappings.isEmpty()) {
            sb.append("  (No mappings defined)\n");
            return sb.toString();
        }

        for (int i = 0; i < mappings.size(); i++) {
            DeviceMapping m = mappings.get(i);
            sb.append("  [").append(i + 1).append("] ")
                    .append(m.prefix).append(" → ")
                    .append(m.replacement).append("\n");
        }

        return sb.toString();
    }

    /**
     * Validate configuration is loaded and mappings exist
     */
    public void validate() throws IllegalStateException {
        if (config == null) {
            throw new IllegalStateException("Configuration not loaded");
        }
        if (config.deviceMappings == null || config.deviceMappings.isEmpty()) {
            throw new IllegalStateException("No device mappings found in configuration");
        }
    }

    /**
     * Get configuration info (mapping count)
     */
    public String getConfigInfo() {
        return String.format("DeviceMappingConfig[mappings=%d]", getMappings().size());
    }
}
