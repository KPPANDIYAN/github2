package com.gilead.testscripts.module1;

import org.testng.annotations.*;

/**
 * DuckDB Validation Test Suite - TestNG Implementation
 * 
 * This class contains ONLY @BeforeSuite, @Test, and @AfterSuite annotations.
 * All utilities and helpers are in DuckDBSampleTest.
 * 
 * Execution Order:
 * 1. setupSuite() - Initializes test environment
 * 2. testDateValidation (Priority 1)
 * 3. testDeviceIdValidation (Priority 2)
 * 4. testDeviceEntityValidation (Priority 3)
 * 5. testColumnSplitValidation (Priority 4)
 * 6. cleanupSuite() - Saves results and cleanup
 */
public class DuckDBValidationTestNG {

    @BeforeSuite
    public void setupSuite() throws Exception {
        System.out.println("\n╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║          DUCKDB VALIDATION TEST SUITE - INITIALIZATION             ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝\n");
        DuckDBSampleTest.initializeTestEnvironment();
    }

    @Test(priority = 1, description = "Validate DATE matching between Excel and CSV")
    public void testDateValidation() throws Exception {
        DuckDBSampleTest.executeTestDateValidation();
    }

    @Test(priority = 2, description = "Validate DEVICE ID matching between Excel and CSV")
    public void testDeviceIdValidation() throws Exception {
        DuckDBSampleTest.executeTestDeviceIdValidation();
    }

    @Test(priority = 3, description = "Validate DEVICE to ENTITY transformation")
    public void testDeviceEntityValidation() throws Exception {
        DuckDBSampleTest.executeTestDeviceEntityValidation();
    }

    @Test(priority = 4, description = "Validate COLUMN SPLITS (15 columns: Raw + LG)")
    public void testColumnSplitValidation() throws Exception {
        DuckDBSampleTest.executeTestColumnSplitValidation();
    }

    @AfterSuite
    public void cleanupSuite() throws Exception {
        System.out.println("\n╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║          DUCKDB VALIDATION TEST SUITE - CLEANUP                    ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        DuckDBSampleTest.cleanupTestEnvironment();
    }
}
