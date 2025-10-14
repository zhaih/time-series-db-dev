/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.opensearch.tsdb.framework.models.TestCase;
import org.opensearch.tsdb.framework.models.TestSetup;

import java.io.InputStream;
import java.time.Instant;

/**
 * Generic YAML loader for test configuration files using Jackson.
 * Can load both TestCase and TestSetup configurations.
 *
 * This loader ensures that all relative timestamps (e.g., "now-50m") in a single
 * test file use the same reference time for consistency and to prevent test flakiness.
 *
 * Usage patterns:
 *
 * 1. Single file with independent reference time:
 *    TestCase testCase = YamlLoader.loadTestCase("path/to/test.yaml");
 *
 * 2. Multiple files with shared reference time (recommended for consistency):
 *    Instant referenceTime = Instant.now();
 *    TestSetup setup = YamlLoader.loadTestSetup("setup.yaml", referenceTime);
 *    TestCase testCase = YamlLoader.loadTestCase("test.yaml", referenceTime);
 */
public class YamlLoader {

    /**
     * Attribute key for storing the reference time in Jackson's DeserializationContext.
     * This allows all TimestampDeserializer instances to use the same "now" reference.
     */
    public static final String REFERENCE_TIME_KEY = "referenceTime";

    private static final ObjectMapper yamlMapper = createYamlMapper();

    private static ObjectMapper createYamlMapper() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

        return mapper;
    }

    /**
     * Load a TestCase from a YAML file.
     * Captures a single reference time at the start to ensure all relative timestamps
     * (e.g., "now-50m") use the same base time, preventing test flakiness.
     */
    public static TestCase loadTestCase(String resourcePath) throws Exception {
        return loadTestCase(resourcePath, Instant.now());
    }

    /**
     * Load a TestCase from a YAML file with a specific reference time.
     * Use this when loading multiple YAML files that should share the same reference time.
     *
     * @param resourcePath Path to the YAML test case file
     * @param referenceTime The reference time to use for all relative timestamp calculations
     * @return The loaded TestCase
     */
    public static TestCase loadTestCase(String resourcePath, Instant referenceTime) throws Exception {
        try (InputStream is = YamlLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new RuntimeException("Test case file not found: " + resourcePath);
            }

            // Create a reader with custom context containing the reference time
            TestCaseWrapper wrapper = yamlMapper.reader()
                .withAttribute(REFERENCE_TIME_KEY, referenceTime)
                .readValue(is, TestCaseWrapper.class);

            return wrapper.testCase;
        }
    }

    /**
     * Load a TestSetup from a YAML file.
     * Captures a single reference time at the start to ensure all relative timestamps
     * (e.g., "now-50m") use the same base time, preventing test flakiness.
     */
    public static TestSetup loadTestSetup(String resourcePath) throws Exception {
        return loadTestSetup(resourcePath, Instant.now());
    }

    /**
     * Load a TestSetup from a YAML file with a specific reference time.
     * Use this when loading multiple YAML files that should share the same reference time.
     *
     * @param resourcePath Path to the YAML test setup file
     * @param referenceTime The reference time to use for all relative timestamp calculations
     * @return The loaded TestSetup
     */
    public static TestSetup loadTestSetup(String resourcePath, Instant referenceTime) throws Exception {
        try (InputStream is = YamlLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new RuntimeException("Test setup file not found: " + resourcePath);
            }

            // Create a reader with custom context containing the reference time
            TestSetupWrapper wrapper = yamlMapper.reader()
                .withAttribute(REFERENCE_TIME_KEY, referenceTime)
                .readValue(is, TestSetupWrapper.class);

            return wrapper.testSetup;
        }
    }

    /**
     * Wrapper class for the test_case root element
     */
    private static class TestCaseWrapper {
        @JsonProperty("test_case")
        public TestCase testCase;
    }

    /**
     * Wrapper class for the test_setup root element
     */
    private static class TestSetupWrapper {
        @JsonProperty("test_setup")
        public TestSetup testSetup;
    }
}
