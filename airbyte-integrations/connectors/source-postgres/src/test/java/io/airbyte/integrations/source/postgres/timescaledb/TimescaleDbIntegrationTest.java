/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.timescaledb;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.source.postgres.PostgresTestDatabase;
import io.airbyte.integrations.source.postgres.PostgresTestDatabase.BaseImage;
import io.airbyte.integrations.source.postgres.PostgresTestDatabase.ContainerModifier;
import io.airbyte.integrations.source.postgres.timescaledb.chunking.ChunkDiscoveryService;
import io.airbyte.integrations.source.postgres.timescaledb.chunking.ChunkMetadata;
import io.airbyte.integrations.source.postgres.timescaledb.config.TimescaleDbConfiguration;
import io.airbyte.integrations.source.postgres.timescaledb.utils.TimescaleDbMetrics;
import io.airbyte.integrations.source.postgres.timescaledb.utils.TimescaleDbQueries;
import io.airbyte.protocol.models.v0.AirbyteStream;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.v0.SyncMode;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for TimescaleDB functionality.
 * These tests require a PostgreSQL instance with TimescaleDB extension.
 */
public class TimescaleDbIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDbIntegrationTest.class);
  
  private PostgresTestDatabase testDatabase;
  private JdbcDatabase database;
  private JsonNode config;

  @BeforeEach
  void setup() {
    testDatabase = PostgresTestDatabase.in(BaseImage.POSTGRES_16, ContainerModifier.NETWORK);
    
    // Setup TimescaleDB extension and test data
    testDatabase.with("CREATE EXTENSION IF NOT EXISTS timescaledb;");
    
    // Create a test hypertable
    testDatabase.with("""
        CREATE TABLE metrics (
            time TIMESTAMPTZ NOT NULL,
            device_id TEXT NOT NULL,
            temperature DOUBLE PRECISION,
            humidity DOUBLE PRECISION
        );
        """);
    
    testDatabase.with("SELECT create_hypertable('metrics', 'time');");
    
    // Insert test data
    testDatabase.with("""
        INSERT INTO metrics (time, device_id, temperature, humidity) 
        SELECT 
            time,
            'device_' || (random() * 10)::int as device_id,
            20 + (random() * 20) as temperature,
            40 + (random() * 40) as humidity
        FROM generate_series(
            '2023-01-01 00:00:00'::timestamptz, 
            '2023-01-07 23:59:59'::timestamptz, 
            '5 minutes'::interval
        ) as time;
        """);
    
    // Create regular table for comparison
    testDatabase.with("""
        CREATE TABLE devices (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            location TEXT
        );
        """);
    
    testDatabase.with("""
        INSERT INTO devices (name, location) VALUES 
        ('Device A', 'Room 1'),
        ('Device B', 'Room 2'),
        ('Device C', 'Room 3');
        """);

    database = testDatabase.getDatabase();
    
    // Create configuration with TimescaleDB support enabled
    config = Jsons.jsonNode(Map.of(
        "host", testDatabase.getContainer().getHost(),
        "port", testDatabase.getContainer().getFirstMappedPort(),
        "database", testDatabase.getDatabaseName(),
        "username", testDatabase.getUserName(),
        "password", testDatabase.getPassword(),
        "timescaledb_support", true,
        "timescaledb_chunk_discovery_interval_minutes", 5,
        "timescaledb_max_concurrent_chunks", 2,
        "timescaledb_enable_chunk_caching", true,
        "timescaledb_chunk_cache_ttl_minutes", 10,
        "timescaledb_max_memory_mb", 256
    ));
  }

  @AfterEach
  void cleanup() {
    if (testDatabase != null) {
      testDatabase.close();
    }
  }

  @Test
  void testTimescaleDbExtensionDetection() throws Exception {
    final List<JsonNode> results = database.queryJsons(TimescaleDbQueries.CHECK_TIMESCALEDB_EXTENSION);
    assertFalse(results.isEmpty());
    assertTrue(results.get(0).get("timescaledb_available").asBoolean());
    LOGGER.info("TimescaleDB extension detected successfully");
  }

  @Test
  void testHypertableDiscovery() throws Exception {
    final List<JsonNode> results = database.queryJsons(TimescaleDbQueries.DISCOVER_HYPERTABLES);
    assertFalse(results.isEmpty());
    
    boolean foundMetricsTable = false;
    for (JsonNode result : results) {
      if ("metrics".equals(result.get("table_name").asText())) {
        foundMetricsTable = true;
        assertEquals("public", result.get("schema_name").asText());
        assertEquals("time", result.get("time_column_name").asText());
        break;
      }
    }
    assertTrue(foundMetricsTable, "Metrics hypertable should be discovered");
    LOGGER.info("Hypertable discovery successful, found {} hypertables", results.size());
  }

  @Test
  void testChunkDiscovery() throws Exception {
    final List<JsonNode> results = database.queryJsons(
        TimescaleDbQueries.DISCOVER_CHUNKS_FOR_HYPERTABLE, 
        "public", "metrics"
    );
    assertFalse(results.isEmpty());
    
    for (JsonNode result : results) {
      assertNotNull(result.get("chunk_schema"));
      assertNotNull(result.get("chunk_name"));
      assertEquals("public", result.get("hypertable_schema").asText());
      assertEquals("metrics", result.get("hypertable_name").asText());
      assertTrue(result.get("size_bytes").asLong() >= 0);
    }
    LOGGER.info("Chunk discovery successful, found {} chunks", results.size());
  }

  @Test
  void testTimescaleDbConfiguration() {
    final TimescaleDbConfiguration tsConfig = TimescaleDbConfiguration.fromConfig(config);
    
    assertTrue(tsConfig.isEnabled());
    assertEquals(5, tsConfig.getChunkDiscoveryIntervalMinutes());
    assertEquals(2, tsConfig.getMaxConcurrentChunks());
    assertTrue(tsConfig.isChunkCachingEnabled());
    assertEquals(10, tsConfig.getChunkCacheTtlMinutes());
    assertEquals(256, tsConfig.getMaxMemoryMb());
    
    // Test validation
    assertDoesNotThrow(() -> tsConfig.validate());
    LOGGER.info("TimescaleDB configuration validation successful");
  }

  @Test
  void testChunkDiscoveryService() throws Exception {
    final TimescaleDbConfiguration tsConfig = TimescaleDbConfiguration.fromConfig(config);
    final TimescaleDbMetrics metrics = new TimescaleDbMetrics();
    final ChunkDiscoveryService chunkService = new ChunkDiscoveryService(database, tsConfig, metrics);
    
    // Test hypertable detection
    final ConfiguredAirbyteStream metricsStream = createConfiguredStream("metrics", "public");
    assertTrue(chunkService.isHypertable(metricsStream));
    
    final ConfiguredAirbyteStream devicesStream = createConfiguredStream("devices", "public");
    assertFalse(chunkService.isHypertable(devicesStream));
    
    // Test chunk discovery
    final List<ChunkMetadata> chunks = chunkService.discoverChunks(metricsStream);
    assertFalse(chunks.isEmpty());
    
    for (ChunkMetadata chunk : chunks) {
      assertNotNull(chunk.getChunkSchema());
      assertNotNull(chunk.getChunkName());
      assertEquals("public", chunk.getHypertableSchema());
      assertEquals("metrics", chunk.getHypertableName());
      assertTrue(chunk.getSizeBytes() >= 0);
    }
    
    LOGGER.info("ChunkDiscoveryService test successful, found {} chunks", chunks.size());
  }

  @Test
  void testTimescaleDbDiscoveryHandler() throws Exception {
    // Test catalog enhancement
    final AirbyteStream metricsStream = new AirbyteStream()
        .withName("metrics")
        .withNamespace("public");
    
    final io.airbyte.protocol.models.v0.AirbyteCatalog catalog = 
        new io.airbyte.protocol.models.v0.AirbyteCatalog()
            .withStreams(List.of(metricsStream));
    
    final io.airbyte.protocol.models.v0.AirbyteCatalog enhancedCatalog = 
        TimescaleDbDiscoveryHandler.enhanceCatalog(catalog, database);
    
    assertNotNull(enhancedCatalog);
    assertEquals(1, enhancedCatalog.getStreams().size());
    
    final AirbyteStream enhancedStream = enhancedCatalog.getStreams().get(0);
    assertNotNull(enhancedStream.getJsonSchema());
    
    // Check for TimescaleDB metadata in stream properties
    if (enhancedStream.getJsonSchema().has("properties")) {
      LOGGER.info("Enhanced stream schema: {}", enhancedStream.getJsonSchema());
    }
    
    LOGGER.info("TimescaleDbDiscoveryHandler test successful");
  }

  @Test
  void testTimescaleDbSourceOperations() throws Exception {
    final TimescaleDbSourceOperations sourceOps = new TimescaleDbSourceOperations(database, config);
    
    assertNotNull(sourceOps.getConfiguration());
    assertNotNull(sourceOps.getMetrics());
    
    assertTrue(sourceOps.getConfiguration().isEnabled());
    
    LOGGER.info("TimescaleDbSourceOperations initialization successful");
  }

  @Test
  void testSchemaIncludeList() throws Exception {
    final List<JsonNode> results = database.queryJsons(TimescaleDbQueries.DISCOVER_HYPERTABLE_SCHEMAS);
    assertFalse(results.isEmpty());
    
    boolean foundPublicSchema = false;
    for (JsonNode result : results) {
      if ("public".equals(result.get("schema_name").asText())) {
        foundPublicSchema = true;
        break;
      }
    }
    assertTrue(foundPublicSchema, "Public schema should contain hypertables");
    
    LOGGER.info("Schema include list test successful");
  }

  @Test
  void testPerformanceMetrics() {
    final TimescaleDbMetrics metrics = new TimescaleDbMetrics();
    
    // Test basic metrics functionality
    metrics.recordChunkProcessed();
    metrics.recordChunkProcessed();
    metrics.recordMemoryUsage(100 * 1024 * 1024); // 100MB
    
    assertEquals(2, metrics.getChunksProcessed());
    assertEquals(100 * 1024 * 1024, metrics.getMaxMemoryUsage());
    
    final Map<String, Object> summary = metrics.getSummary();
    assertNotNull(summary);
    assertTrue(summary.containsKey("chunksProcessed"));
    assertTrue(summary.containsKey("maxMemoryUsage"));
    
    LOGGER.info("Performance metrics test successful: {}", summary);
  }

  private ConfiguredAirbyteStream createConfiguredStream(String name, String namespace) {
    final AirbyteStream stream = new AirbyteStream()
        .withName(name)
        .withNamespace(namespace);
    
    return new ConfiguredAirbyteStream()
        .withStream(stream)
        .withSyncMode(SyncMode.FULL_REFRESH);
  }
} 