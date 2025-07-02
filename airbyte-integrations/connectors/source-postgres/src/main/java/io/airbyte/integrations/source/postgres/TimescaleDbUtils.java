/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.commons.exceptions.ConfigErrorException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for TimescaleDB-specific operations including extension detection,
 * table filtering, hypertable discovery, and publication validation.
 */
public class TimescaleDbUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDbUtils.class);

  // Patterns for TimescaleDB internal tables that should be hidden from users
  private static final Set<String> INTERNAL_TABLE_PATTERNS = Set.of(
      "_hyper_\\d+_\\d+_chunk", // Chunk tables (e.g., _hyper_1_1_chunk)
      "bgw_job_stat", // Background worker stats
      "bgw_job_stat_history", // Background worker history
      "bgw_policy_chunk_stats", // Policy chunk stats
      "hypertable_compression_stats", // Compression statistics
      "chunk_compression_stats", // Chunk compression statistics
      "compression_chunk_size" // Compression chunk size info
  );

  // Additional internal tables that should always be hidden
  private static final Set<String> INTERNAL_TABLE_NAMES = Set.of(
      "hypertable",
      "dimension",
      "dimension_slice",
      "chunk",
      "chunk_constraint",
      "chunk_index",
      "tablespace",
      "bgw_policy_compress_chunks",
      "bgw_policy_drop_chunks",
      "bgw_policy_reorder_chunks",
      "bgw_policy_telemetry"
  );

  /**
   * Check if TimescaleDB extension is installed and available in the database.
   *
   * @param database The JDBC database connection
   * @return true if TimescaleDB extension exists, false otherwise
   * @throws SQLException if query execution fails
   */
  public static boolean isTimescaleDbExtensionPresent(final JdbcDatabase database) throws SQLException {
    final String query = "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'";
    final List<JsonNode> result = database.queryJsons(query);
    final boolean isPresent = !result.isEmpty();
    
    if (isPresent) {
      LOGGER.info("TimescaleDB extension detected in database");
    } else {
      LOGGER.debug("TimescaleDB extension not found in database");
    }
    
    return isPresent;
  }

  /**
   * Determine if a table should be exposed to users in the catalog.
   * This method hides internal TimescaleDB tables and chunks from the user interface.
   *
   * @param database The JDBC database connection
   * @param schemaName The schema name of the table
   * @param tableName The name of the table
   * @return true if the table should be visible to users, false if it should be hidden
   * @throws SQLException if query execution fails
   */
  public static boolean shouldExposeTableToUser(final JdbcDatabase database, 
                                               final String schemaName, 
                                               final String tableName) throws SQLException {
    // Always expose tables in user schemas (not _timescaledb_internal)
    if (!"_timescaledb_internal".equals(schemaName)) {
      return true;
    }

    // Hide chunk tables using regex patterns
    for (final String pattern : INTERNAL_TABLE_PATTERNS) {
      if (tableName.matches(pattern)) {
        LOGGER.debug("Hiding internal TimescaleDB table: {}.{}", schemaName, tableName);
        return false;
      }
    }

    // Hide known internal table names
    if (INTERNAL_TABLE_NAMES.contains(tableName)) {
      LOGGER.debug("Hiding internal TimescaleDB table: {}.{}", schemaName, tableName);
      return false;
    }

    // Expose other _timescaledb_internal tables (like information views)
    LOGGER.debug("Exposing TimescaleDB internal table: {}.{}", schemaName, tableName);
    return true;
  }

  /**
   * Discover hypertables in the specified schema.
   *
   * @param database The JDBC database connection
   * @param schema The schema to search for hypertables
   * @return List of hypertable names in the specified schema
   * @throws SQLException if query execution fails
   */
  public static List<String> getHypertables(final JdbcDatabase database, final String schema) throws SQLException {
    try {
      final String query = 
          "SELECT table_name FROM information_schema.tables WHERE table_schema = ? " +
          "AND table_name IN (SELECT table_name FROM timescaledb_information.hypertables " +
          "WHERE hypertable_schema = ?)";
      
      final List<String> hypertables = database.queryJsons(query, schema, schema)
          .stream()
          .map(row -> {
            final JsonNode tableNameNode = row.get("table_name");
            if (tableNameNode == null || tableNameNode.isNull()) {
              LOGGER.warn("Found hypertable row with null table_name in schema '{}'", schema);
              return null;
            }
            return tableNameNode.asText();
          })
          .filter(tableName -> tableName != null && !tableName.isEmpty())
          .collect(Collectors.toList());
      
      LOGGER.info("Found {} hypertables in schema '{}': {}", hypertables.size(), schema, hypertables);
      return hypertables;
    } catch (final SQLException e) {
      LOGGER.error("Failed to discover hypertables in schema '{}': {}", schema, e.getMessage());
      if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
        LOGGER.warn("TimescaleDB information views may not be available. Ensure TimescaleDB extension is properly installed and accessible.");
        return Collections.emptyList();
      }
      throw e;
    }
  }

  /**
   * Discover continuous aggregates in the specified schema.
   *
   * @param database The JDBC database connection
   * @param schema The schema to search for continuous aggregates
   * @return List of continuous aggregate view names in the specified schema
   * @throws SQLException if query execution fails
   */
  public static List<String> getContinuousAggregates(final JdbcDatabase database, final String schema) throws SQLException {
    try {
      final String query = 
          "SELECT view_name FROM timescaledb_information.continuous_aggregates " +
          "WHERE view_schema = ?";
      
      final List<String> aggregates = database.queryJsons(query, schema)
          .stream()
          .map(row -> {
            final JsonNode viewNameNode = row.get("view_name");
            if (viewNameNode == null || viewNameNode.isNull()) {
              LOGGER.warn("Found continuous aggregate row with null view_name in schema '{}'", schema);
              return null;
            }
            return viewNameNode.asText();
          })
          .filter(viewName -> viewName != null && !viewName.isEmpty())
          .collect(Collectors.toList());
      
      LOGGER.info("Found {} continuous aggregates in schema '{}': {}", aggregates.size(), schema, aggregates);
      return aggregates;
    } catch (final SQLException e) {
      LOGGER.error("Failed to discover continuous aggregates in schema '{}': {}", schema, e.getMessage());
      if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
        LOGGER.warn("TimescaleDB continuous aggregates information view may not be available. Ensure TimescaleDB extension is properly installed and accessible.");
        return Collections.emptyList();
      }
      throw e;
    }
  }

  /**
   * Validate that the publication includes the necessary tables for TimescaleDB.
   * TimescaleDB requires either ALL TABLES or explicit inclusion of _timescaledb_internal schema.
   *
   * @param database The JDBC database connection
   * @param publicationName The name of the publication to validate
   * @throws SQLException if query execution fails
   * @throws ConfigErrorException if publication is not properly configured for TimescaleDB
   */
  public static void validatePublication(final JdbcDatabase database, final String publicationName) throws SQLException {
    LOGGER.info("Validating publication '{}' for TimescaleDB compatibility", publicationName);
    
    // Check if publication publishes all tables
    final String allTablesQuery = "SELECT puballtables FROM pg_publication WHERE pubname = ?";
    final boolean hasAllTables = database.queryJsons(allTablesQuery, publicationName)
        .stream()
        .anyMatch(row -> row.get("puballtables").asBoolean());

    if (hasAllTables) {
      LOGGER.info("Publication '{}' includes ALL TABLES - TimescaleDB compatible", publicationName);
      return;
    }

    // If not all tables, check if _timescaledb_internal schema is included
    final String schemaTablesQuery = "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = ?";
    final List<JsonNode> tables = database.queryJsons(schemaTablesQuery, publicationName);
    
    final boolean hasInternalSchema = tables.stream()
        .anyMatch(row -> "_timescaledb_internal".equals(row.get("schemaname").asText()));

    if (!hasInternalSchema) {
      final String errorMessage = String.format(
          "TimescaleDB support requires publication '%s' to include ALL TABLES or explicitly include _timescaledb_internal schema. " +
          "Current publication does not include _timescaledb_internal schema tables. " +
          "Please update your publication with: ALTER PUBLICATION %s ADD ALL TABLES IN SCHEMA _timescaledb_internal;",
          publicationName, publicationName);
      
      LOGGER.error("Publication validation failed: {}", errorMessage);
      throw new ConfigErrorException(errorMessage);
    }

    LOGGER.info("Publication '{}' includes _timescaledb_internal schema - TimescaleDB compatible", publicationName);
  }

  /**
   * Validate that TimescaleDB information views are accessible.
   * This method performs basic connectivity checks to ensure TimescaleDB views can be queried.
   *
   * @param database The JDBC database connection
   * @return true if information views are accessible, false otherwise
   */
  public static boolean validateTimescaleDbInformationViews(final JdbcDatabase database) {
    try {
      // Test hypertables information view
      final String hypertablesQuery = "SELECT 1 FROM timescaledb_information.hypertables LIMIT 1";
      database.queryJsons(hypertablesQuery);
      
      // Test continuous aggregates information view
      final String continuousAggregatesQuery = "SELECT 1 FROM timescaledb_information.continuous_aggregates LIMIT 1";
      database.queryJsons(continuousAggregatesQuery);
      
      LOGGER.debug("TimescaleDB information views validation successful");
      return true;
    } catch (final SQLException e) {
      LOGGER.warn("TimescaleDB information views validation failed: {}. " +
                  "This may indicate TimescaleDB extension is not properly installed or accessible.", e.getMessage());
      return false;
    }
  }

  /**
   * Check TimescaleDB version compatibility and log warnings for potential issues.
   *
   * @param database The JDBC database connection
   * @return true if version is compatible or check fails, false only for known incompatible versions
   */
  public static boolean checkTimescaleDbVersionCompatibility(final JdbcDatabase database) {
    try {
      final String version = getTimescaleDbVersion(database);
      if (version == null) {
        LOGGER.warn("Unable to determine TimescaleDB version. Proceeding with default behavior.");
        return true;
      }

      // Parse major version (e.g., "2.11.2" -> 2)
      final String[] versionParts = version.split("\\.");
      if (versionParts.length > 0) {
        try {
          final int majorVersion = Integer.parseInt(versionParts[0]);
          
          // TimescaleDB 2.x is well supported, 1.x might have limitations
          if (majorVersion < 2) {
            LOGGER.warn("TimescaleDB version {} detected. Version 2.x or higher is recommended for optimal compatibility.", version);
          } else {
            LOGGER.info("TimescaleDB version {} is compatible.", version);
          }
          
          return true;
        } catch (final NumberFormatException e) {
          LOGGER.warn("Unable to parse TimescaleDB version '{}'. Proceeding with default behavior.", version);
          return true;
        }
      }
      
      return true;
    } catch (final Exception e) {
      LOGGER.warn("TimescaleDB version compatibility check failed: {}. Proceeding with default behavior.", e.getMessage());
      return true;
    }
  }

  /**
   * Check if TimescaleDB support is enabled in the source configuration.
   *
   * @param sourceConfig The source configuration JSON node
   * @return true if TimescaleDB support is enabled, false otherwise
   */
  public static boolean isTimescaleDbEnabled(final JsonNode sourceConfig) {
    return sourceConfig.has("replication_method") &&
           sourceConfig.get("replication_method").has("enable_timescaledb_support") &&
           sourceConfig.get("replication_method").get("enable_timescaledb_support").asBoolean();
  }

  /**
   * Check if chunk tables should be hidden from the catalog.
   *
   * @param sourceConfig The source configuration JSON node
   * @return true if chunk tables should be hidden (default behavior)
   */
  public static boolean shouldHideChunkTables(final JsonNode sourceConfig) {
    if (!isTimescaleDbEnabled(sourceConfig)) {
      return false; // Not relevant if TimescaleDB support is disabled
    }

    final JsonNode transforms = sourceConfig.get("replication_method").get("timescaledb_transforms");
    if (transforms == null || !transforms.has("hide_chunk_tables")) {
      return true; // Default to hiding chunk tables
    }

    return transforms.get("hide_chunk_tables").asBoolean();
  }

  /**
   * Check if hypertables should be auto-detected.
   *
   * @param sourceConfig The source configuration JSON node
   * @return true if hypertables should be auto-detected (default behavior)
   */
  public static boolean shouldAutoDetectHypertables(final JsonNode sourceConfig) {
    if (!isTimescaleDbEnabled(sourceConfig)) {
      return false; // Not relevant if TimescaleDB support is disabled
    }

    final JsonNode transforms = sourceConfig.get("replication_method").get("timescaledb_transforms");
    if (transforms == null || !transforms.has("auto_detect_hypertables")) {
      return true; // Default to auto-detecting hypertables
    }

    return transforms.get("auto_detect_hypertables").asBoolean();
  }

  /**
   * Get TimescaleDB version information for debugging and compatibility checks.
   *
   * @param database The JDBC database connection
   * @return TimescaleDB version string, or null if extension is not available
   * @throws SQLException if query execution fails
   */
  public static String getTimescaleDbVersion(final JdbcDatabase database) throws SQLException {
    if (!isTimescaleDbExtensionPresent(database)) {
      return null;
    }

    try {
      final String query = "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'";
      final List<JsonNode> result = database.queryJsons(query);
      
      if (!result.isEmpty()) {
        final String version = result.get(0).get("extversion").asText();
        LOGGER.info("TimescaleDB version: {}", version);
        return version;
      }
    } catch (final SQLException e) {
      LOGGER.warn("Failed to retrieve TimescaleDB version: {}", e.getMessage());
    }

    return null;
  }
} 