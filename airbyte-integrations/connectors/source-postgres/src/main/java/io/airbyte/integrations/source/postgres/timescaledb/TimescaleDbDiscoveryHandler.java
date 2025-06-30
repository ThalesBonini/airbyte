/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.timescaledb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.source.postgres.timescaledb.utils.TimescaleDbQueries;
import io.airbyte.protocol.models.v0.AirbyteCatalog;
import io.airbyte.protocol.models.v0.AirbyteStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles discovery enhancement for TimescaleDB hypertables.
 * Adds TimescaleDB-specific metadata to the Airbyte catalog.
 */
public class TimescaleDbDiscoveryHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDbDiscoveryHandler.class);

  private TimescaleDbDiscoveryHandler() {
    // Utility class
  }

  /**
   * Enhances the discovered catalog with TimescaleDB-specific metadata.
   */
  public static AirbyteCatalog enhanceCatalog(final AirbyteCatalog catalog, final JdbcDatabase database) {
    LOGGER.info("Enhancing catalog with TimescaleDB metadata");
    
    try {
      // Get all hypertables information
      final Map<String, HypertableInfo> hypertables = discoverHypertables(database);
      
      // Enhance each stream if it's a hypertable
      final List<AirbyteStream> enhancedStreams = catalog.getStreams().stream()
          .map(stream -> enhanceStreamIfHypertable(stream, hypertables, database))
          .collect(Collectors.toList());
      
      LOGGER.info("Enhanced {} streams with TimescaleDB metadata", enhancedStreams.size());
      return new AirbyteCatalog().withStreams(enhancedStreams);
      
    } catch (final Exception e) {
      LOGGER.error("Failed to enhance catalog with TimescaleDB metadata", e);
      // Return original catalog if enhancement fails
      return catalog;
    }
  }

  /**
   * Checks if a table is a TimescaleDB hypertable.
   */
  public static boolean isHypertable(final AirbyteStream stream, final JdbcDatabase database) {
    try {
      final List<JsonNode> results = database.queryJsons(
          TimescaleDbQueries.CHECK_IS_HYPERTABLE,
          stream.getNamespace(),
          stream.getName()
      );
      
      final boolean isHypertable = !results.isEmpty() && 
          results.get(0).get("is_hypertable").asBoolean();
      
      LOGGER.debug("Table {}.{} is hypertable: {}", 
          stream.getNamespace(), stream.getName(), isHypertable);
      
      return isHypertable;
      
    } catch (final Exception e) {
      LOGGER.debug("Failed to check if {}.{} is a hypertable: {}", 
          stream.getNamespace(), stream.getName(), e.getMessage());
      return false;
    }
  }

  /**
   * Gets the list of schemas that should be included for TimescaleDB CDC.
   */
  public static String getSchemaIncludeList(final JdbcDatabase database) {
    try {
      final List<JsonNode> results = database.queryJsons(TimescaleDbQueries.BUILD_SCHEMA_INCLUDE_LIST);
      
      if (!results.isEmpty() && results.get(0).has("schema_list")) {
        final String schemaList = results.get(0).get("schema_list").asText();
        LOGGER.info("TimescaleDB schema include list: {}", schemaList);
        return schemaList;
      }
      
      // Fallback to just _timescaledb_internal
      LOGGER.warn("Could not build complete schema list, falling back to _timescaledb_internal only");
      return "_timescaledb_internal";
      
    } catch (final Exception e) {
      LOGGER.error("Failed to build TimescaleDB schema include list", e);
      return "_timescaledb_internal";
    }
  }

  private static Map<String, HypertableInfo> discoverHypertables(final JdbcDatabase database) {
    final Map<String, HypertableInfo> hypertables = new HashMap<>();
    
    try {
      final List<JsonNode> results = database.queryJsons(TimescaleDbQueries.DISCOVER_HYPERTABLES);
      
      for (final JsonNode row : results) {
        final String schemaName = row.get("schema_name").asText();
        final String tableName = row.get("table_name").asText();
        final String key = schemaName + "." + tableName;
        
        final HypertableInfo info = new HypertableInfo(
            schemaName,
            tableName,
            row.get("num_dimensions").asInt(),
            row.has("time_column_name") ? row.get("time_column_name").asText() : null,
            row.has("chunk_time_interval") ? row.get("chunk_time_interval").asText() : null
        );
        
        hypertables.put(key, info);
      }
      
      LOGGER.info("Discovered {} hypertables", hypertables.size());
      
    } catch (final Exception e) {
      LOGGER.error("Failed to discover hypertables", e);
    }
    
    return hypertables;
  }

  private static AirbyteStream enhanceStreamIfHypertable(final AirbyteStream stream,
                                                        final Map<String, HypertableInfo> hypertables,
                                                        final JdbcDatabase database) {
    final String streamKey = stream.getNamespace() + "." + stream.getName();
    final HypertableInfo hypertableInfo = hypertables.get(streamKey);
    
    if (hypertableInfo != null) {
      return enhanceStreamWithTimescaleDbMetadata(stream, hypertableInfo, database);
    }
    
    return stream;
  }

  private static AirbyteStream enhanceStreamWithTimescaleDbMetadata(final AirbyteStream stream,
                                                                   final HypertableInfo hypertableInfo,
                                                                   final JdbcDatabase database) {
    try {
      // Clone the existing JSON schema
      final ObjectNode jsonSchema = stream.getJsonSchema().deepCopy();
      
      // Add TimescaleDB-specific metadata
      final Map<String, Object> timescaleDbMetadata = new HashMap<>();
      timescaleDbMetadata.put("timescaledb_hypertable", true);
      timescaleDbMetadata.put("timescaledb_schema", hypertableInfo.schemaName);
      timescaleDbMetadata.put("timescaledb_table", hypertableInfo.tableName);
      timescaleDbMetadata.put("timescaledb_dimensions", hypertableInfo.numDimensions);
      
      if (hypertableInfo.timeColumnName != null) {
        timescaleDbMetadata.put("timescaledb_time_column", hypertableInfo.timeColumnName);
      }
      
      if (hypertableInfo.chunkTimeInterval != null) {
        timescaleDbMetadata.put("timescaledb_chunk_time_interval", hypertableInfo.chunkTimeInterval);
      }
      
      // Get chunk count for this hypertable
      final int chunkCount = getChunkCount(database, hypertableInfo);
      timescaleDbMetadata.put("timescaledb_chunk_count", chunkCount);
      
      // Add metadata to the JSON schema
      timescaleDbMetadata.forEach((key, value) -> {
        if (value instanceof String) {
          jsonSchema.put(key, (String) value);
        } else if (value instanceof Integer) {
          jsonSchema.put(key, (Integer) value);
        } else if (value instanceof Boolean) {
          jsonSchema.put(key, (Boolean) value);
        }
      });
      
      LOGGER.debug("Enhanced stream {} with TimescaleDB metadata: {} chunks, time column: {}", 
          stream.getName(), chunkCount, hypertableInfo.timeColumnName);
      
      return stream.withJsonSchema(jsonSchema);
      
    } catch (final Exception e) {
      LOGGER.error("Failed to enhance stream {} with TimescaleDB metadata", stream.getName(), e);
      return stream;
    }
  }

  private static int getChunkCount(final JdbcDatabase database, final HypertableInfo hypertableInfo) {
    try {
      final List<JsonNode> results = database.queryJsons(
          "SELECT COUNT(*) as chunk_count FROM timescaledb_information.chunks " +
          "WHERE hypertable_schema = ? AND hypertable_name = ?",
          hypertableInfo.schemaName,
          hypertableInfo.tableName
      );
      
      if (!results.isEmpty()) {
        return results.get(0).get("chunk_count").asInt();
      }
      
    } catch (final Exception e) {
      LOGGER.debug("Failed to get chunk count for {}.{}", 
          hypertableInfo.schemaName, hypertableInfo.tableName);
    }
    
    return 0;
  }

  /**
   * Information about a TimescaleDB hypertable.
   */
  private static class HypertableInfo {
    final String schemaName;
    final String tableName;
    final int numDimensions;
    final String timeColumnName;
    final String chunkTimeInterval;

    HypertableInfo(final String schemaName,
                   final String tableName,
                   final int numDimensions,
                   final String timeColumnName,
                   final String chunkTimeInterval) {
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.numDimensions = numDimensions;
      this.timeColumnName = timeColumnName;
      this.chunkTimeInterval = chunkTimeInterval;
    }
  }

} 