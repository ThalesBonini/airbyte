/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.timescaledb.chunking;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.source.postgres.timescaledb.config.TimescaleDbConfiguration;
import io.airbyte.integrations.source.postgres.timescaledb.utils.TimescaleDbMetrics;
import io.airbyte.integrations.source.postgres.timescaledb.utils.TimescaleDbQueries;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for discovering TimescaleDB chunks with intelligent caching.
 * Implements self-contained chunk discovery without requiring database triggers.
 */
public class ChunkDiscoveryService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkDiscoveryService.class);

  private final JdbcDatabase database;
  private final TimescaleDbConfiguration config;
  private final TimescaleDbMetrics metrics;

  // Cache for chunk metadata
  private final Map<String, List<ChunkMetadata>> chunkCache = new ConcurrentHashMap<>();
  private final Map<String, Instant> lastDiscoveryTime = new ConcurrentHashMap<>();

  public ChunkDiscoveryService(final JdbcDatabase database, 
                              final TimescaleDbConfiguration config,
                              final TimescaleDbMetrics metrics) {
    this.database = database;
    this.config = config;
    this.metrics = metrics;
  }

  /**
   * Discovers chunks for a specific hypertable with caching support.
   */
  public List<ChunkMetadata> discoverChunks(final ConfiguredAirbyteStream stream) {
    final String streamKey = getStreamKey(stream);
    
    LOGGER.debug("Discovering chunks for stream: {}", streamKey);
    
    // Use cached chunks if valid and caching is enabled
    if (config.isEnableChunkCaching() && isCacheValid(streamKey)) {
      LOGGER.info("Using cached chunk information for {}", streamKey);
      return chunkCache.get(streamKey);
    }
    
    // Discover chunks from database
    final Instant startTime = Instant.now();
    final List<ChunkMetadata> chunks = queryChunks(stream);
    final Duration discoveryTime = Duration.between(startTime, Instant.now());
    
    // Update cache if caching is enabled
    if (config.isEnableChunkCaching()) {
      chunkCache.put(streamKey, chunks);
      lastDiscoveryTime.put(streamKey, Instant.now());
    }
    
    // Record metrics
    metrics.recordChunkDiscovery(streamKey, chunks.size(), discoveryTime);
    
    LOGGER.info("Discovered {} chunks for hypertable {} in {}ms", 
        chunks.size(), streamKey, discoveryTime.toMillis());
    
    return chunks;
  }

  /**
   * Discovers all chunks in the database for bulk operations.
   */
  public List<ChunkMetadata> discoverAllChunks() {
    LOGGER.info("Discovering all chunks in database");
    
    try {
      final Instant startTime = Instant.now();
      final List<JsonNode> results = database.queryJsons(TimescaleDbQueries.DISCOVER_ALL_CHUNKS);
      final Duration discoveryTime = Duration.between(startTime, Instant.now());
      
      final List<ChunkMetadata> chunks = results.stream()
          .map(this::mapToChunkMetadata)
          .collect(Collectors.toList());
      
      LOGGER.info("Discovered {} total chunks in database in {}ms", 
          chunks.size(), discoveryTime.toMillis());
      
      return chunks;
      
    } catch (final Exception e) {
      LOGGER.error("Failed to discover all chunks", e);
      return Collections.emptyList();
    }
  }

  /**
   * Checks if a table is a TimescaleDB hypertable.
   */
  public boolean isHypertable(final ConfiguredAirbyteStream stream) {
    try {
      final List<JsonNode> results = database.queryJsons(
          TimescaleDbQueries.CHECK_IS_HYPERTABLE,
          stream.getStream().getNamespace(),
          stream.getStream().getName()
      );
      
      final boolean isHypertable = !results.isEmpty() && 
          results.get(0).get("is_hypertable").asBoolean();
      
      LOGGER.debug("Table {}.{} is hypertable: {}", 
          stream.getStream().getNamespace(),
          stream.getStream().getName(),
          isHypertable);
      
      return isHypertable;
      
    } catch (final Exception e) {
      LOGGER.error("Failed to check if {}.{} is a hypertable", 
          stream.getStream().getNamespace(),
          stream.getStream().getName(), e);
      return false;
    }
  }

  /**
   * Gets the time column for a hypertable.
   */
  public String getTimeColumn(final ConfiguredAirbyteStream stream) {
    try {
      final List<JsonNode> results = database.queryJsons(
          TimescaleDbQueries.GET_TIME_COLUMN,
          stream.getStream().getNamespace(),
          stream.getStream().getName()
      );
      
      if (!results.isEmpty() && results.get(0).has("time_column")) {
        return results.get(0).get("time_column").asText();
      }
      
      LOGGER.warn("No time column found for hypertable {}.{}", 
          stream.getStream().getNamespace(),
          stream.getStream().getName());
      return null;
      
    } catch (final Exception e) {
      LOGGER.error("Failed to get time column for {}.{}", 
          stream.getStream().getNamespace(),
          stream.getStream().getName(), e);
      return null;
    }
  }

  /**
   * Invalidates cache for a specific stream or all streams.
   */
  public void invalidateCache(final String streamKey) {
    if (streamKey != null) {
      chunkCache.remove(streamKey);
      lastDiscoveryTime.remove(streamKey);
      LOGGER.info("Invalidated chunk cache for stream: {}", streamKey);
    } else {
      chunkCache.clear();
      lastDiscoveryTime.clear();
      LOGGER.info("Invalidated all chunk caches");
    }
  }

  /**
   * Gets cache statistics for monitoring.
   */
  public Map<String, Object> getCacheStats() {
    return Map.of(
        "cached_streams", chunkCache.size(),
        "total_cached_chunks", chunkCache.values().stream().mapToInt(List::size).sum(),
        "cache_enabled", config.isEnableChunkCaching(),
        "cache_ttl_minutes", config.getChunkCacheTtl().toMinutes()
    );
  }

  private String getStreamKey(final ConfiguredAirbyteStream stream) {
    return String.format("%s.%s", 
        stream.getStream().getNamespace(), 
        stream.getStream().getName());
  }

  private boolean isCacheValid(final String streamKey) {
    final Instant lastDiscovery = lastDiscoveryTime.get(streamKey);
    if (lastDiscovery == null) {
      return false;
    }
    
    final Duration timeSinceLastDiscovery = Duration.between(lastDiscovery, Instant.now());
    final boolean isValid = timeSinceLastDiscovery.compareTo(config.getChunkCacheTtl()) < 0;
    
    LOGGER.debug("Cache validity for {}: {} (age: {})", streamKey, isValid, timeSinceLastDiscovery);
    return isValid;
  }

  private List<ChunkMetadata> queryChunks(final ConfiguredAirbyteStream stream) {
    try {
      final List<JsonNode> results = database.queryJsons(
          TimescaleDbQueries.DISCOVER_CHUNKS_FOR_HYPERTABLE,
          stream.getStream().getNamespace(),
          stream.getStream().getName()
      );
      
      return results.stream()
          .map(this::mapToChunkMetadata)
          .collect(Collectors.toList());
          
    } catch (final Exception e) {
      LOGGER.error("Failed to discover chunks for {}", getStreamKey(stream), e);
      return Collections.emptyList();
    }
  }

  private ChunkMetadata mapToChunkMetadata(final JsonNode row) {
    return new ChunkMetadata(
        row.get("chunk_schema").asText(),
        row.get("chunk_name").asText(),
        row.get("hypertable_schema").asText(),
        row.get("hypertable_name").asText(),
        row.has("range_start") ? row.get("range_start").asText() : null,
        row.has("range_end") ? row.get("range_end").asText() : null,
        row.has("size_bytes") ? row.get("size_bytes").asLong() : 0L
    );
  }

} 