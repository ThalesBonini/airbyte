/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.timescaledb.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.source.postgres.PostgresUtils;
import java.time.Duration;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration class for TimescaleDB-specific settings.
 * Follows Airbyte's configuration patterns and provides validation.
 */
@JsonDeserialize(builder = TimescaleDbConfiguration.Builder.class)
public class TimescaleDbConfiguration {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDbConfiguration.class);

  // Default configuration values
  private static final boolean DEFAULT_ENABLED = false;
  private static final Duration DEFAULT_CHUNK_DISCOVERY_INTERVAL = Duration.ofMinutes(5);
  private static final int DEFAULT_MAX_CONCURRENT_CHUNKS = 1;
  private static final boolean DEFAULT_ENABLE_CHUNK_CACHING = true;
  private static final Duration DEFAULT_CHUNK_CACHE_TTL = Duration.ofMinutes(10);
  private static final long DEFAULT_MAX_MEMORY_MB = 512;

  private final boolean enabled;
  private final Duration chunkDiscoveryInterval;
  private final int maxConcurrentChunks;
  private final boolean enableChunkCaching;
  private final Duration chunkCacheTtl;
  private final long maxMemoryMb;

  @JsonCreator
  private TimescaleDbConfiguration(
      @JsonProperty("enabled") final Boolean enabled,
      @JsonProperty("chunkDiscoveryInterval") final Duration chunkDiscoveryInterval,
      @JsonProperty("maxConcurrentChunks") final Integer maxConcurrentChunks,
      @JsonProperty("enableChunkCaching") final Boolean enableChunkCaching,
      @JsonProperty("chunkCacheTtl") final Duration chunkCacheTtl,
      @JsonProperty("maxMemoryMb") final Long maxMemoryMb) {
    this.enabled = enabled != null ? enabled : DEFAULT_ENABLED;
    this.chunkDiscoveryInterval = chunkDiscoveryInterval != null ? chunkDiscoveryInterval : DEFAULT_CHUNK_DISCOVERY_INTERVAL;
    this.maxConcurrentChunks = maxConcurrentChunks != null ? maxConcurrentChunks : DEFAULT_MAX_CONCURRENT_CHUNKS;
    this.enableChunkCaching = enableChunkCaching != null ? enableChunkCaching : DEFAULT_ENABLE_CHUNK_CACHING;
    this.chunkCacheTtl = chunkCacheTtl != null ? chunkCacheTtl : DEFAULT_CHUNK_CACHE_TTL;
    this.maxMemoryMb = maxMemoryMb != null ? maxMemoryMb : DEFAULT_MAX_MEMORY_MB;
  }

  /**
   * Checks if TimescaleDB support should be enabled based on configuration and database availability.
   */
  public static boolean isEnabled(final JsonNode config, final JdbcDatabase database) {
    if (config == null || !config.has("timescaledb_support")) {
      LOGGER.debug("TimescaleDB support not configured");
      return false;
    }

    final boolean configEnabled = config.get("timescaledb_support").asBoolean();
    if (!configEnabled) {
      LOGGER.debug("TimescaleDB support disabled in configuration");
      return false;
    }

    // Verify TimescaleDB extension availability
    final boolean available = PostgresUtils.isTimescaleDbAvailable(database);
    if (!available) {
      LOGGER.warn("TimescaleDB support is enabled in configuration but TimescaleDB extension is not available in the database");
      return false;
    }

    LOGGER.info("TimescaleDB support is enabled and extension is available");
    return true;
  }

  /**
   * Creates TimescaleDbConfiguration from Airbyte connector configuration.
   */
  public static TimescaleDbConfiguration fromConfig(final JsonNode config) {
    if (config == null) {
      return createDefault();
    }

    return new Builder()
        .enabled(config.path("timescaledb_support").asBoolean(DEFAULT_ENABLED))
        .chunkDiscoveryInterval(Duration.ofMinutes(
            config.path("timescaledb_chunk_discovery_interval_minutes").asInt((int) DEFAULT_CHUNK_DISCOVERY_INTERVAL.toMinutes())))
        .maxConcurrentChunks(
            config.path("timescaledb_max_concurrent_chunks").asInt(DEFAULT_MAX_CONCURRENT_CHUNKS))
        .enableChunkCaching(
            config.path("timescaledb_enable_chunk_caching").asBoolean(DEFAULT_ENABLE_CHUNK_CACHING))
        .chunkCacheTtl(Duration.ofMinutes(
            config.path("timescaledb_chunk_cache_ttl_minutes").asInt((int) DEFAULT_CHUNK_CACHE_TTL.toMinutes())))
        .maxMemoryMb(
            config.path("timescaledb_max_memory_mb").asLong(DEFAULT_MAX_MEMORY_MB))
        .build();
  }

  /**
   * Creates a default configuration instance.
   */
  public static TimescaleDbConfiguration createDefault() {
    return new Builder().build();
  }

  @JsonProperty("enabled")
  public boolean isEnabled() {
    return enabled;
  }

  @JsonProperty("chunkDiscoveryInterval")
  public Duration getChunkDiscoveryInterval() {
    return chunkDiscoveryInterval;
  }

  @JsonProperty("maxConcurrentChunks")
  public int getMaxConcurrentChunks() {
    return maxConcurrentChunks;
  }

  @JsonProperty("enableChunkCaching")
  public boolean isEnableChunkCaching() {
    return enableChunkCaching;
  }

  @JsonProperty("chunkCacheTtl")
  public Duration getChunkCacheTtl() {
    return chunkCacheTtl;
  }

  @JsonProperty("maxMemoryMb")
  public long getMaxMemoryMb() {
    return maxMemoryMb;
  }

  /**
   * Gets maximum memory usage in bytes.
   */
  public long getMaxMemoryBytes() {
    return maxMemoryMb * 1024 * 1024;
  }

  /**
   * Validates the configuration and logs any warnings.
   */
  public void validate() {
    if (maxConcurrentChunks < 1) {
      LOGGER.warn("Max concurrent chunks is set to {}, minimum value is 1", maxConcurrentChunks);
    }
    if (maxConcurrentChunks > 10) {
      LOGGER.warn("Max concurrent chunks is set to {}, values above 10 may cause high database load", maxConcurrentChunks);
    }
    if (chunkDiscoveryInterval.toMinutes() < 1) {
      LOGGER.warn("Chunk discovery interval is set to {} minutes, minimum recommended value is 1 minute", 
          chunkDiscoveryInterval.toMinutes());
    }
    if (maxMemoryMb < 100) {
      LOGGER.warn("Max memory is set to {} MB, minimum recommended value is 100 MB", maxMemoryMb);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TimescaleDbConfiguration that = (TimescaleDbConfiguration) o;
    return enabled == that.enabled &&
           maxConcurrentChunks == that.maxConcurrentChunks &&
           enableChunkCaching == that.enableChunkCaching &&
           maxMemoryMb == that.maxMemoryMb &&
           Objects.equals(chunkDiscoveryInterval, that.chunkDiscoveryInterval) &&
           Objects.equals(chunkCacheTtl, that.chunkCacheTtl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(enabled, chunkDiscoveryInterval, maxConcurrentChunks, 
                       enableChunkCaching, chunkCacheTtl, maxMemoryMb);
  }

  @Override
  public String toString() {
    return String.format("TimescaleDbConfiguration{enabled=%s, chunkDiscoveryInterval=%s, " +
                        "maxConcurrentChunks=%d, enableChunkCaching=%s, chunkCacheTtl=%s, maxMemoryMb=%d}",
        enabled, chunkDiscoveryInterval, maxConcurrentChunks, enableChunkCaching, chunkCacheTtl, maxMemoryMb);
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class Builder {
    private Boolean enabled;
    private Duration chunkDiscoveryInterval;
    private Integer maxConcurrentChunks;
    private Boolean enableChunkCaching;
    private Duration chunkCacheTtl;
    private Long maxMemoryMb;

    public Builder enabled(final boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder chunkDiscoveryInterval(final Duration chunkDiscoveryInterval) {
      this.chunkDiscoveryInterval = chunkDiscoveryInterval;
      return this;
    }

    public Builder maxConcurrentChunks(final int maxConcurrentChunks) {
      this.maxConcurrentChunks = maxConcurrentChunks;
      return this;
    }

    public Builder enableChunkCaching(final boolean enableChunkCaching) {
      this.enableChunkCaching = enableChunkCaching;
      return this;
    }

    public Builder chunkCacheTtl(final Duration chunkCacheTtl) {
      this.chunkCacheTtl = chunkCacheTtl;
      return this;
    }

    public Builder maxMemoryMb(final long maxMemoryMb) {
      this.maxMemoryMb = maxMemoryMb;
      return this;
    }

    public TimescaleDbConfiguration build() {
      return new TimescaleDbConfiguration(enabled, chunkDiscoveryInterval, maxConcurrentChunks, 
                                         enableChunkCaching, chunkCacheTtl, maxMemoryMb);
    }
  }

} 