/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.timescaledb.chunking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Objects;

/**
 * Represents metadata for a TimescaleDB chunk.
 * Contains information about chunk location, size, and time ranges.
 */
public class ChunkMetadata {

  private final String chunkSchema;
  private final String chunkName;
  private final String hypertableSchema;
  private final String hypertableName;
  private final String rangeStart;
  private final String rangeEnd;
  private final long sizeBytes;
  private final String chunkId;

  @JsonCreator
  public ChunkMetadata(@JsonProperty("chunkSchema") final String chunkSchema,
                       @JsonProperty("chunkName") final String chunkName,
                       @JsonProperty("hypertableSchema") final String hypertableSchema,
                       @JsonProperty("hypertableName") final String hypertableName,
                       @JsonProperty("rangeStart") final String rangeStart,
                       @JsonProperty("rangeEnd") final String rangeEnd,
                       @JsonProperty("sizeBytes") final long sizeBytes) {
    this.chunkSchema = chunkSchema;
    this.chunkName = chunkName;
    this.hypertableSchema = hypertableSchema;
    this.hypertableName = hypertableName;
    this.rangeStart = rangeStart;
    this.rangeEnd = rangeEnd;
    this.sizeBytes = sizeBytes;
    this.chunkId = generateChunkId();
  }

  private String generateChunkId() {
    return String.format("%s.%s.%s.%s", 
        hypertableSchema, hypertableName, chunkSchema, chunkName);
  }

  @JsonProperty("chunkSchema")
  public String getChunkSchema() {
    return chunkSchema;
  }

  @JsonProperty("chunkName")
  public String getChunkName() {
    return chunkName;
  }

  @JsonProperty("hypertableSchema")
  public String getHypertableSchema() {
    return hypertableSchema;
  }

  @JsonProperty("hypertableName")
  public String getHypertableName() {
    return hypertableName;
  }

  @JsonProperty("rangeStart")
  public String getRangeStart() {
    return rangeStart;
  }

  @JsonProperty("rangeEnd")
  public String getRangeEnd() {
    return rangeEnd;
  }

  @JsonProperty("sizeBytes")
  public long getSizeBytes() {
    return sizeBytes;
  }

  public String getChunkId() {
    return chunkId;
  }

  public String getFullChunkName() {
    return String.format("%s.%s", chunkSchema, chunkName);
  }

  public String getFullHypertableName() {
    return String.format("%s.%s", hypertableSchema, hypertableName);
  }

  /**
   * Estimates memory usage for this chunk based on size and typical overhead.
   */
  public long estimateMemoryUsage() {
    // Conservative estimate: chunk size / 10 for memory overhead
    return Math.max(sizeBytes / 10, 1024 * 1024); // At least 1MB
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ChunkMetadata that = (ChunkMetadata) o;
    return Objects.equals(chunkId, that.chunkId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(chunkId);
  }

  @Override
  public String toString() {
    return String.format("ChunkMetadata{id='%s', size=%d bytes, range=%s to %s}", 
        chunkId, sizeBytes, rangeStart, rangeEnd);
  }

} 