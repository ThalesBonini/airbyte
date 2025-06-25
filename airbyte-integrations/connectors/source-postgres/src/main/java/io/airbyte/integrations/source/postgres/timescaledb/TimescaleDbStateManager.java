/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.timescaledb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.integrations.source.postgres.timescaledb.chunking.ChunkMetadata;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages state for TimescaleDB chunk processing.
 * Provides chunk-aware state tracking and recovery capabilities.
 */
public class TimescaleDbStateManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDbStateManager.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String TIMESCALEDB_STATE_KEY = "timescaledb_state";
  private static final String PROCESSED_CHUNKS_KEY = "processed_chunks";
  private static final String CURRENT_CHUNK_KEY = "current_chunk";
  private static final String LAST_PROCESSED_TIME_KEY = "last_processed_time";
  private static final String CHUNK_PROGRESS_KEY = "chunk_progress";

  private final Map<String, ChunkState> streamStates = new HashMap<>();

  /**
   * Updates the state with chunk processing progress.
   */
  public void updateChunkProgress(final String streamName, 
                                 final ChunkMetadata chunk,
                                 final long recordsProcessed,
                                 final boolean isCompleted) {
    
    final ChunkState state = streamStates.computeIfAbsent(streamName, k -> new ChunkState());
    
    state.currentChunk = chunk.getChunkId();
    state.recordsProcessed = recordsProcessed;
    state.lastProcessedTime = Instant.now();
    
    if (isCompleted) {
      state.completedChunks.put(chunk.getChunkId(), recordsProcessed);
      state.currentChunk = null;
      state.recordsProcessed = 0;
    }
    
    LOGGER.debug("Updated state for stream {}: chunk {}, records {}, completed {}", 
        streamName, chunk.getChunkId(), recordsProcessed, isCompleted);
  }

  /**
   * Gets the current state for a stream.
   */
  public Optional<ChunkState> getStreamState(final String streamName) {
    return Optional.ofNullable(streamStates.get(streamName));
  }

  /**
   * Checks if a chunk has been completed.
   */
  public boolean isChunkCompleted(final String streamName, final String chunkId) {
    return streamStates.containsKey(streamName) && 
           streamStates.get(streamName).completedChunks.containsKey(chunkId);
  }

  /**
   * Serializes the state to Airbyte state format.
   */
  public JsonNode serializeState() {
    final ObjectNode stateNode = OBJECT_MAPPER.createObjectNode();
    final ObjectNode timescaleDbState = OBJECT_MAPPER.createObjectNode();
    
    for (final Map.Entry<String, ChunkState> entry : streamStates.entrySet()) {
      final String streamName = entry.getKey();
      final ChunkState state = entry.getValue();
      
      final ObjectNode streamState = OBJECT_MAPPER.createObjectNode();
      streamState.put(CURRENT_CHUNK_KEY, state.currentChunk);
      streamState.put(CHUNK_PROGRESS_KEY, state.recordsProcessed);
      streamState.put(LAST_PROCESSED_TIME_KEY, 
          state.lastProcessedTime != null ? state.lastProcessedTime.toString() : null);
      
      // Add completed chunks
      final ObjectNode completedChunks = OBJECT_MAPPER.createObjectNode();
      state.completedChunks.forEach(completedChunks::put);
      streamState.set(PROCESSED_CHUNKS_KEY, completedChunks);
      
      timescaleDbState.set(streamName, streamState);
    }
    
    stateNode.set(TIMESCALEDB_STATE_KEY, timescaleDbState);
    
    LOGGER.debug("Serialized TimescaleDB state for {} streams", streamStates.size());
    return stateNode;
  }

  /**
   * Deserializes state from Airbyte state format.
   */
  public void deserializeState(final JsonNode stateNode) {
    if (stateNode == null || !stateNode.has(TIMESCALEDB_STATE_KEY)) {
      LOGGER.info("No TimescaleDB state found in provided state");
      return;
    }
    
    final JsonNode timescaleDbState = stateNode.get(TIMESCALEDB_STATE_KEY);
    
    timescaleDbState.fieldNames().forEachRemaining(streamName -> {
      final JsonNode streamState = timescaleDbState.get(streamName);
      
      final ChunkState state = new ChunkState();
      
      // Restore current chunk and progress
      if (streamState.has(CURRENT_CHUNK_KEY) && !streamState.get(CURRENT_CHUNK_KEY).isNull()) {
        state.currentChunk = streamState.get(CURRENT_CHUNK_KEY).asText();
      }
      
      if (streamState.has(CHUNK_PROGRESS_KEY)) {
        state.recordsProcessed = streamState.get(CHUNK_PROGRESS_KEY).asLong();
      }
      
      if (streamState.has(LAST_PROCESSED_TIME_KEY) && !streamState.get(LAST_PROCESSED_TIME_KEY).isNull()) {
        try {
          state.lastProcessedTime = Instant.parse(streamState.get(LAST_PROCESSED_TIME_KEY).asText());
        } catch (final Exception e) {
          LOGGER.warn("Failed to parse last processed time for stream {}", streamName);
        }
      }
      
      // Restore completed chunks
      if (streamState.has(PROCESSED_CHUNKS_KEY)) {
        final JsonNode completedChunks = streamState.get(PROCESSED_CHUNKS_KEY);
        completedChunks.fieldNames().forEachRemaining(chunkId -> {
          state.completedChunks.put(chunkId, completedChunks.get(chunkId).asLong());
        });
      }
      
      streamStates.put(streamName, state);
    });
    
    LOGGER.info("Deserialized TimescaleDB state for {} streams", streamStates.size());
  }

  /**
   * Clears state for a specific stream.
   */
  public void clearStreamState(final String streamName) {
    streamStates.remove(streamName);
    LOGGER.info("Cleared state for stream: {}", streamName);
  }

  /**
   * Gets statistics about the current state.
   */
  public Map<String, Object> getStateStats() {
    final Map<String, Object> stats = new HashMap<>();
    
    int totalStreams = streamStates.size();
    int totalCompletedChunks = streamStates.values().stream()
        .mapToInt(state -> state.completedChunks.size())
        .sum();
    int streamsInProgress = (int) streamStates.values().stream()
        .filter(state -> state.currentChunk != null)
        .count();
    
    stats.put("total_streams", totalStreams);
    stats.put("total_completed_chunks", totalCompletedChunks);
    stats.put("streams_in_progress", streamsInProgress);
    
    return stats;
  }

  /**
   * Represents the processing state for a single stream.
   */
  public static class ChunkState {
    private String currentChunk;
    private long recordsProcessed;
    private Instant lastProcessedTime;
    private final Map<String, Long> completedChunks = new HashMap<>();

    public String getCurrentChunk() {
      return currentChunk;
    }

    public long getRecordsProcessed() {
      return recordsProcessed;
    }

    public Instant getLastProcessedTime() {
      return lastProcessedTime;
    }

    public Map<String, Long> getCompletedChunks() {
      return new HashMap<>(completedChunks);
    }

    public boolean hasCompletedChunk(final String chunkId) {
      return completedChunks.containsKey(chunkId);
    }

    public long getTotalRecordsProcessed() {
      return completedChunks.values().stream().mapToLong(Long::longValue).sum() + recordsProcessed;
    }
  }

} 