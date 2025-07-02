package io.airbyte.integrations.source.postgres.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.source.postgres.TimescaleDbUtils;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles state management when TimescaleDB SMT routes messages from chunk tables to logical hypertable topics.
 * 
 * The TimescaleDB SMT transforms messages like this:
 * Source: _timescaledb_internal._hyper_1_1_chunk → Destination: public.metrics
 * 
 * This breaks Airbyte's standard 1:1 source-to-destination state tracking assumption.
 * This class aggregates chunk-level LSN positions into logical hypertable state.
 */
public class TimescaleDbStateManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDbStateManager.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    // Headers added by TimescaleDB SMT
    private static final String TIMESCALEDB_CHUNK_TABLE_HEADER = "debezium_timescaledb_chunk_table";
    private static final String TIMESCALEDB_CHUNK_SCHEMA_HEADER = "debezium_timescaledb_chunk_schema";
    private static final String TIMESCALEDB_HYPERTABLE_HEADER = "debezium_timescaledb_hypertable";
    
    private final boolean isTimescaleDbEnabled;
    
    // Maps logical hypertable names to their aggregated state
    private final Map<String, TimescaleDbHypertableState> hypertableStates;
    
    public TimescaleDbStateManager(final JsonNode sourceConfig) {
        this.isTimescaleDbEnabled = TimescaleDbUtils.isTimescaleDbEnabled(sourceConfig);
        this.hypertableStates = new HashMap<>();
        
        if (isTimescaleDbEnabled) {
            LOGGER.info("TimescaleDbStateManager initialized - SMT-aware state management enabled");
        }
    }
    
    /**
     * Process a state message, handling TimescaleDB SMT routing if enabled.
     * 
     * @param stateMessage The original state message from Debezium
     * @param message The corresponding record message containing SMT headers
     * @return The processed state message with proper logical hypertable state
     */
    public Optional<AirbyteStateMessage> processTimescaleDbState(final AirbyteStateMessage stateMessage, 
                                                               final AirbyteMessage message) {
        if (!isTimescaleDbEnabled) {
            return Optional.of(stateMessage);
        }
        
        try {
            // Extract TimescaleDB headers from the message
            final TimescaleDbMessageInfo messageInfo = extractTimescaleDbInfo(message);
            
            if (messageInfo == null) {
                // Not a TimescaleDB SMT message, pass through unchanged
                return Optional.of(stateMessage);
            }
            
            LOGGER.debug("Processing TimescaleDB state for chunk {} → hypertable {}", 
                messageInfo.getChunkTableName(), messageInfo.getHypertableName());
            
            // Update our internal chunk-to-hypertable state mapping
            updateHypertableState(messageInfo, stateMessage);
            
            // Create aggregated state for the logical hypertable
            final AirbyteStateMessage aggregatedState = createAggregatedState(messageInfo.getHypertableName());
            
            return Optional.of(aggregatedState);
            
        } catch (final Exception e) {
            LOGGER.error("Error processing TimescaleDB state, falling back to original state: {}", e.getMessage(), e);
            return Optional.of(stateMessage);
        }
    }
    
    /**
     * Extract TimescaleDB SMT header information from a message.
     */
    private TimescaleDbMessageInfo extractTimescaleDbInfo(final AirbyteMessage message) {
        if (message.getType() != AirbyteMessage.Type.RECORD || message.getRecord() == null) {
            return null;
        }
        
        final JsonNode recordData = message.getRecord().getData();
        if (recordData == null) {
            return null;
        }
        
        // Check for TimescaleDB SMT headers
        final String chunkTable = extractHeader(recordData, TIMESCALEDB_CHUNK_TABLE_HEADER);
        final String chunkSchema = extractHeader(recordData, TIMESCALEDB_CHUNK_SCHEMA_HEADER);
        final String hypertableName = extractHeader(recordData, TIMESCALEDB_HYPERTABLE_HEADER);
        
        if (chunkTable == null || hypertableName == null) {
            return null; // Not a TimescaleDB SMT message
        }
        
        return new TimescaleDbMessageInfo(chunkTable, chunkSchema, hypertableName);
    }
    
    /**
     * Extract header value from message data.
     */
    private String extractHeader(final JsonNode recordData, final String headerName) {
        if (recordData.has("__headers") && recordData.get("__headers").has(headerName)) {
            return recordData.get("__headers").get(headerName).asText();
        }
        return null;
    }
    
    /**
     * Update internal state tracking for a hypertable based on chunk message.
     */
    private void updateHypertableState(final TimescaleDbMessageInfo messageInfo, 
                                     final AirbyteStateMessage chunkState) {
        final String hypertableName = messageInfo.getHypertableName();
        
        // Get or create hypertable state
        final TimescaleDbHypertableState hypertableState = hypertableStates.computeIfAbsent(
            hypertableName, 
            name -> new TimescaleDbHypertableState(name)
        );
        
        // Update chunk state within the hypertable
        hypertableState.updateChunkState(messageInfo.getChunkTableName(), chunkState);
        
        LOGGER.debug("Updated hypertable {} state with chunk {} LSN", 
            hypertableName, messageInfo.getChunkTableName());
    }
    
    /**
     * Create aggregated state message for a logical hypertable.
     */
    private AirbyteStateMessage createAggregatedState(final String hypertableName) {
        final TimescaleDbHypertableState hypertableState = hypertableStates.get(hypertableName);
        
        if (hypertableState == null) {
            LOGGER.warn("No state found for hypertable: {}", hypertableName);
            return null;
        }
        
        // Get the aggregated state from all chunks
        final AirbyteStateMessage aggregatedState = hypertableState.getAggregatedState();
        
        LOGGER.debug("Created aggregated state for hypertable {} from {} chunks", 
            hypertableName, hypertableState.getChunkCount());
        
        return aggregatedState;
    }
    
    /**
     * Validate that state is consistent across chunk routing.
     */
    public boolean validateStateConsistency() {
        if (!isTimescaleDbEnabled) {
            return true;
        }
        
        boolean allConsistent = true;
        
        for (final Map.Entry<String, TimescaleDbHypertableState> entry : hypertableStates.entrySet()) {
            final String hypertableName = entry.getKey();
            final TimescaleDbHypertableState state = entry.getValue();
            
            if (!state.isConsistent()) {
                LOGGER.warn("Inconsistent state detected for hypertable: {}", hypertableName);
                allConsistent = false;
            }
        }
        
        return allConsistent;
    }
    
    /**
     * Reset state for a specific hypertable (used during connection reset).
     */
    public void resetHypertableState(final String hypertableName) {
        hypertableStates.remove(hypertableName);
        LOGGER.info("Reset state for hypertable: {}", hypertableName);
    }
    
    /**
     * Reset all TimescaleDB state (used during full connection reset).
     */
    public void resetAllState() {
        hypertableStates.clear();
        LOGGER.info("Reset all TimescaleDB state");
    }
    
    /**
     * Get current state summary for debugging.
     */
    public String getStateSummary() {
        if (!isTimescaleDbEnabled) {
            return "TimescaleDB state management disabled";
        }
        
        final StringBuilder summary = new StringBuilder();
        summary.append("TimescaleDB State Summary:\n");
        
        for (final Map.Entry<String, TimescaleDbHypertableState> entry : hypertableStates.entrySet()) {
            final String hypertableName = entry.getKey();
            final TimescaleDbHypertableState state = entry.getValue();
            
            summary.append(String.format("  Hypertable: %s, Chunks: %d, Consistent: %s\n",
                hypertableName, state.getChunkCount(), state.isConsistent()));
        }
        
        return summary.toString();
    }
    
    /**
     * Information extracted from TimescaleDB SMT headers.
     */
    private static class TimescaleDbMessageInfo {
        private final String chunkTableName;
        private final String chunkSchema;
        private final String hypertableName;
        
        public TimescaleDbMessageInfo(final String chunkTableName, final String chunkSchema, final String hypertableName) {
            this.chunkTableName = chunkTableName;
            this.chunkSchema = chunkSchema;
            this.hypertableName = hypertableName;
        }
        
        public String getChunkTableName() { return chunkTableName; }
        public String getChunkSchema() { return chunkSchema; }
        public String getHypertableName() { return hypertableName; }
    }
    
    /**
     * Tracks state for a logical hypertable aggregated from multiple chunk tables.
     */
    private static class TimescaleDbHypertableState {
        private final String hypertableName;
        private final Map<String, AirbyteStateMessage> chunkStates;
        private long maxLsn;
        private String maxLsnSource;
        
        public TimescaleDbHypertableState(final String hypertableName) {
            this.hypertableName = hypertableName;
            this.chunkStates = new HashMap<>();
            this.maxLsn = 0L;
            this.maxLsnSource = null;
        }
        
        /**
         * Update state for a specific chunk within this hypertable.
         */
        public void updateChunkState(final String chunkName, final AirbyteStateMessage chunkState) {
            chunkStates.put(chunkName, chunkState);
            
            // Extract LSN from chunk state and track maximum
            final Long chunkLsn = extractLsnFromState(chunkState);
            if (chunkLsn != null && chunkLsn > maxLsn) {
                maxLsn = chunkLsn;
                maxLsnSource = chunkName;
                
                LOGGER.debug("Updated max LSN for hypertable {} to {} from chunk {}", 
                    hypertableName, maxLsn, chunkName);
            }
        }
        
        /**
         * Get aggregated state representing all chunks in this hypertable.
         */
        public AirbyteStateMessage getAggregatedState() {
            if (maxLsnSource == null) {
                return null;
            }
            
            // Use the state from the chunk with the highest LSN as the base
            final AirbyteStateMessage baseState = chunkStates.get(maxLsnSource);
            if (baseState == null) {
                return null;
            }
            
            // Create a copy of the state but modify it to represent the logical hypertable
            final AirbyteStateMessage aggregatedState = Jsons.clone(baseState);
            
            // Update any stream names to reflect the logical hypertable instead of chunk
            if (aggregatedState.getStream() != null && aggregatedState.getStream().getStreamDescriptor() != null) {
                // Replace chunk table name with hypertable name in stream descriptor
                final String streamName = aggregatedState.getStream().getStreamDescriptor().getName();
                if (streamName != null && streamName.contains("_hyper_")) {
                    // This is a chunk table name, replace with hypertable name
                    aggregatedState.getStream().getStreamDescriptor().setName(hypertableName);
                }
            }
            
            return aggregatedState;
        }
        
        /**
         * Extract LSN value from state message.
         */
        private Long extractLsnFromState(final AirbyteStateMessage state) {
            try {
                if (state.getStream() != null && state.getStream().getStreamState() != null) {
                    final JsonNode streamState = state.getStream().getStreamState();
                    
                    // Look for LSN in various possible locations
                    if (streamState.has("lsn")) {
                        return streamState.get("lsn").asLong();
                    }
                    
                    if (streamState.has("cursor") && streamState.get("cursor").has("lsn")) {
                        return streamState.get("cursor").get("lsn").asLong();
                    }
                }
                
                return null;
            } catch (final Exception e) {
                LOGGER.debug("Could not extract LSN from state: {}", e.getMessage());
                return null;
            }
        }
        
        /**
         * Check if the state is consistent across all chunks.
         */
        public boolean isConsistent() {
            // For now, consider state consistent if we have at least one chunk with valid LSN
            return maxLsn > 0 && maxLsnSource != null;
        }
        
        public int getChunkCount() {
            return chunkStates.size();
        }
    }
} 