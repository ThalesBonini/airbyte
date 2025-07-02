package io.airbyte.integrations.source.postgres.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.integrations.debezium.internals.ChangeEventWithMetadata;
import io.airbyte.cdk.integrations.debezium.internals.DebeziumEventConverter;
import io.airbyte.cdk.integrations.debezium.internals.RelationalDbDebeziumEventConverter;
import io.airbyte.integrations.source.postgres.TimescaleDbUtils;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TimescaleDB-aware event converter that handles SMT message routing and state management.
 * 
 * This converter wraps the standard RelationalDbDebeziumEventConverter and adds 
 * TimescaleDB-specific processing for messages that have been routed by the TimescaleDB SMT.
 */
public class TimescaleDbEventConverter implements DebeziumEventConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDbEventConverter.class);
    
    private final RelationalDbDebeziumEventConverter delegate;
    private final TimescaleDbStateManager timescaleDbStateManager;
    private final boolean isTimescaleDbEnabled;
    
    // Track the last record message for state correlation
    private AirbyteMessage lastRecordMessage;

    public TimescaleDbEventConverter(final RelationalDbDebeziumEventConverter delegate,
                                   final JsonNode sourceConfig) {
        this.delegate = delegate;
        this.isTimescaleDbEnabled = TimescaleDbUtils.isTimescaleDbEnabled(sourceConfig);
        this.timescaleDbStateManager = isTimescaleDbEnabled ? new TimescaleDbStateManager(sourceConfig) : null;
        
        if (isTimescaleDbEnabled) {
            LOGGER.info("TimescaleDbEventConverter initialized - SMT-aware message processing enabled");
        }
    }

    @Override
    public AirbyteMessage toAirbyteMessage(final ChangeEventWithMetadata event) {
        // First, let the delegate converter process the event normally
        final AirbyteMessage message = delegate.toAirbyteMessage(event);
        
        if (!isTimescaleDbEnabled) {
            return message;
        }
        
        try {
            // Handle different message types
            switch (message.getType()) {
                case RECORD:
                    return handleRecordMessage(message);
                case STATE:
                    return handleStateMessage(message);
                default:
                    return message;
            }
        } catch (final Exception e) {
            LOGGER.error("Error processing TimescaleDB message, falling back to original: {}", e.getMessage(), e);
            return message;
        }
    }
    
    /**
     * Handle record messages, potentially from TimescaleDB SMT routing.
     */
    private AirbyteMessage handleRecordMessage(final AirbyteMessage recordMessage) {
        // Store the last record message for state correlation
        lastRecordMessage = recordMessage;
        
        // Check if this is a TimescaleDB SMT message
        if (isTimescaleDbSmtMessage(recordMessage)) {
            LOGGER.debug("Processing TimescaleDB SMT message for stream: {}", 
                recordMessage.getRecord().getStream());
            
            // For now, just log SMT message detection
            // The actual state aggregation will happen when state messages are processed
            logTimescaleDbMessageInfo(recordMessage);
        }
        
        return recordMessage;
    }
    
    /**
     * Handle state messages, applying TimescaleDB SMT routing if needed.
     */
    private AirbyteMessage handleStateMessage(final AirbyteMessage stateMessage) {
        if (timescaleDbStateManager == null || lastRecordMessage == null) {
            return stateMessage;
        }
        
        // Process the state message through TimescaleDB state manager
        final Optional<AirbyteMessage> processedState = timescaleDbStateManager
            .processTimescaleDbState(stateMessage.getState(), lastRecordMessage)
            .map(processedStateMessage -> {
                final AirbyteMessage newMessage = new AirbyteMessage();
                newMessage.setType(AirbyteMessage.Type.STATE);
                newMessage.setState(processedStateMessage);
                return newMessage;
            });
        
        return processedState.orElse(stateMessage);
    }
    
    /**
     * Check if a message has TimescaleDB SMT headers.
     */
    private boolean isTimescaleDbSmtMessage(final AirbyteMessage message) {
        if (message.getType() != AirbyteMessage.Type.RECORD || message.getRecord() == null) {
            return false;
        }
        
        final JsonNode recordData = message.getRecord().getData();
        if (recordData == null) {
            return false;
        }
        
        // Check for TimescaleDB SMT headers
        return recordData.has("__headers") && 
               recordData.get("__headers").has("debezium_timescaledb_chunk_table");
    }
    
    /**
     * Log TimescaleDB message information for debugging.
     */
    private void logTimescaleDbMessageInfo(final AirbyteMessage message) {
        try {
            final JsonNode recordData = message.getRecord().getData();
            if (recordData.has("__headers")) {
                final JsonNode headers = recordData.get("__headers");
                final String chunkTable = headers.has("debezium_timescaledb_chunk_table") 
                    ? headers.get("debezium_timescaledb_chunk_table").asText() : "unknown";
                final String hypertable = headers.has("debezium_timescaledb_hypertable")
                    ? headers.get("debezium_timescaledb_hypertable").asText() : "unknown";
                
                LOGGER.debug("TimescaleDB SMT routing: {} → {} (stream: {})", 
                    chunkTable, hypertable, message.getRecord().getStream());
            }
        } catch (final Exception e) {
            LOGGER.debug("Could not extract TimescaleDB header info: {}", e.getMessage());
        }
    }
    
    /**
     * Get the TimescaleDB state manager for debugging or state validation.
     */
    public Optional<TimescaleDbStateManager> getTimescaleDbStateManager() {
        return Optional.ofNullable(timescaleDbStateManager);
    }
    
    /**
     * Get state summary for debugging.
     */
    public String getStateSummary() {
        if (timescaleDbStateManager == null) {
            return "TimescaleDB state management disabled";
        }
        return timescaleDbStateManager.getStateSummary();
    }
} 