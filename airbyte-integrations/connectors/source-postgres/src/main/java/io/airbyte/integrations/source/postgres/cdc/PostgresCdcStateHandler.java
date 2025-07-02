/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.integrations.debezium.CdcStateHandler;
import io.airbyte.cdk.integrations.debezium.internals.AirbyteSchemaHistoryStorage.SchemaHistory;
import io.airbyte.cdk.integrations.source.relationaldb.models.CdcState;
import io.airbyte.cdk.integrations.source.relationaldb.state.StateManager;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteMessage.Type;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresCdcStateHandler implements CdcStateHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCdcStateHandler.class);
  private final StateManager stateManager;
  private final TimescaleDbStateManager timescaleDbStateManager;

  public PostgresCdcStateHandler(final StateManager stateManager) {
    this.stateManager = stateManager;
    this.timescaleDbStateManager = null; // Will be null for non-TimescaleDB configurations
  }

  public PostgresCdcStateHandler(final StateManager stateManager, final TimescaleDbStateManager timescaleDbStateManager) {
    this.stateManager = stateManager;
    this.timescaleDbStateManager = timescaleDbStateManager;
    
    if (timescaleDbStateManager != null) {
      LOGGER.info("PostgresCdcStateHandler initialized with TimescaleDB state management");
    }
  }

  @Override
  public boolean isCdcCheckpointEnabled() {
    return true;
  }

  @Override
  public AirbyteMessage saveState(final Map<String, String> offset, final SchemaHistory<String> ignored) {
    final JsonNode asJson = Jsons.jsonNode(offset);
    LOGGER.info("debezium state: {}", asJson);
    
    final CdcState cdcState = new CdcState().withState(asJson);
    stateManager.getCdcStateManager().setCdcState(cdcState);
    
    /*
     * Namespace pair is ignored by global state manager, but is needed for satisfy the API contract.
     * Therefore, provide an empty optional.
     */
    final AirbyteStateMessage originalStateMessage = stateManager.emit(Optional.empty());
    
    // If TimescaleDB state manager is enabled, process the state message for SMT routing
    if (timescaleDbStateManager != null) {
      LOGGER.debug("Processing state message through TimescaleDbStateManager");
      
      // We need the current record message to extract SMT headers, but we don't have it here
      // For now, just validate state consistency and return the original state
      if (!timescaleDbStateManager.validateStateConsistency()) {
        LOGGER.warn("TimescaleDB state inconsistency detected during state save");
        LOGGER.debug("TimescaleDB state summary: {}", timescaleDbStateManager.getStateSummary());
      }
      
      // TODO: In a future enhancement, we could aggregate chunk-level state here
      // For now, we ensure the original state is preserved
      return new AirbyteMessage().withType(Type.STATE).withState(originalStateMessage);
    }
    
    return new AirbyteMessage().withType(Type.STATE).withState(originalStateMessage);
  }

  /**
   * Here we just want to emit the state to update the list of streams in the database to mark the
   * completion of snapshot of new added streams. The addition of new streams in the state is done
   * here
   * {@link io.airbyte.cdk.integrations.source.relationaldb.state.GlobalStateManager#toState(Optional)}
   * which is called inside the {@link StateManager#emit(Optional)} method which is being triggered
   * below. The toState method adds all the streams present in the catalog in the state. Since there
   * is no change in the CDC state value, whatever was present in the database will again be stored.
   * This is done so that we can mark the completion of snapshot of new tables.
   */
  @Override
  public AirbyteMessage saveStateAfterCompletionOfSnapshotOfNewStreams() {
    LOGGER.info("Snapshot of new tables is complete, saving state");
    
    /*
     * Namespace pair is ignored by global state manager, but is needed for satisfy the API contract.
     * Therefore, provide an empty optional.
     */
    final AirbyteStateMessage stateMessage = stateManager.emit(Optional.empty());
    
    // Reset TimescaleDB state when snapshot is complete
    if (timescaleDbStateManager != null) {
      LOGGER.info("Resetting TimescaleDB state after snapshot completion");
      timescaleDbStateManager.resetAllState();
    }
    
    return new AirbyteMessage().withType(Type.STATE).withState(stateMessage);
  }

  /**
   * Get the TimescaleDB state manager if available.
   */
  public Optional<TimescaleDbStateManager> getTimescaleDbStateManager() {
    return Optional.ofNullable(timescaleDbStateManager);
  }

}
