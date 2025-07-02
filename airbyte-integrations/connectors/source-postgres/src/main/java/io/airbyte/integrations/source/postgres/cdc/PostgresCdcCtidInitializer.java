/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.cdc;

import static io.airbyte.cdk.db.DbAnalyticsUtils.cdcCursorInvalidMessage;
import static io.airbyte.cdk.db.DbAnalyticsUtils.cdcResyncMessage;
import static io.airbyte.cdk.db.DbAnalyticsUtils.wassOccurrenceMessage;
import static io.airbyte.integrations.source.postgres.PostgresQueryUtils.streamsUnderVacuum;
import static io.airbyte.integrations.source.postgres.PostgresSpecConstants.FAIL_SYNC_OPTION;
import static io.airbyte.integrations.source.postgres.PostgresSpecConstants.INVALID_CDC_CURSOR_POSITION_PROPERTY;
import static io.airbyte.integrations.source.postgres.PostgresSpecConstants.RESYNC_DATA_OPTION;
import static io.airbyte.integrations.source.postgres.PostgresUtils.isDebugMode;
import static io.airbyte.integrations.source.postgres.PostgresUtils.prettyPrintConfiguredAirbyteStreamList;
import static io.airbyte.integrations.source.postgres.ctid.CtidUtils.createInitialLoader;
import static io.airbyte.integrations.source.postgres.PostgresUtils.isCdc;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.cdk.db.jdbc.JdbcUtils;
import io.airbyte.cdk.integrations.base.AirbyteTraceMessageUtility;
import io.airbyte.cdk.integrations.debezium.AirbyteDebeziumHandler;
import io.airbyte.cdk.integrations.debezium.internals.DebeziumEventConverter;
import io.airbyte.cdk.integrations.debezium.internals.RelationalDbDebeziumEventConverter;
import io.airbyte.cdk.integrations.debezium.internals.RelationalDbDebeziumPropertiesManager;
import io.airbyte.cdk.integrations.debezium.internals.DebeziumPropertiesManager;
import java.sql.Array;
import java.sql.SQLException;
import java.util.stream.Collectors;
import io.airbyte.cdk.integrations.source.relationaldb.InitialLoadTimeoutUtil;
import io.airbyte.cdk.integrations.source.relationaldb.TableInfo;
import io.airbyte.cdk.integrations.source.relationaldb.models.CdcState;
import io.airbyte.cdk.integrations.source.relationaldb.state.StateManager;
import io.airbyte.cdk.integrations.source.relationaldb.streamstatus.StreamStatusTraceEmitterIterator;
import io.airbyte.commons.exceptions.ConfigErrorException;
import io.airbyte.commons.exceptions.TransientErrorException;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.stream.AirbyteStreamStatusHolder;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.source.postgres.PostgresQueryUtils;
import io.airbyte.integrations.source.postgres.PostgresType;
import io.airbyte.integrations.source.postgres.PostgresUtils;
import io.airbyte.integrations.source.postgres.TimescaleDbUtils;
import io.airbyte.integrations.source.postgres.cdc.PostgresCdcCtidUtils.CtidStreams;
import io.airbyte.integrations.source.postgres.ctid.CtidGlobalStateManager;
import io.airbyte.integrations.source.postgres.ctid.FileNodeHandler;
import io.airbyte.integrations.source.postgres.ctid.PostgresCtidHandler;
import io.airbyte.protocol.models.CommonField;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteStream;
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.v0.SyncMode;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.airbyte.integrations.source.postgres.cdc.PostgresCdcConnectorMetadataInjector;
import java.util.Properties;
import io.airbyte.cdk.integrations.debezium.internals.AirbyteFileOffsetBackingStore;
import io.airbyte.cdk.integrations.debezium.internals.AirbyteSchemaHistoryStorage;

public class PostgresCdcCtidInitializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCdcCtidInitializer.class);

  public static boolean getSavedOffsetAfterReplicationSlotLSN(final JdbcDatabase database,
                                                              final ConfiguredAirbyteCatalog catalog,
                                                              final StateManager stateManager,
                                                              final JsonNode replicationSlot) {
    final PostgresDebeziumStateUtil postgresDebeziumStateUtil = new PostgresDebeziumStateUtil();

    final CdcState defaultCdcState = getDefaultCdcState(postgresDebeziumStateUtil, database);

    final JsonNode state =
        (stateManager.getCdcStateManager().getCdcState() == null || stateManager.getCdcStateManager().getCdcState().getState() == null)
            ? defaultCdcState.getState()
            : Jsons.clone(stateManager.getCdcStateManager().getCdcState().getState());

    final OptionalLong savedOffset = postgresDebeziumStateUtil.savedOffset(
        Jsons.clone(PostgresCdcProperties.getDebeziumDefaultProperties(database)),
        catalog,
        state,
        database.getSourceConfig());
    return postgresDebeziumStateUtil.isSavedOffsetAfterReplicationSlotLSN(
        // We can assume that there will be only 1 replication slot cause before the sync starts for
        // Postgres CDC,
        // we run all the check operations and one of the check validates that the replication slot exists
        // and has only 1 entry
        replicationSlot,
        savedOffset,
        database.getSourceConfig());
  }

  public static CtidGlobalStateManager getCtidInitialLoadGlobalStateManager(final JdbcDatabase database,
                                                                            final ConfiguredAirbyteCatalog catalog,
                                                                            final StateManager stateManager,
                                                                            final String quoteString,
                                                                            final boolean savedOffsetAfterReplicationSlotLSN) {
    final PostgresDebeziumStateUtil postgresDebeziumStateUtil = new PostgresDebeziumStateUtil();

    final CtidStreams ctidStreams = PostgresCdcCtidUtils.streamsToSyncViaCtid(stateManager.getCdcStateManager(), catalog,
        savedOffsetAfterReplicationSlotLSN);
    final List<AirbyteStreamNameNamespacePair> streamsUnderVacuum = new ArrayList<>();
    streamsUnderVacuum.addAll(streamsUnderVacuum(database,
        ctidStreams.streamsForCtidSync(), quoteString).result());

    if (!streamsUnderVacuum.isEmpty()) {
      throw new TransientErrorException(
          "Postgres database is undergoing a full vacuum - cannot proceed with the sync. Please sync again when the vacuum is finished.");
    }

    final List<ConfiguredAirbyteStream> finalListOfStreamsToBeSyncedViaCtid = ctidStreams.streamsForCtidSync();

    LOGGER.info("Streams to be synced via ctid (can include RFR streams) : {}", finalListOfStreamsToBeSyncedViaCtid.size());
    LOGGER.info("Streams: {}", prettyPrintConfiguredAirbyteStreamList(finalListOfStreamsToBeSyncedViaCtid));
    final FileNodeHandler fileNodeHandler = PostgresQueryUtils.fileNodeForStreams(database,
        finalListOfStreamsToBeSyncedViaCtid,
        quoteString);
    final CdcState defaultCdcState = getDefaultCdcState(postgresDebeziumStateUtil, database);

    final CtidGlobalStateManager ctidStateManager =
        new CtidGlobalStateManager(ctidStreams, fileNodeHandler, stateManager, catalog, savedOffsetAfterReplicationSlotLSN, defaultCdcState);
    return ctidStateManager;

  }

  private static CdcState getDefaultCdcState(final PostgresDebeziumStateUtil postgresDebeziumStateUtil, final JdbcDatabase database) {
    var sourceConfig = database.getSourceConfig();
    final JsonNode initialDebeziumState = postgresDebeziumStateUtil.constructInitialDebeziumState(database,
        sourceConfig.get(JdbcUtils.DATABASE_KEY).asText());
    return new CdcState().withState(initialDebeziumState);
  }

  public static List<AutoCloseableIterator<AirbyteMessage>> cdcCtidIteratorsCombined(final JdbcDatabase database,
                                                                                     final ConfiguredAirbyteCatalog catalog,
                                                                                     final Map<String, TableInfo<CommonField<PostgresType>>> tableNameToTable,
                                                                                     final StateManager stateManager,
                                                                                     final Instant emittedAt,
                                                                                     final String quoteString,
                                                                                     final CtidGlobalStateManager ctidStateManager,
                                                                                     final boolean savedOffsetAfterReplicationSlotLSN) {
    final JsonNode sourceConfig = database.getSourceConfig();
    final Duration firstRecordWaitTime = PostgresUtils.getFirstRecordWaitTime(sourceConfig);
    final Duration initialLoadTimeout = InitialLoadTimeoutUtil.getInitialLoadTimeout(sourceConfig);
    final int queueSize = PostgresUtils.getQueueSize(sourceConfig);
    LOGGER.info("First record waiting time: {} seconds", firstRecordWaitTime.getSeconds());
    LOGGER.info("Initial load timeout: {} hours", initialLoadTimeout.toHours());
    LOGGER.info("Queue size: {}", queueSize);

    if (isDebugMode(sourceConfig) && !PostgresUtils.shouldFlushAfterSync(sourceConfig)) {
      throw new ConfigErrorException("WARNING: The config indicates that we are clearing the WAL while reading data. This will mutate the WAL" +
          " associated with the source being debugged and is not advised.");
    }

    final PostgresDebeziumStateUtil postgresDebeziumStateUtil = new PostgresDebeziumStateUtil();

    final JsonNode initialDebeziumState = postgresDebeziumStateUtil.constructInitialDebeziumState(database,
        sourceConfig.get(JdbcUtils.DATABASE_KEY).asText());

    final JsonNode state =
        (stateManager.getCdcStateManager().getCdcState() == null || stateManager.getCdcStateManager().getCdcState().getState() == null)
            ? initialDebeziumState
            : Jsons.clone(stateManager.getCdcStateManager().getCdcState().getState());

    final OptionalLong savedOffset = postgresDebeziumStateUtil.savedOffset(
        Jsons.clone(PostgresCdcProperties.getDebeziumDefaultProperties(database)),
        catalog,
        state,
        sourceConfig);

    // We should always be able to extract offset out of state if it's not null
    if (state != null && savedOffset.isEmpty()) {
      throw new RuntimeException(
          "Unable extract the offset out of state, State mutation might not be working. " + state.asText());
    }

    if (!savedOffsetAfterReplicationSlotLSN) {
      AirbyteTraceMessageUtility.emitAnalyticsTrace(cdcCursorInvalidMessage());
      if (!sourceConfig.get("replication_method").has(INVALID_CDC_CURSOR_POSITION_PROPERTY) || sourceConfig.get("replication_method").get(
          INVALID_CDC_CURSOR_POSITION_PROPERTY).asText().equals(FAIL_SYNC_OPTION)) {
        throw new ConfigErrorException(
            "Saved offset is before replication slot's confirmed lsn. Please reset the connection, and then increase WAL retention and/or increase sync frequency to prevent this from happening in the future. See https://docs.airbyte.com/integrations/sources/postgres/postgres-troubleshooting#under-cdc-incremental-mode-there-are-still-full-refresh-syncs for more details.");
      } else if (sourceConfig.get("replication_method").get(INVALID_CDC_CURSOR_POSITION_PROPERTY).asText().equals(RESYNC_DATA_OPTION)) {
        AirbyteTraceMessageUtility.emitAnalyticsTrace(cdcResyncMessage());
        LOGGER.warn("Saved offset is before Replication slot's confirmed_flush_lsn, Airbyte will trigger sync from scratch");
      }
    } else if (!isDebugMode(sourceConfig) && PostgresUtils.shouldFlushAfterSync(sourceConfig)) {
      // We do not want to acknowledge the WAL logs in debug mode.
      postgresDebeziumStateUtil.commitLSNToPostgresDatabase(database.getDatabaseConfig(),
          savedOffset,
          sourceConfig.get("replication_method").get("replication_slot").asText(),
          sourceConfig.get("replication_method").get("publication").asText(),
          PostgresUtils.getPluginValue(sourceConfig.get("replication_method")));
    }
    final CdcState stateToBeUsed = ctidStateManager.getCdcState();
    final CtidStreams ctidStreams = PostgresCdcCtidUtils.streamsToSyncViaCtid(stateManager.getCdcStateManager(), catalog,
        savedOffsetAfterReplicationSlotLSN);

    final List<AutoCloseableIterator<AirbyteMessage>> initialSyncCtidIterators = new ArrayList<>();
    final List<AirbyteStreamNameNamespacePair> streamsUnderVacuum = new ArrayList<>();
    final List<ConfiguredAirbyteStream> finalListOfStreamsToBeSyncedViaCtid = new ArrayList<>();
    if (!ctidStreams.streamsForCtidSync().isEmpty()) {
      streamsUnderVacuum.addAll(streamsUnderVacuum(database,
          ctidStreams.streamsForCtidSync(), quoteString).result());

      // Any stream currently undergoing full vacuum should not be synced via CTID as it is not a stable
      // cursor. In practice, this will never happen
      // during a sync as a full vacuum in Postgres locks the entire database, so thrown a TransientError
      // in this case and try again.
      if (!streamsUnderVacuum.isEmpty()) {
        throw new TransientErrorException(
            "Postgres database is undergoing a full vacuum - cannot proceed with the sync. Please sync again when the vacuum is finished.");
      }

      final FileNodeHandler fileNodeHandler = PostgresQueryUtils.fileNodeForStreams(database,
          ctidStreams.streamsForCtidSync(),
          quoteString);
      final PostgresCtidHandler ctidHandler;
      // Check if a full vacuum occurred between syncs. If we are unable to determine whether this has
      // occurred, we will exclude the tables for which
      // we were unable to determine this from the initial CTID sync.
      finalListOfStreamsToBeSyncedViaCtid.addAll(ctidStreams.streamsForCtidSync().stream()
          .filter(stream -> !fileNodeHandler.getFailedToQuery().contains(
              new AirbyteStreamNameNamespacePair(stream.getStream().getName(), stream.getStream().getNamespace())))
          .collect(Collectors.toList()));

      LOGGER.info("Streams to be synced via ctid : {}", finalListOfStreamsToBeSyncedViaCtid.size());

      try {
        ctidHandler =
            createInitialLoader(database, finalListOfStreamsToBeSyncedViaCtid, fileNodeHandler, quoteString, ctidStateManager,
                Optional.of(
                    new PostgresCdcConnectorMetadataInjector(emittedAt.toString(), io.airbyte.cdk.db.PostgresUtils.getLsn(database).asLong())));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }

      initialSyncCtidIterators.addAll(ctidHandler.getInitialSyncCtidIterator(
          new ConfiguredAirbyteCatalog().withStreams(finalListOfStreamsToBeSyncedViaCtid), tableNameToTable, emittedAt, /*
                                                                                                                         * decorateWithStartedStatus=
                                                                                                                         */ false, /*
                                                                                                                                    * decorateWithCompletedStatus=
                                                                                                                                    */ false,
          Optional.of(initialLoadTimeout)));
    } else {
      LOGGER.info("No streams will be synced via ctid");
    }

    // Gets the target position.
    final var targetPosition = PostgresCdcTargetPosition.targetPosition(database);
    // Attempt to advance LSN past the target position. For versions of Postgres before PG15, this
    // ensures that there is an event that debezium will
    // receive that is after the target LSN.
    PostgresUtils.advanceLsn(database);
    final AirbyteDebeziumHandler<Long> handler = new AirbyteDebeziumHandler<>(sourceConfig,
        targetPosition, false, firstRecordWaitTime, queueSize, false);
    final PostgresCdcStateHandler postgresCdcStateHandler = new PostgresCdcStateHandler(stateManager);
    final var allCdcStreamList = catalog.getStreams().stream()
        .filter(stream -> stream.getSyncMode() == SyncMode.INCREMENTAL)
        .map(stream -> stream.getStream().getNamespace() + "." + stream.getStream().getName()).toList();
    // Debezium is started for incremental streams that have been started - that is they have been
    // partially or
    // fully completed.
    final var startedCdcStreamList = catalog.getStreams().stream()
        .filter(stream -> stream.getSyncMode() == SyncMode.INCREMENTAL)
        .filter(stream -> isStreamPartiallyOrFullyCompleted(stream, finalListOfStreamsToBeSyncedViaCtid, ctidStreams))
        .map(stream -> stream.getStream().getNamespace() + "." + stream.getStream().getName()).toList();

    // Create TimescaleDB-aware event converter if TimescaleDB support is enabled
    final RelationalDbDebeziumEventConverter baseEventConverter = new RelationalDbDebeziumEventConverter(new PostgresCdcConnectorMetadataInjector(), emittedAt);
    final DebeziumEventConverter eventConverter = TimescaleDbUtils.isTimescaleDbEnabled(sourceConfig) 
        ? new TimescaleDbEventConverter(baseEventConverter, sourceConfig)
        : baseEventConverter;
    
    if (TimescaleDbUtils.isTimescaleDbEnabled(sourceConfig)) {
      LOGGER.info("Using TimescaleDB-aware event converter for SMT message routing");
    }

    final List<AutoCloseableIterator<AirbyteMessage>> cdcStreamsStartStatusEmitters = catalog.getStreams().stream()
        .filter(stream -> stream.getSyncMode() == SyncMode.INCREMENTAL)
        .map(stream -> (AutoCloseableIterator<AirbyteMessage>) new StreamStatusTraceEmitterIterator(
            new AirbyteStreamStatusHolder(
                new io.airbyte.protocol.models.AirbyteStreamNameNamespacePair(stream.getStream().getName(), stream.getStream().getNamespace()),
                AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.STARTED)))
        .toList();

    final List<AutoCloseableIterator<AirbyteMessage>> cdcStreamsCompleteStatusEmitters = catalog.getStreams().stream()
        .filter(stream -> stream.getSyncMode() == SyncMode.INCREMENTAL)
        .map(stream -> (AutoCloseableIterator<AirbyteMessage>) new StreamStatusTraceEmitterIterator(
            new AirbyteStreamStatusHolder(
                new io.airbyte.protocol.models.AirbyteStreamNameNamespacePair(stream.getStream().getName(), stream.getStream().getNamespace()),
                AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE)))
        .toList();

    if (startedCdcStreamList.isEmpty()) {
      LOGGER.info("First sync - no cdc streams have been completed or started");
      /*
       * This is the first run case - no initial loads have been started. In this case, we want to run the
       * iterators in the following order: 1. Run the initial load iterators. This step will timeout and
       * throw a transient error if run for too long (> 8hrs by default). 2. Run the debezium iterators
       * with ALL of the incremental streams configured. This is because if step 1 completes, the initial
       * load can be considered finished.
       */
      final var propertiesManager = createTimescaleDbEnhancedPropertiesManager(database, sourceConfig, catalog, allCdcStreamList);
      final Supplier<AutoCloseableIterator<AirbyteMessage>> incrementalIteratorsSupplier = createIncrementalIteratorsSupplier(handler,
          propertiesManager, eventConverter, stateToBeUsed, postgresCdcStateHandler);
      return Collections.singletonList(
          AutoCloseableIterators.concatWithEagerClose(
              Stream
                  .of(
                      cdcStreamsStartStatusEmitters,
                      initialSyncCtidIterators,
                      Collections.singletonList(AutoCloseableIterators.lazyIterator(incrementalIteratorsSupplier, null)),
                      cdcStreamsCompleteStatusEmitters)
                  .flatMap(Collection::stream)
                  .collect(Collectors.toList()),
              AirbyteTraceMessageUtility::emitStreamStatusTrace));
    } else if (initialSyncCtidIterators.isEmpty()) {
      LOGGER.info("Initial load has finished completely - only reading the WAL");
      /*
       * In this case, the initial load has completed and only debezium should be run. The iterators
       * should be run in the following order: 1. Run the debezium iterators with ALL of the incremental
       * streams configured.
       */
      final var propertiesManager = createTimescaleDbEnhancedPropertiesManager(database, sourceConfig, catalog, allCdcStreamList);
      final Supplier<AutoCloseableIterator<AirbyteMessage>> incrementalIteratorSupplier = createIncrementalIteratorsSupplier(handler,
          propertiesManager, eventConverter, stateToBeUsed, postgresCdcStateHandler);
      return Stream.of(cdcStreamsStartStatusEmitters, Collections.singletonList(incrementalIteratorSupplier.get()), cdcStreamsCompleteStatusEmitters)
          .flatMap(Collection::stream)
          .collect(Collectors.toList());
    } else {
      LOGGER.info("Initial load is in progress - reading WAL first and then resuming with initial load.");
      /*
       * In this case, the initial load has partially completed (WASS case). The iterators should be run
       * in the following order: 1. Run the debezium iterators with only the incremental streams which
       * have been fully or partially completed configured. 2. Resume initial load for partially completed
       * and not started streams. This step will timeout and throw a transient error if run for too long
       * (> 8hrs by default).
       */
      AirbyteTraceMessageUtility.emitAnalyticsTrace(wassOccurrenceMessage());
      final var propertiesManager = createTimescaleDbEnhancedPropertiesManager(database, sourceConfig, catalog, startedCdcStreamList);
      final Supplier<AutoCloseableIterator<AirbyteMessage>> incrementalIteratorSupplier = createIncrementalIteratorsSupplier(handler,
          propertiesManager, eventConverter, stateToBeUsed, postgresCdcStateHandler);
      return Collections.singletonList(
          AutoCloseableIterators.concatWithEagerClose(
              Stream
                  .of(
                      cdcStreamsStartStatusEmitters,
                      Collections.singletonList(AutoCloseableIterators.lazyIterator(incrementalIteratorSupplier, null)),
                      initialSyncCtidIterators,
                      cdcStreamsCompleteStatusEmitters)
                  .flatMap(Collection::stream)
                  .collect(Collectors.toList()),
              AirbyteTraceMessageUtility::emitStreamStatusTrace));
    }
  }

  public static CdcState getCdcState(final JdbcDatabase database,
                                     final StateManager stateManager) {

    final JsonNode sourceConfig = database.getSourceConfig();
    final PostgresDebeziumStateUtil postgresDebeziumStateUtil = new PostgresDebeziumStateUtil();

    final JsonNode initialDebeziumState = postgresDebeziumStateUtil.constructInitialDebeziumState(database,
        sourceConfig.get(JdbcUtils.DATABASE_KEY).asText());

    return (stateManager.getCdcStateManager().getCdcState() == null
        || stateManager.getCdcStateManager().getCdcState().getState() == null) ? new CdcState().withState(initialDebeziumState)
            : stateManager.getCdcStateManager().getCdcState();
  }

  private static boolean isStreamPartiallyOrFullyCompleted(ConfiguredAirbyteStream stream,
                                                           List<ConfiguredAirbyteStream> finalListOfStreamsToBeSynced,
                                                           CtidStreams ctidStreams) {
    boolean isStreamCompleted = !ctidStreams.streamsForCtidSync().contains(stream);
    // A stream has been partially completed if an initial load status exists.
    boolean isStreamPartiallyCompleted = finalListOfStreamsToBeSynced.contains(stream) && (ctidStreams.pairToCtidStatus()
        .get(new AirbyteStreamNameNamespacePair(stream.getStream().getName(), stream.getStream().getNamespace()))) != null;
    return isStreamCompleted || isStreamPartiallyCompleted;
  }

  /**
   * 🎯 NEW ELEGANT APPROACH: Enhance catalog and completedStreamNames with TimescaleDB chunk tables
   * This works by adding synthetic ConfiguredAirbyteStream entries for chunk tables to the catalog,
   * and adding chunk table names to completedStreamNames. The existing getTableIncludelist() logic
   * naturally produces the enhanced table.include.list without any complex inheritance workarounds.
   */
  private static RelationalDbDebeziumPropertiesManager createTimescaleDbEnhancedPropertiesManager(
      JdbcDatabase database, JsonNode sourceConfig, ConfiguredAirbyteCatalog catalog, List<String> completedStreamNames) {
    
    if (!TimescaleDbUtils.isTimescaleDbEnabled(sourceConfig)) {
      // No TimescaleDB - use standard manager
      return new RelationalDbDebeziumPropertiesManager(
          PostgresCdcProperties.getDebeziumDefaultProperties(database), 
          sourceConfig, 
          catalog, 
          completedStreamNames);
    }
    
    LOGGER.info("Enhancing catalog and stream names with TimescaleDB chunk tables for CDC");
    
    try {
      // Discover chunk tables for hypertables in the catalog
      List<String> chunkTableNames = discoverChunkTablesForCatalog(database, catalog, completedStreamNames);
      
      if (chunkTableNames.isEmpty()) {
        LOGGER.warn("No chunk tables found for TimescaleDB hypertables - using standard manager");
        return new RelationalDbDebeziumPropertiesManager(
            PostgresCdcProperties.getDebeziumDefaultProperties(database), 
            sourceConfig, 
            catalog, 
            completedStreamNames);
      }
      
      // ✨ ELEGANT SOLUTION: Enhance the inputs instead of intercepting outputs
      ConfiguredAirbyteCatalog enhancedCatalog = enhanceCatalogWithChunkTables(catalog, chunkTableNames);
      List<String> enhancedStreamNames = enhanceStreamNamesWithChunkTables(completedStreamNames, chunkTableNames);
      
      LOGGER.info("Enhanced catalog with {} chunk tables for TimescaleDB CDC", chunkTableNames.size());
      LOGGER.debug("Chunk tables added: {}", chunkTableNames);
      
      // Create properties manager with enhanced inputs - existing logic naturally produces enhanced table.include.list
      return new RelationalDbDebeziumPropertiesManager(
          PostgresCdcProperties.getDebeziumDefaultProperties(database), 
          sourceConfig, 
          enhancedCatalog, 
          enhancedStreamNames);
      
    } catch (Exception e) {
      LOGGER.error("Failed to discover TimescaleDB chunk tables: {}", e.getMessage(), e);
      // Fall back to standard manager
      return new RelationalDbDebeziumPropertiesManager(
          PostgresCdcProperties.getDebeziumDefaultProperties(database), 
          sourceConfig, 
          catalog, 
          completedStreamNames);
    }
  }

  /**
   * Enhances the catalog by adding synthetic ConfiguredAirbyteStream entries for chunk tables.
   * These entries will be processed by getTableIncludelist() to naturally include chunk tables.
   */
  private static ConfiguredAirbyteCatalog enhanceCatalogWithChunkTables(
      ConfiguredAirbyteCatalog originalCatalog, 
      List<String> chunkTableNames) {
    
    List<ConfiguredAirbyteStream> enhancedStreams = new ArrayList<>(originalCatalog.getStreams());
    
    for (String chunkTableName : chunkTableNames) {
      // Parse "_timescaledb_internal._hyper_1_2_chunk" 
      String[] parts = chunkTableName.split("\\.", 2); // Split into max 2 parts
      if (parts.length != 2) {
        LOGGER.warn("Invalid chunk table name format: {}, skipping", chunkTableName);
        continue;
      }
      
      String namespace = parts[0];  // "_timescaledb_internal"
      String name = parts[1];       // "_hyper_1_2_chunk"
      
      // Create synthetic stream entry that will pass getTableIncludelist() filters
      // ✅ FIX: Add minimal jsonSchema to prevent NPE in getColumnIncludeList()
      JsonNode minimalSchema = Jsons.jsonNode(java.util.Map.of(
          "type", "object",
          "properties", java.util.Map.of()  // Empty properties - Debezium will discover actual columns
      ));
      
      ConfiguredAirbyteStream chunkStream = new ConfiguredAirbyteStream()
          .withSyncMode(SyncMode.INCREMENTAL)  // ✅ Passes .filter { s.syncMode == SyncMode.INCREMENTAL }
          .withStream(new AirbyteStream()
              .withNamespace(namespace)        // ✅ Creates "namespace.name" format
              .withName(name)                  // ✅ Actual chunk name
              .withJsonSchema(minimalSchema)); // ✅ Prevents NPE in getColumnIncludeList()
      
      enhancedStreams.add(chunkStream);
      LOGGER.debug("Added synthetic stream for chunk table: {}.{}", namespace, name);
    }
    
    return new ConfiguredAirbyteCatalog().withStreams(enhancedStreams);
  }

  /**
   * Enhances completedStreamNames by adding chunk table names.
   * These will pass the .filter { completedStreamNames.contains(streamName) } check.
   */
  private static List<String> enhanceStreamNamesWithChunkTables(
      List<String> originalStreamNames, 
      List<String> chunkTableNames) {
    
    List<String> enhancedNames = new ArrayList<>(originalStreamNames);
    enhancedNames.addAll(chunkTableNames);  // Add "_timescaledb_internal._hyper_1_2_chunk"
    
    LOGGER.debug("Enhanced stream names from {} to {} entries", originalStreamNames.size(), enhancedNames.size());
    return enhancedNames;
  }

  /*
   * ========================================================================
   * VESTIGIAL CODE - COMMENTED OUT (Complex inheritance workarounds)
   * ========================================================================
   * The following code was our previous complex approach using inheritance,
   * delegation, and property post-processing. It's now replaced by the simple
   * catalog enhancement approach above. Keeping commented for reference.
   */

  /*
   * OLD COMPLEX APPROACH - Creates a TimescaleDB-aware properties manager that includes 
   * actual chunk table names in the table include list using complex inheritance workarounds.
   */
  /*
  private static RelationalDbDebeziumPropertiesManager createTimescaleDbAwarePropertiesManager(
      JdbcDatabase database, JsonNode sourceConfig, ConfiguredAirbyteCatalog catalog, List<String> completedStreamNames) {
    
    // Create the standard properties manager
    Properties baseProperties = PostgresCdcProperties.getDebeziumDefaultProperties(database);
    RelationalDbDebeziumPropertiesManager standardManager = 
        new RelationalDbDebeziumPropertiesManager(baseProperties, sourceConfig, catalog, completedStreamNames);
    
    // For TimescaleDB, discover chunk tables and store enhancement info for later use
    if (TimescaleDbUtils.isTimescaleDbEnabled(sourceConfig)) {
      LOGGER.info("Preparing TimescaleDB chunk table discovery for CDC enhancement");
      
      try {
        // Get the original table include list that would be generated
        String originalTableList = RelationalDbDebeziumPropertiesManager.Companion.getTableIncludelist(catalog, completedStreamNames);
        LOGGER.info("Original table include list: {}", originalTableList);
        
        if (!originalTableList.isEmpty()) {
          // Discover chunk tables for hypertables in the catalog
          List<String> chunkTableNames = discoverChunkTablesForCatalog(database, catalog, completedStreamNames);
          
          if (!chunkTableNames.isEmpty()) {
            // Format chunk table names with proper escaping
            String chunkTablesFormatted = chunkTableNames.stream()
                .map(chunkName -> "\\Q" + chunkName + "\\E")
                .collect(Collectors.joining(","));
            
            // Create enhanced table list
            String enhancedTableList = originalTableList + "," + chunkTablesFormatted;
            
            LOGGER.info("Enhanced table include list for TimescaleDB with {} chunk tables: {}", 
                       chunkTableNames.size(), enhancedTableList);
            
            // Store the enhanced table list in base properties for later use
            baseProperties.setProperty("_airbyte_timescaledb_enhanced_table_list", enhancedTableList);
          } else {
            LOGGER.warn("No chunk tables found for TimescaleDB hypertables");
          }
        } else {
          LOGGER.warn("No original table include list found");
        }
      } catch (Exception e) {
        LOGGER.error("Failed to discover TimescaleDB chunk tables: {}", e.getMessage(), e);
      }
    }
    
    // Return standard properties manager with enhancement data stored in properties
    return standardManager;
  }
  */

  /*
   * VESTIGIAL CODE - OLD COMPLEX SUPPLIER WITH WRAPPER CLASSES
   * This is replaced by direct call to handler.getIncrementalIterators()
   */
  /*
  @SuppressWarnings("unchecked")
  private static Supplier<AutoCloseableIterator<AirbyteMessage>> getCdcIncrementalIteratorsSupplier(AirbyteDebeziumHandler handler,
                                                                                                    RelationalDbDebeziumPropertiesManager propertiesManager,
                                                                                                    DebeziumEventConverter eventConverter,
                                                                                                    CdcState stateToBeUsed,
                                                                                                    PostgresCdcStateHandler postgresCdcStateHandler) {
    return () -> {
      // Create enhanced handler for TimescaleDB if needed
      TimescaleDbAwareDebeziumHandler enhancedHandler = new TimescaleDbAwareDebeziumHandler(handler);
      
      return enhancedHandler.getIncrementalIterators(
          propertiesManager, eventConverter, new PostgresCdcSavedInfoFetcher(stateToBeUsed), postgresCdcStateHandler);
    };
  }
  */

  /**
   * 🎯 NEW SIMPLE APPROACH: Create incremental iterators supplier without complex wrappers
   */
  @SuppressWarnings("unchecked")
  private static Supplier<AutoCloseableIterator<AirbyteMessage>> createIncrementalIteratorsSupplier(AirbyteDebeziumHandler handler,
                                                                                                    RelationalDbDebeziumPropertiesManager propertiesManager,
                                                                                                    DebeziumEventConverter eventConverter,
                                                                                                    CdcState stateToBeUsed,
                                                                                                    PostgresCdcStateHandler postgresCdcStateHandler) {
    return () -> {
      // Direct call - no complex wrappers needed since catalog enhancement handles everything
      return handler.getIncrementalIterators(
          propertiesManager, eventConverter, new PostgresCdcSavedInfoFetcher(stateToBeUsed), postgresCdcStateHandler);
    };
  }

    /*
   * ========================================================================
   * VESTIGIAL CODE - COMPLEX WRAPPER CLASSES (Commented out)
   * ========================================================================
   * The following classes were our complex inheritance workaround approach.
   * They are no longer needed with the catalog enhancement solution.
   */

  /*
   * OLD WRAPPER CLASS - Wrapper for AirbyteDebeziumHandler that enhances properties 
   * for TimescaleDB before calling the original handler.
   */
  /*
  private static class TimescaleDbAwareDebeziumHandler {
    private final AirbyteDebeziumHandler delegate;
    
    public TimescaleDbAwareDebeziumHandler(AirbyteDebeziumHandler delegate) {
      this.delegate = delegate;
    }
    
    public AutoCloseableIterator<AirbyteMessage> getIncrementalIterators(
        RelationalDbDebeziumPropertiesManager propertiesManager,
        DebeziumEventConverter eventConverter,
        PostgresCdcSavedInfoFetcher savedInfoFetcher,
        PostgresCdcStateHandler stateHandler) {
      
      // Create a wrapper that enhances properties when requested
      TimescaleDbPropertiesManagerDelegate enhancedManager = new TimescaleDbPropertiesManagerDelegate(propertiesManager);
      
      // Call the original handler with the enhanced manager
      return delegate.getIncrementalIterators(enhancedManager, eventConverter, savedInfoFetcher, stateHandler);
    }
  }
  */

  /*
   * OLD DELEGATE CLASS - Delegate that enhances getDebeziumProperties calls for TimescaleDB 
   * without extending the final class.
   */
  /*
  private static class TimescaleDbPropertiesManagerDelegate extends RelationalDbDebeziumPropertiesManager {
    private final RelationalDbDebeziumPropertiesManager delegate;
    
    public TimescaleDbPropertiesManagerDelegate(RelationalDbDebeziumPropertiesManager delegate) {
      // Required super call with minimal dummy data
      super(new Properties(), 
            Jsons.jsonNode(java.util.Map.of("host", "dummy", "port", "5432", "database", "dummy", "username", "dummy")), 
            new ConfiguredAirbyteCatalog().withStreams(java.util.List.of()), 
            java.util.List.of());
      this.delegate = delegate;
    }
    
    @Override
    public Properties getDebeziumProperties(AirbyteFileOffsetBackingStore offsetManager) {
      Properties properties = delegate.getDebeziumProperties(offsetManager);
      return enhancePropertiesForTimescaleDb(properties);
    }

    @Override
    public Properties getDebeziumProperties(AirbyteFileOffsetBackingStore offsetManager, 
                                          Optional<AirbyteSchemaHistoryStorage> schemaHistoryManager) {
      Properties properties = delegate.getDebeziumProperties(offsetManager, schemaHistoryManager);
      return enhancePropertiesForTimescaleDb(properties);
    }
    
    private Properties enhancePropertiesForTimescaleDb(Properties originalProperties) {
      // Check if TimescaleDB enhancement is available
      String enhancedTableList = originalProperties.getProperty("_airbyte_timescaledb_enhanced_table_list");
      if (enhancedTableList != null && !enhancedTableList.isEmpty()) {
        LOGGER.info("Applying TimescaleDB enhanced table include list");
        originalProperties.setProperty("table.include.list", enhancedTableList);
        LOGGER.debug("Final table.include.list: {}", enhancedTableList);
        
        // Remove the temporary property
        originalProperties.remove("_airbyte_timescaledb_enhanced_table_list");
      }
      
      return originalProperties;
    }
  }
  */

  /**
   * Discovers all chunk tables for hypertables that are present in the catalog.
   * Returns a list of fully qualified chunk table names (schema.table_name).
   */
  private static List<String> discoverChunkTablesForCatalog(JdbcDatabase database, ConfiguredAirbyteCatalog catalog, 
                                                           List<String> completedStreamNames) throws Exception {
    List<String> chunkTableNames = new ArrayList<>();
    
    // Get hypertable names from catalog that are in completed stream names
    List<String> hypertableNames = catalog.getStreams().stream()
        .filter(stream -> stream.getSyncMode() == SyncMode.INCREMENTAL)
        .map(stream -> stream.getStream().getNamespace() + "." + stream.getStream().getName())
        .filter(streamName -> completedStreamNames.contains(streamName))
        .map(streamName -> {
          // Extract just the table name (after the last dot)
          String[] parts = streamName.split("\\.");
          return parts[parts.length - 1];
        })
        .collect(Collectors.toList());
    
    if (hypertableNames.isEmpty()) {
      LOGGER.info("No hypertables found in completed stream names for chunk discovery");
      return chunkTableNames;
    }
    
    LOGGER.info("Discovering chunk tables for hypertables: {}", hypertableNames);
    
    // Query to get all chunks for each hypertable individually (safer than array approach)
    String chunkDiscoveryQuery = """
        SELECT DISTINCT
            c.chunk_schema || '.' || c.chunk_name as full_chunk_name
        FROM timescaledb_information.chunks c
        WHERE c.hypertable_name = ?
        ORDER BY c.chunk_schema || '.' || c.chunk_name
        """;
    
    // Query each hypertable individually to get its chunks
    for (String hypertableName : hypertableNames) {
      try {
        List<JsonNode> chunkResults = database.queryJsons(chunkDiscoveryQuery, hypertableName);
        
        for (JsonNode result : chunkResults) {
          String chunkName = result.get("full_chunk_name").asText();
          chunkTableNames.add(chunkName);
          LOGGER.debug("Found chunk table: {} for hypertable: {}", chunkName, hypertableName);
        }
        
        LOGGER.debug("Found {} chunk tables for hypertable: {}", chunkResults.size(), hypertableName);
      } catch (SQLException e) {
        LOGGER.warn("Failed to discover chunks for hypertable '{}': {}", hypertableName, e.getMessage());
        // Continue with other hypertables even if one fails
      }
    }
    
    LOGGER.info("Discovered {} total chunk tables for TimescaleDB hypertables", chunkTableNames.size());
    return chunkTableNames;
  }

}
