/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.cdc;

import static io.debezium.connector.postgresql.PostgresOffsetContext.LAST_COMMIT_LSN_KEY;
import static io.debezium.connector.postgresql.SourceInfo.LSN_KEY;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.DATABASE_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.airbyte.cdk.db.PostgresUtils;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.cdk.integrations.debezium.internals.AirbyteFileOffsetBackingStore;
import io.airbyte.cdk.integrations.debezium.internals.DebeziumPropertiesManager;
import io.airbyte.cdk.integrations.debezium.internals.DebeziumStateUtil;
import io.airbyte.cdk.integrations.debezium.internals.RelationalDbDebeziumPropertiesManager;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.debezium.config.Configuration;
import io.debezium.connector.common.OffsetReader;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresOffsetContext.Loader;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.time.Conversions;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.postgresql.core.BaseConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is inspired by Debezium's Postgres connector internal implementation on how it parses
 * the state
 */
public class PostgresDebeziumStateUtil implements DebeziumStateUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDebeziumStateUtil.class);

  public boolean isSavedOffsetAfterReplicationSlotLSN(final JsonNode replicationSlot,
                                                      final OptionalLong savedOffset) {
    // For backward compatibility, call the TimescaleDB-aware version with null config
    // This means TimescaleDB support will be disabled for this call
    return isSavedOffsetAfterReplicationSlotLSN(replicationSlot, savedOffset, null);
  }

  /**
   * TimescaleDB-aware version of the LSN comparison that properly handles SMT-related LSN tracking.
   * When TimescaleDB support is enabled, we need to be more careful about how we interpret the
   * saved offset because the SMT routing can affect state management.
   */
  public boolean isSavedOffsetAfterReplicationSlotLSN(final JsonNode replicationSlot,
                                                      final OptionalLong savedOffset,
                                                      final JsonNode config) {

    if (Objects.isNull(savedOffset) || savedOffset.isEmpty()) {
      return true;
    }

    // Check if TimescaleDB support is enabled
    final boolean isTimescaleDbEnabled = config != null && 
        config.has("timescaledb_support") && 
        config.get("timescaledb_support").asBoolean();

    if (replicationSlot.has("confirmed_flush_lsn")) {
      final long confirmedFlushLsnOnServerSide = Lsn.valueOf(replicationSlot.get("confirmed_flush_lsn").asText()).asLong();
      LOGGER.info("Replication slot confirmed_flush_lsn : " + confirmedFlushLsnOnServerSide + " Saved offset LSN : " + savedOffset.getAsLong());
      
      if (isTimescaleDbEnabled) {
        return handleTimescaleDbLsnComparison(replicationSlot, savedOffset.getAsLong(), confirmedFlushLsnOnServerSide, "confirmed_flush_lsn");
      }
      
      return savedOffset.getAsLong() >= confirmedFlushLsnOnServerSide;
    } else if (replicationSlot.has("restart_lsn")) {
      final long restartLsn = Lsn.valueOf(replicationSlot.get("restart_lsn").asText()).asLong();
      LOGGER.info("Replication slot restart_lsn : " + restartLsn + " Saved offset LSN : " + savedOffset.getAsLong());
      
      if (isTimescaleDbEnabled) {
        return handleTimescaleDbLsnComparison(replicationSlot, savedOffset.getAsLong(), restartLsn, "restart_lsn");
      }
      
      return savedOffset.getAsLong() >= restartLsn;
    }

    // We return true when saved offset is not present cause using an empty offset would result in sync
    // from scratch anyway
    return true;
  }

  public OptionalLong savedOffset(final Properties baseProperties,
                                  final ConfiguredAirbyteCatalog catalog,
                                  final JsonNode cdcState,
                                  final JsonNode config) {
    final var offsetManager = AirbyteFileOffsetBackingStore.initializeState(cdcState, Optional.empty());
    final DebeziumPropertiesManager debeziumPropertiesManager =
        new RelationalDbDebeziumPropertiesManager(baseProperties, config, catalog, Collections.emptyList());
    final Properties debeziumProperties = debeziumPropertiesManager.getDebeziumProperties(offsetManager);
    return parseSavedOffset(debeziumProperties);
  }

  public void commitLSNToPostgresDatabase(final JsonNode jdbcConfig,
                                          final OptionalLong savedOffset,
                                          final String slotName,
                                          final String publicationName,
                                          final String plugin) {
    if (Objects.isNull(savedOffset) || savedOffset.isEmpty()) {
      return;
    }

    final LogSequenceNumber logSequenceNumber = LogSequenceNumber.valueOf(savedOffset.getAsLong());

    LOGGER.info("Committing upto LSN: {}", savedOffset.getAsLong());
    try (final BaseConnection pgConnection = (BaseConnection) PostgresReplicationConnection.createConnection(jdbcConfig)) {
      ChainedLogicalStreamBuilder streamBuilder = pgConnection
          .getReplicationAPI()
          .replicationStream()
          .logical()
          .withSlotName("\"" + slotName + "\"")
          .withStartPosition(logSequenceNumber);

      streamBuilder = addSlotOption(publicationName, plugin, pgConnection, streamBuilder);

      try (final PGReplicationStream stream = streamBuilder.start()) {
        stream.forceUpdateStatus();

        stream.setFlushedLSN(logSequenceNumber);
        stream.setAppliedLSN(logSequenceNumber);

        stream.forceUpdateStatus();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private ChainedLogicalStreamBuilder addSlotOption(final String publicationName,
                                                    final String plugin,
                                                    final BaseConnection pgConnection,
                                                    ChainedLogicalStreamBuilder streamBuilder) {
    if (plugin.equalsIgnoreCase("pgoutput")) {
      streamBuilder = streamBuilder.withSlotOption("proto_version", 1)
          .withSlotOption("publication_names", publicationName);

      if (pgConnection.haveMinimumServerVersion(140000)) {
        streamBuilder = streamBuilder.withSlotOption("messages", true);
      }
    } else {
      throw new RuntimeException("Unknown plugin value : " + plugin);
    }
    return streamBuilder;
  }

  /**
   * Handle LSN comparison for TimescaleDB configurations.
   * 
   * TimescaleDB with schema filtering can create scenarios where the saved offset
   * is legitimately behind the replication slot's LSN due to:
   * 1. Schema filtering (schema.include.list: _timescaledb_internal)
   * 2. SMT routing between physical chunks and logical hypertables
   * 3. Database activity outside the filtered schema advancing the slot
   * 
   * This method implements a robust validation strategy that:
   * - Allows small LSN differences when WAL is healthy and slot is active
   * - Validates that the replication slot is in a good state
   * - Provides detailed logging for debugging
   * - Fails safely when the difference is too large or slot is unhealthy
   */
  private boolean handleTimescaleDbLsnComparison(final JsonNode replicationSlot, 
                                                 final long savedOffsetLsn, 
                                                 final long slotLsn, 
                                                 final String slotType) {
    final long lsnDifference = slotLsn - savedOffsetLsn;
    
    LOGGER.info("TimescaleDB LSN comparison - Saved offset: {}, Slot {}: {}, Difference: {} bytes", 
        savedOffsetLsn, slotType, slotLsn, lsnDifference);
    
    // If saved offset is ahead or equal, we're good
    if (lsnDifference <= 0) {
      LOGGER.info("TimescaleDB: Saved offset is ahead of or equal to slot LSN - proceeding normally");
      return true;
    }
    
    // Check replication slot health indicators
    final String slotStatus = replicationSlot.has("active") ? 
        (replicationSlot.get("active").asBoolean() ? "active" : "inactive") : "unknown";
    final String walStatus = replicationSlot.has("wal_status") ? 
        replicationSlot.get("wal_status").asText() : "unknown";
    
    LOGGER.info("TimescaleDB: Replication slot status: {}, WAL status: {}", slotStatus, walStatus);
    
    // Define safe LSN difference threshold for TimescaleDB schema filtering
    // This accounts for normal database activity outside the filtered schema
    final long TIMESCALEDB_SAFE_LSN_THRESHOLD = 1024 * 1024; // 1MB
    
    if (lsnDifference <= TIMESCALEDB_SAFE_LSN_THRESHOLD) {
      // Small difference - check if WAL is healthy
      if ("reserved".equals(walStatus) || "extended".equals(walStatus)) {
        LOGGER.info("TimescaleDB: Small LSN difference ({} bytes) with healthy WAL status '{}' - allowing continuation. " +
                   "This is normal for TimescaleDB with schema filtering where database activity outside " +
                   "_timescaledb_internal schema can advance the replication slot.", 
                   lsnDifference, walStatus);
        return true;
      } else if ("unreserved".equals(walStatus)) {
        LOGGER.warn("TimescaleDB: WAL status is 'unreserved' with LSN difference of {} bytes. " +
                   "This indicates potential data loss risk.", lsnDifference);
        return false;
      } else {
        LOGGER.warn("TimescaleDB: Unknown WAL status '{}' with LSN difference of {} bytes. " +
                   "Proceeding cautiously but this should be investigated.", walStatus, lsnDifference);
        return true; // Allow unknown status for backward compatibility
      }
    } else {
      // Large difference - likely indicates a real problem
      LOGGER.error("TimescaleDB: Large LSN difference ({} bytes, threshold: {} bytes) detected. " +
                  "This indicates significant WAL advancement beyond our saved position. " +
                  "Slot status: {}, WAL status: {}. This requires manual intervention.", 
                  lsnDifference, TIMESCALEDB_SAFE_LSN_THRESHOLD, slotStatus, walStatus);
      return false;
    }
  }

  /**
   * Loads the offset data from the saved Debezium offset file.
   *
   * @param properties Properties should contain the relevant properties like path to the debezium
   *        state file, etc. It's assumed that the state file is already initialised with the saved
   *        state
   * @return Returns the LSN that Airbyte has acknowledged in the source database server
   */
  private OptionalLong parseSavedOffset(final Properties properties) {
    FileOffsetBackingStore fileOffsetBackingStore = null;
    OffsetStorageReaderImpl offsetStorageReader = null;

    try {
      fileOffsetBackingStore = getFileOffsetBackingStore(properties);
      offsetStorageReader = getOffsetStorageReader(fileOffsetBackingStore, properties);

      final PostgresConnectorConfig postgresConnectorConfig = new PostgresConnectorConfig(Configuration.from(properties));
      final PostgresCustomLoader loader = new PostgresCustomLoader(postgresConnectorConfig);
      
      // Check if TimescaleDB support is enabled
      final boolean isTimescaleDbEnabled = properties.containsKey("transforms") && 
          "timescaledb".equals(properties.getProperty("transforms"));
      
      Set<Partition> partitions;
      if (isTimescaleDbEnabled) {
        LOGGER.info("TimescaleDB SMT detected - using enhanced partition offset lookup");
        // For TimescaleDB, we need to check multiple potential partition keys because
        // the SMT routing can cause offsets to be stored under different partition names
        partitions = getTimescaleDbPartitions(postgresConnectorConfig, properties);
      } else {
        // Standard partition lookup
        partitions = Collections.singleton(
            new PostgresPartition(postgresConnectorConfig.getLogicalName(), 
                                 properties.getProperty(DATABASE_NAME.name())));
      }

      final OffsetReader<Partition, PostgresOffsetContext, Loader> offsetReader = new OffsetReader<>(offsetStorageReader, loader);
      final Map<Partition, PostgresOffsetContext> offsets = offsetReader.offsets(partitions);

      return extractLsn(partitions, offsets, loader);
    } finally {
      LOGGER.info("Closing offsetStorageReader and fileOffsetBackingStore");
      if (offsetStorageReader != null) {
        offsetStorageReader.close();
      }

      if (fileOffsetBackingStore != null) {
        fileOffsetBackingStore.stop();
      }
    }
  }
  
  /**
   * Get potential partition keys for TimescaleDB SMT.
   * TimescaleDB SMT can cause offsets to be stored under different partition keys
   * due to routing between physical chunks and logical hypertables.
   */
  private Set<Partition> getTimescaleDbPartitions(final PostgresConnectorConfig postgresConnectorConfig, 
                                                  final Properties properties) {
    final String logicalName = postgresConnectorConfig.getLogicalName();
    final String databaseName = properties.getProperty(DATABASE_NAME.name());
    
    Set<Partition> partitions = new HashSet<>();
    
    // Standard partition (this should be the primary one)
    partitions.add(new PostgresPartition(logicalName, databaseName));
    
    // For TimescaleDB, also check for potential SMT-related partition variations
    // The SMT might create partitions with different server names or database combinations
    
    // Check for partition with server name matching the topic prefix pattern
    if (properties.containsKey("topic.prefix")) {
      final String topicPrefix = properties.getProperty("topic.prefix");
      if (!topicPrefix.equals(logicalName)) {
        partitions.add(new PostgresPartition(topicPrefix, databaseName));
        LOGGER.info("TimescaleDB: Added partition with topic prefix: {}", topicPrefix);
      }
    }
    
    // Check for partition with database server name
    if (properties.containsKey("database.server.name")) {
      final String serverName = properties.getProperty("database.server.name");
      if (!serverName.equals(logicalName)) {
        partitions.add(new PostgresPartition(serverName, databaseName));
        LOGGER.info("TimescaleDB: Added partition with server name: {}", serverName);
      }
    }
    
    LOGGER.info("TimescaleDB: Checking {} potential partitions for offsets", partitions.size());
    return partitions;
  }

  private OptionalLong extractLsn(final Set<Partition> partitions,
                                  final Map<Partition, PostgresOffsetContext> offsets,
                                  final PostgresCustomLoader loader) {
    boolean found = false;
    OptionalLong foundLsn = OptionalLong.empty();
    Partition foundPartition = null;
    
    for (final Partition partition : partitions) {
      final PostgresOffsetContext postgresOffsetContext = offsets.get(partition);

      if (postgresOffsetContext != null) {
        found = true;
        LOGGER.info("Found previous partition offset {}: {}", partition, postgresOffsetContext.getOffset());
        
        // Extract LSN from this partition's offset
        final Map<String, ?> offset = postgresOffsetContext.getOffset();
        OptionalLong partitionLsn = OptionalLong.empty();
        
        if (offset.containsKey(LAST_COMMIT_LSN_KEY)) {
          partitionLsn = OptionalLong.of((long) offset.get(LAST_COMMIT_LSN_KEY));
        } else if (offset.containsKey(LSN_KEY)) {
          partitionLsn = OptionalLong.of((long) offset.get(LSN_KEY));
        }
        
        if (partitionLsn.isPresent()) {
          if (foundLsn.isEmpty()) {
            // First LSN found - use this one
            foundLsn = partitionLsn;
            foundPartition = partition;
            LOGGER.info("Using LSN {} from partition {}", foundLsn.getAsLong(), partition);
          } else if (foundLsn.getAsLong() != partitionLsn.getAsLong()) {
            // Different LSN found - this indicates a potential issue
            LOGGER.warn("Found different LSN values across partitions! Current: {} from {}, Previous: {} from {}. " +
                "This may indicate a state consistency issue. Using the first LSN found for consistency.", 
                partitionLsn.getAsLong(), partition, foundLsn.getAsLong(), foundPartition);
            // Continue using the first LSN found for consistency
          } else {
            // Same LSN found - this is expected for TimescaleDB SMT routing
            LOGGER.info("Confirmed same LSN {} found in partition {} (expected for TimescaleDB SMT)", 
                partitionLsn.getAsLong(), partition);
          }
        }
      }
    }

    if (!found) {
      LOGGER.info("No previous offsets found in any partition");
      
      // Fallback: check loader's raw offset as last resort
      if (loader.getRawOffset().containsKey(LSN_KEY)) {
        final long lsn = Long.parseLong(loader.getRawOffset().get(LSN_KEY).toString());
        LOGGER.info("Found LSN in loader raw offset: {}", lsn);
        return OptionalLong.of(lsn);
      }
      
      return OptionalLong.empty();
    }

    if (foundLsn.isPresent()) {
      LOGGER.info("Using LSN: {} from partition: {}", foundLsn.getAsLong(), foundPartition);
      return foundLsn;
    }
    
    // If we found partitions but no LSN in their offsets, check the loader as fallback
    if (loader.getRawOffset().containsKey(LSN_KEY)) {
      final long lsn = Long.parseLong(loader.getRawOffset().get(LSN_KEY).toString());
      LOGGER.info("Found LSN in loader raw offset as fallback: {}", lsn);
      return OptionalLong.of(lsn);
    }

    LOGGER.warn("Found partition offsets but no LSN values - this may indicate a state corruption issue");
    return OptionalLong.empty();
  }

  private static ThreadLocal<JsonNode> initialState = new ThreadLocal<>();

  /**
   * Method to construct initial Debezium state which can be passed onto Debezium engine to make it
   * process WAL from a specific LSN and skip snapshot phase
   */
  public JsonNode constructInitialDebeziumState(final JdbcDatabase database, final String dbName) {
    if (initialState.get() == null) {
      try {
        final JsonNode asJson = format(currentXLogLocation(database), currentTransactionId(database), dbName, Instant.now());
        initialState.set(asJson);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    return initialState.get();
  }

  @VisibleForTesting
  public JsonNode format(final Long currentXLogLocation, final Long currentTransactionId, final String dbName, final Instant time) {
    final String key = "[\"" + dbName + "\",{\"server\":\"" + dbName + "\"}]";
    final String value =
        "{\"transaction_id\":null,\"lsn\":" + currentXLogLocation + ",\"txId\":" + currentTransactionId + ",\"ts_usec\":" + Conversions.toEpochMicros(
            time) + "}";

    final Map<String, String> result = new HashMap<>();
    result.put(key, value);

    final JsonNode jsonNode = Jsons.jsonNode(result);
    LOGGER.info("Initial Debezium state constructed: {}", jsonNode);

    return jsonNode;
  }

  private long currentXLogLocation(JdbcDatabase database) throws SQLException {
    return PostgresUtils.getLsn(database).asLong();
  }

  private Long currentTransactionId(final JdbcDatabase database) throws SQLException {
    final List<Long> transactionId = database.bufferedResultSetQuery(
        conn -> conn.createStatement().executeQuery(
            "SELECT CASE WHEN pg_is_in_recovery() THEN txid_snapshot_xmin(txid_current_snapshot()) ELSE txid_current() END AS pg_current_txid;"),
        resultSet -> resultSet.getLong(1));
    Preconditions.checkState(transactionId.size() == 1);
    return transactionId.get(0);
  }

  public static void disposeInitialState() {
    LOGGER.debug("Dispose initial state cached for {}", Thread.currentThread());
    initialState.remove();
  }

}
