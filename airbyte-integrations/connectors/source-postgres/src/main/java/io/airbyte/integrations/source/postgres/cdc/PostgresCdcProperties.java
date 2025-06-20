/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.cdc;

import static io.airbyte.cdk.db.jdbc.JdbcSSLConnectionUtils.CLIENT_KEY_STORE_PASS;
import static io.airbyte.cdk.db.jdbc.JdbcSSLConnectionUtils.CLIENT_KEY_STORE_URL;
import static io.airbyte.cdk.db.jdbc.JdbcSSLConnectionUtils.SSL_MODE;
import static io.airbyte.cdk.db.jdbc.JdbcSSLConnectionUtils.TRUST_KEY_STORE_PASS;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.cdk.db.jdbc.JdbcSSLConnectionUtils.SslMode;
import io.airbyte.cdk.db.jdbc.JdbcUtils;
import io.airbyte.integrations.source.postgres.PostgresSource;
import io.airbyte.integrations.source.postgres.PostgresUtils;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresCdcProperties {

  private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(10L);

  // Test execution latency is lower when heartbeats are more frequent.
  private static final Duration HEARTBEAT_INTERVAL_IN_TESTS = Duration.ofSeconds(1L);

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCdcProperties.class);

  public static Properties getDebeziumDefaultProperties(final JdbcDatabase database) {
    final JsonNode sourceConfig = database.getSourceConfig();
    final Properties props = commonProperties(database);
    props.setProperty("plugin.name", PostgresUtils.getPluginValue(sourceConfig.get("replication_method")));
    if (sourceConfig.has("snapshot_mode")) {
      // The parameter `snapshot_mode` is passed in test to simulate reading the WAL Logs directly and
      // skip initial snapshot
      props.setProperty("snapshot.mode", sourceConfig.get("snapshot_mode").asText());
    } else {
      props.setProperty("snapshot.mode", "initial");
    }

    props.setProperty("slot.name", sourceConfig.get("replication_method").get("replication_slot").asText());
    props.setProperty("publication.name", sourceConfig.get("replication_method").get("publication").asText());

    props.setProperty("publication.autocreate.mode", "disabled");

    // TimescaleDB configuration
    if (sourceConfig.has("timescaledb_support") && sourceConfig.get("timescaledb_support").asBoolean()) {
      LOGGER.info("TimescaleDB support enabled, configuring TimescaleDB SMT");
      LOGGER.info("Source config keys: {}", sourceConfig.fieldNames());
      configureTimescaleDbProperties(props, database);
      LOGGER.info("TimescaleDB configuration completed. Final properties contain {} entries", props.size());
      // Log key TimescaleDB properties for debugging
      if (props.containsKey("schema.include.list")) {
        LOGGER.info("schema.include.list: {}", props.getProperty("schema.include.list"));
      }
      if (props.containsKey("transforms")) {
        LOGGER.info("transforms: {}", props.getProperty("transforms"));
      }
      // Log all properties for debugging
      LOGGER.info("All Debezium properties:");
      props.forEach((key, value) -> LOGGER.info("  {}: {}", key, value));
    } else {
      LOGGER.info("TimescaleDB support is disabled or not configured");
    }

    return props;
  }

  private static Properties commonProperties(final JdbcDatabase database) {
    final JsonNode dbConfig = database.getDatabaseConfig();
    final JsonNode sourceConfig = database.getSourceConfig();

    final Properties props = new Properties();
    props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");

    props.setProperty("converters", "datetime");
    props.setProperty("datetime.type", PostgresConverter.class.getName());
    props.setProperty("include.unknown.datatypes", "true");

    final Duration heartbeatInterval =
        (database.getSourceConfig().has("is_test") && database.getSourceConfig().get("is_test").asBoolean())
            ? HEARTBEAT_INTERVAL_IN_TESTS
            : HEARTBEAT_INTERVAL;
    props.setProperty("heartbeat.interval.ms", Long.toString(heartbeatInterval.toMillis()));

    if (sourceConfig.get("replication_method").has("heartbeat_action_query")
        && !sourceConfig.get("replication_method").get("heartbeat_action_query").asText().isEmpty()) {
      props.setProperty("heartbeat.action.query", sourceConfig.get("replication_method").get("heartbeat_action_query").asText());
    }

    if (PostgresUtils.shouldFlushAfterSync(sourceConfig)) {
      props.setProperty("flush.lsn.source", "false");
    }

    // Check params for SSL connection in config and add properties for CDC SSL connection
    // https://debezium.io/documentation/reference/2.2/connectors/postgresql.html#postgresql-property-database-sslmode
    if (!sourceConfig.has(JdbcUtils.SSL_KEY) || sourceConfig.get(JdbcUtils.SSL_KEY).asBoolean()) {
      if (sourceConfig.has(JdbcUtils.SSL_MODE_KEY) && sourceConfig.get(JdbcUtils.SSL_MODE_KEY).has(JdbcUtils.MODE_KEY)) {

        if (dbConfig.has(SSL_MODE) && !dbConfig.get(SSL_MODE).asText().isEmpty()) {
          LOGGER.debug("sslMode: {}", dbConfig.get(SSL_MODE).asText());
          props.setProperty("database.sslmode", PostgresSource.toSslJdbcParamInternal(SslMode.valueOf(dbConfig.get(SSL_MODE).asText())));
        }

        if (dbConfig.has(PostgresSource.CA_CERTIFICATE_PATH) && !dbConfig.get(PostgresSource.CA_CERTIFICATE_PATH).asText().isEmpty()) {
          props.setProperty("database.sslrootcert", dbConfig.get(PostgresSource.CA_CERTIFICATE_PATH).asText());
        }

        if (dbConfig.has(TRUST_KEY_STORE_PASS) && !dbConfig.get(TRUST_KEY_STORE_PASS).asText().isEmpty()) {
          props.setProperty("database.ssl.truststore.password", dbConfig.get(TRUST_KEY_STORE_PASS).asText());
        }

        if (dbConfig.has(CLIENT_KEY_STORE_URL) && !dbConfig.get(CLIENT_KEY_STORE_URL).asText().isEmpty()) {
          props.setProperty("database.sslkey", Path.of(URI.create(dbConfig.get(CLIENT_KEY_STORE_URL).asText())).toString());
        }

        if (dbConfig.has(CLIENT_KEY_STORE_PASS) && !dbConfig.get(CLIENT_KEY_STORE_PASS).asText().isEmpty()) {
          props.setProperty("database.sslpassword", dbConfig.get(CLIENT_KEY_STORE_PASS).asText());
        }
      } else {
        props.setProperty("database.sslmode", "required");
      }
    }
    return props;
  }

  public static Properties getSnapshotProperties(final JdbcDatabase database) {
    final Properties props = commonProperties(database);
    props.setProperty("snapshot.mode", "initial_only");
    return props;
  }

  /**
   * Configures TimescaleDB-specific properties for Debezium connector.
   * Based on the TimescaleDB SMT documentation and real-world testing, this includes:
   * - Including BOTH _timescaledb_internal schema (for chunk events) AND schemas containing hypertables
   * - Configuring the TimescaleDB transform to route chunk events to logical hypertable topics
   * - Setting up database connection details for the SMT
   * 
   * CRITICAL: The SMT routes events FROM _timescaledb_internal._hyper_X_Y_chunk TO logical hypertables
   * in other schemas. Therefore, we must include BOTH schemas in the catalog to avoid 
   * "Missing catalog stream" errors.
   */
  private static void configureTimescaleDbProperties(final Properties props, final JdbcDatabase database) {
    final JsonNode sourceConfig = database.getSourceConfig();
    final JsonNode dbConfig = database.getDatabaseConfig();
    
    // For TimescaleDB, we need to include:
    // 1. _timescaledb_internal schema (physical chunk tables)
    // 2. Schemas containing hypertables (logical tables that SMT routes to)
    String schemaIncludeList = getTimescaleDbSchemaIncludeList(database);
    props.setProperty("schema.include.list", schemaIncludeList);
    
    LOGGER.info("TimescaleDB mode: Including schemas for both chunks and hypertables: {}", schemaIncludeList);
    
    // Configure TimescaleDB transform
    props.setProperty("transforms", "timescaledb");
    props.setProperty("transforms.timescaledb.type", "io.debezium.connector.postgresql.transforms.timescaledb.TimescaleDb");
    
    // Configure database connection details for the SMT using source config
    // (the original config before JDBC transformation)
    if (sourceConfig.has(JdbcUtils.HOST_KEY) && sourceConfig.get(JdbcUtils.HOST_KEY) != null) {
      props.setProperty("transforms.timescaledb.database.hostname", sourceConfig.get(JdbcUtils.HOST_KEY).asText());
    }
    
    if (sourceConfig.has(JdbcUtils.PORT_KEY) && sourceConfig.get(JdbcUtils.PORT_KEY) != null) {
      props.setProperty("transforms.timescaledb.database.port", sourceConfig.get(JdbcUtils.PORT_KEY).asText());
    }
    
    if (sourceConfig.has(JdbcUtils.USERNAME_KEY) && sourceConfig.get(JdbcUtils.USERNAME_KEY) != null) {
      props.setProperty("transforms.timescaledb.database.user", sourceConfig.get(JdbcUtils.USERNAME_KEY).asText());
    }
    
    if (sourceConfig.has(JdbcUtils.PASSWORD_KEY) && sourceConfig.get(JdbcUtils.PASSWORD_KEY) != null) {
      props.setProperty("transforms.timescaledb.database.password", sourceConfig.get(JdbcUtils.PASSWORD_KEY).asText());
    }
    
    if (sourceConfig.has(JdbcUtils.DATABASE_KEY) && sourceConfig.get(JdbcUtils.DATABASE_KEY) != null) {
      props.setProperty("transforms.timescaledb.database.dbname", sourceConfig.get(JdbcUtils.DATABASE_KEY).asText());
    }
    
    // Enable additional debugging for TimescaleDB SMT
    LOGGER.info("TimescaleDB SMT configured with:");
    LOGGER.info("  schema.include.list: _timescaledb_internal");
    LOGGER.info("  transforms: timescaledb");
    LOGGER.info("  TimescaleDB SMT will capture chunk events and route to logical hypertable topics");
    LOGGER.info("  Hypertable events will appear on topics like: {}.public.metrics", props.getProperty("database.server.name", "server"));
  }

  /**
   * Determines the schema include list for TimescaleDB configurations.
   * 
   * This method queries the database to find all schemas that contain hypertables,
   * then creates an include list that contains both:
   * 1. _timescaledb_internal (for physical chunk tables)
   * 2. All schemas containing hypertables (for logical tables that SMT routes to)
   * 
   * This prevents "Missing catalog stream" errors when the TimescaleDB SMT routes
   * events from chunk tables to hypertable topics.
   */
  private static String getTimescaleDbSchemaIncludeList(final JdbcDatabase database) {
    try {
      // Query to find all schemas containing hypertables
      final String hypertableSchemaQuery = """
          SELECT DISTINCT h.schema_name 
          FROM _timescaledb_catalog.hypertable h
          WHERE h.schema_name IS NOT NULL
          ORDER BY h.schema_name
          """;
      
      final var hypertableSchemas = database.queryJsons(hypertableSchemaQuery);
      
      // Start with _timescaledb_internal (always required for chunks)
      StringBuilder schemaList = new StringBuilder("_timescaledb_internal");
      
      // Add schemas containing hypertables
      for (var schema : hypertableSchemas) {
        String schemaName = schema.get("schema_name").asText();
        if (!schemaName.equals("_timescaledb_internal")) {
          schemaList.append(",").append(schemaName);
        }
      }
      
      String result = schemaList.toString();
      LOGGER.info("TimescaleDB hypertable schema discovery found schemas: {}", result);
      
      return result;
      
    } catch (Exception e) {
      LOGGER.warn("Failed to discover TimescaleDB hypertable schemas, falling back to _timescaledb_internal only: {}", e.getMessage());
      // Fallback to original behavior if schema discovery fails
      return "_timescaledb_internal";
    }
  }

}
