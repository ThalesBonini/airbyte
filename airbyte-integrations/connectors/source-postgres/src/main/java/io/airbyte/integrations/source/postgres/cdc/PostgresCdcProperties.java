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
import io.airbyte.integrations.source.postgres.TimescaleDbUtils;
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

    // Fix logical replication protocol version for TimescaleDB compatibility
    configureLogicalReplicationProtocol(props, database);

    // Configure TimescaleDB SMT if enabled
    addTimescaleDbTransforms(props, database);
    configureSchemaIncludeList(props, database);
    configureTableIncludeList(props, database);

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
   * Configure TimescaleDB Single Message Transform (SMT) when TimescaleDB support is enabled.
   * The SMT reroutes messages from chunk tables to logical hypertable topics.
   *
   * @param props The Debezium properties to configure
   * @param database The JDBC database connection
   */
  private static void addTimescaleDbTransforms(final Properties props, final JdbcDatabase database) {
    final JsonNode sourceConfig = database.getSourceConfig();
    final JsonNode dbConfig = database.getDatabaseConfig();

    if (TimescaleDbUtils.isTimescaleDbEnabled(sourceConfig)) {
      LOGGER.info("Configuring TimescaleDB Single Message Transform (SMT)");

      // Add TimescaleDB SMT configuration
      props.setProperty("transforms", "timescaledb");
      props.setProperty("transforms.timescaledb.type",
          "io.debezium.connector.postgresql.transforms.timescaledb.TimescaleDb");

      // Configure database connection details for the SMT
      // Access basic connection details from sourceConfig (like PostgresSource does)
      if (sourceConfig.has(JdbcUtils.HOST_KEY)) {
        props.setProperty("transforms.timescaledb.database.hostname",
            sourceConfig.get(JdbcUtils.HOST_KEY).asText());
      }
      
      if (sourceConfig.has(JdbcUtils.PORT_KEY)) {
        props.setProperty("transforms.timescaledb.database.port",
            sourceConfig.get(JdbcUtils.PORT_KEY).asText());
      }
      
      if (sourceConfig.has(JdbcUtils.DATABASE_KEY)) {
        props.setProperty("transforms.timescaledb.database.dbname",
            sourceConfig.get(JdbcUtils.DATABASE_KEY).asText());
      }
      
      // Access username/password from sourceConfig (like PostgresSource does)
      if (sourceConfig.has(JdbcUtils.USERNAME_KEY)) {
        props.setProperty("transforms.timescaledb.database.user",
            sourceConfig.get(JdbcUtils.USERNAME_KEY).asText());
      }
      
      if (sourceConfig.has(JdbcUtils.PASSWORD_KEY)) {
        props.setProperty("transforms.timescaledb.database.password",
            sourceConfig.get(JdbcUtils.PASSWORD_KEY).asText());
      }

      // Copy SSL configuration for the SMT if present
      copySSLConfigurationForTimescaleDb(props, sourceConfig, dbConfig);

      LOGGER.info("TimescaleDB SMT configured successfully");
    }
  }

  /**
   * Configure schema include list to capture TimescaleDB internal tables when TimescaleDB support is enabled.
   * This is critical for the SMT to receive chunk table changes.
   *
   * @param props The Debezium properties to configure
   * @param database The JDBC database connection
   */
  private static void configureSchemaIncludeList(final Properties props, final JdbcDatabase database) {
    final JsonNode sourceConfig = database.getSourceConfig();

    if (TimescaleDbUtils.isTimescaleDbEnabled(sourceConfig)) {
      LOGGER.info("Including _timescaledb_internal schema for chunk table capture");

      // Include TimescaleDB internal schema - CRITICAL for chunk capture
      final String existingSchemas = props.getProperty("schema.include.list", "");
      if (existingSchemas.isEmpty()) {
        props.setProperty("schema.include.list", "_timescaledb_internal");
      } else {
        props.setProperty("schema.include.list", existingSchemas + ",_timescaledb_internal");
      }

      LOGGER.debug("Schema include list updated: {}", props.getProperty("schema.include.list"));
    }
  }

  /**
   * Configure table include list to capture TimescaleDB chunk tables when TimescaleDB support is enabled.
   * This is critical for capturing actual data changes that occur in chunk tables.
   *
   * @param props The Debezium properties to configure
   * @param database The JDBC database connection
   */
  private static void configureTableIncludeList(final Properties props, final JdbcDatabase database) {
    final JsonNode sourceConfig = database.getSourceConfig();

    if (TimescaleDbUtils.isTimescaleDbEnabled(sourceConfig)) {
      LOGGER.info("Configuring table include list for TimescaleDB chunk table capture");

      // NOTE: The actual chunk table discovery and table.include.list enhancement 
      // is now handled in PostgresCdcCtidInitializer.createTimescaleDbAwarePropertiesManager()
      // to avoid being overridden by RelationalDbDebeziumPropertiesManager.getIncludeConfiguration()
      
      LOGGER.info("TimescaleDB chunk table pattern configured for properties manager");
    }
  }

  /**
   * Copy SSL configuration to TimescaleDB SMT if SSL is configured.
   * The SMT needs its own database connection with the same SSL settings.
   *
   * @param props The Debezium properties to configure
   * @param sourceConfig The source configuration
   * @param dbConfig The database configuration
   */
  private static void copySSLConfigurationForTimescaleDb(final Properties props,
                                                        final JsonNode sourceConfig,
                                                        final JsonNode dbConfig) {
    // Copy SSL configuration for TimescaleDB SMT if SSL is enabled
    if (!sourceConfig.has(JdbcUtils.SSL_KEY) || sourceConfig.get(JdbcUtils.SSL_KEY).asBoolean()) {
      if (sourceConfig.has(JdbcUtils.SSL_MODE_KEY) && sourceConfig.get(JdbcUtils.SSL_MODE_KEY).has(JdbcUtils.MODE_KEY)) {

        if (dbConfig.has(SSL_MODE) && !dbConfig.get(SSL_MODE).asText().isEmpty()) {
          props.setProperty("transforms.timescaledb.database.sslmode",
              PostgresSource.toSslJdbcParamInternal(SslMode.valueOf(dbConfig.get(SSL_MODE).asText())));
          LOGGER.debug("TimescaleDB SMT SSL mode: {}", dbConfig.get(SSL_MODE).asText());
        }

        if (dbConfig.has(PostgresSource.CA_CERTIFICATE_PATH) && !dbConfig.get(PostgresSource.CA_CERTIFICATE_PATH).asText().isEmpty()) {
          props.setProperty("transforms.timescaledb.database.sslrootcert",
              dbConfig.get(PostgresSource.CA_CERTIFICATE_PATH).asText());
        }

        if (dbConfig.has(CLIENT_KEY_STORE_URL) && !dbConfig.get(CLIENT_KEY_STORE_URL).asText().isEmpty()) {
          props.setProperty("transforms.timescaledb.database.sslkey",
              Path.of(URI.create(dbConfig.get(CLIENT_KEY_STORE_URL).asText())).toString());
        }

        if (dbConfig.has(CLIENT_KEY_STORE_PASS) && !dbConfig.get(CLIENT_KEY_STORE_PASS).asText().isEmpty()) {
          props.setProperty("transforms.timescaledb.database.sslpassword",
              dbConfig.get(CLIENT_KEY_STORE_PASS).asText());
        }
      } else {
        props.setProperty("transforms.timescaledb.database.sslmode", "required");
      }
    }
  }

  /**
   * Configure logical replication protocol version to fix TimescaleDB/PostgreSQL compatibility issues.
   * 
   * TimescaleDB/PostgreSQL may require protocol version 1 or higher, but Debezium defaults to version 0.
   * This method ensures the correct protocol version is used, particularly important for TimescaleDB environments.
   *
   * @param props The Debezium properties to configure
   * @param database The JDBC database connection
   */
  private static void configureLogicalReplicationProtocol(final Properties props, final JdbcDatabase database) {
    final JsonNode sourceConfig = database.getSourceConfig();
    
    // Configure logical replication protocol version for better compatibility
    // Protocol version 1 is widely supported and resolves "proto_version=0" errors
    props.setProperty("slot.stream.params", "proto_version=1;publication_names=" + 
        sourceConfig.get("replication_method").get("publication").asText());
    
    if (TimescaleDbUtils.isTimescaleDbEnabled(sourceConfig)) {
      LOGGER.info("Configured logical replication protocol version 1 for TimescaleDB compatibility");
    } else {
      LOGGER.debug("Configured logical replication protocol version 1 for enhanced PostgreSQL compatibility");
    }
  }

}
