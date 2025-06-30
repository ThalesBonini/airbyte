/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.timescaledb.utils;

/**
 * Contains SQL queries for TimescaleDB operations.
 * All queries are optimized for minimal database load and self-contained execution.
 */
public final class TimescaleDbQueries {

  private TimescaleDbQueries() {
    // Utility class
  }

  /**
   * Query to check if TimescaleDB extension is installed.
   */
  public static final String CHECK_TIMESCALEDB_EXTENSION = """
      SELECT EXISTS(
          SELECT 1 FROM pg_extension 
          WHERE extname = 'timescaledb'
      ) as timescaledb_available
      """;

  /**
   * Query to check if a table is a hypertable.
   */
  public static final String CHECK_IS_HYPERTABLE = """
      SELECT EXISTS(
          SELECT 1 FROM _timescaledb_catalog.hypertable 
          WHERE schema_name = ? AND table_name = ?
      ) as is_hypertable
      """;

  /**
   * Query to discover all hypertables in the database.
   */
  public static final String DISCOVER_HYPERTABLES = """
      SELECT 
          h.schema_name,
          h.table_name,
          h.num_dimensions,
          h.chunk_sizing_func_schema,
          h.chunk_sizing_func_name,
          h.chunk_target_size,
          d.column_name as time_column_name,
          d.interval_length as chunk_time_interval
      FROM _timescaledb_catalog.hypertable h
      LEFT JOIN _timescaledb_catalog.dimension d ON h.id = d.hypertable_id
      WHERE d.dimension_type = 1  -- Time dimension
      ORDER BY h.schema_name, h.table_name
      """;

  /**
   * Query to discover chunks for a specific hypertable.
   * Includes chunk metadata and size information.
   */
  public static final String DISCOVER_CHUNKS_FOR_HYPERTABLE = """
      SELECT 
          c.chunk_schema,
          c.chunk_name,
          c.hypertable_schema,
          c.hypertable_name,
          c.range_start,
          c.range_end,
          COALESCE(pg_total_relation_size(format('%I.%I', c.chunk_schema, c.chunk_name)), 0) as size_bytes,
          c.chunk_id
      FROM timescaledb_information.chunks c
      WHERE c.hypertable_schema = ? AND c.hypertable_name = ?
      ORDER BY c.range_start
      """;

  /**
   * Query to discover all chunks in the database with basic information.
   * Used for bulk operations and caching.
   */
  public static final String DISCOVER_ALL_CHUNKS = """
      SELECT 
          c.chunk_schema,
          c.chunk_name,
          c.hypertable_schema,
          c.hypertable_name,
          c.range_start,
          c.range_end,
          COALESCE(pg_total_relation_size(format('%I.%I', c.chunk_schema, c.chunk_name)), 0) as size_bytes,
          c.chunk_id
      FROM timescaledb_information.chunks c
      ORDER BY c.hypertable_schema, c.hypertable_name, c.range_start
      """;

  /**
   * Query to get chunk time interval for a hypertable.
   */
  public static final String GET_CHUNK_TIME_INTERVAL = """
      SELECT 
          d.interval_length as chunk_time_interval,
          d.column_name as time_column_name
      FROM _timescaledb_catalog.hypertable h
      JOIN _timescaledb_catalog.dimension d ON h.id = d.hypertable_id
      WHERE h.schema_name = ? AND h.table_name = ?
        AND d.dimension_type = 1  -- Time dimension
      """;

  /**
   * Query to discover all schemas that contain hypertables.
   * Used for automatic schema inclusion in CDC configuration.
   */
  public static final String DISCOVER_HYPERTABLE_SCHEMAS = """
      SELECT DISTINCT h.schema_name 
      FROM _timescaledb_catalog.hypertable h
      WHERE h.schema_name IS NOT NULL
        AND h.schema_name != '_timescaledb_internal'
      ORDER BY h.schema_name
      """;

  /**
   * Query to get continuous aggregates information.
   */
  public static final String DISCOVER_CONTINUOUS_AGGREGATES = """
      SELECT 
          ca.user_view_schema,
          ca.user_view_name,
          ca.partial_view_schema,
          ca.partial_view_name,
          ca.bucket_width,
          ca.job_id,
          ca.refresh_lag,
          ca.max_interval_per_job
      FROM _timescaledb_catalog.continuous_agg ca
      ORDER BY ca.user_view_schema, ca.user_view_name
      """;

  /**
   * Query to check table statistics for chunk processing optimization.
   */
  public static final String GET_TABLE_STATS = """
      SELECT 
          schemaname,
          tablename,
          n_tup_ins as inserts,
          n_tup_upd as updates,
          n_tup_del as deletes,
          n_live_tup as live_tuples,
          n_dead_tup as dead_tuples,
          last_vacuum,
          last_autovacuum,
          last_analyze,
          last_autoanalyze
      FROM pg_stat_user_tables
      WHERE schemaname = ? AND tablename = ?
      """;

  /**
   * Query to get chunk-specific statistics.
   */
  public static final String GET_CHUNK_STATS = """
      SELECT 
          schemaname,
          tablename,
          n_live_tup as live_tuples,
          n_dead_tup as dead_tuples,
          pg_total_relation_size(schemaname||'.'||tablename) as total_size_bytes,
          pg_relation_size(schemaname||'.'||tablename) as table_size_bytes
      FROM pg_stat_user_tables
      WHERE schemaname = ? AND tablename = ?
      """;

  /**
   * Query to build the schema include list for TimescaleDB CDC.
   * Returns comma-separated list of schemas to include.
   */
  public static final String BUILD_SCHEMA_INCLUDE_LIST = """
      SELECT string_agg(DISTINCT schema_name, ',' ORDER BY schema_name) as schema_list
      FROM (
          SELECT '_timescaledb_internal' as schema_name
          UNION
          SELECT DISTINCT h.schema_name
          FROM _timescaledb_catalog.hypertable h
          WHERE h.schema_name IS NOT NULL
            AND h.schema_name != '_timescaledb_internal'
      ) schemas
      """;

  /**
   * Query to validate chunk existence and accessibility.
   */
  public static final String VALIDATE_CHUNK_ACCESS = """
      SELECT 
          c.relname as chunk_name,
          n.nspname as chunk_schema,
          c.reltuples as estimated_rows,
          pg_total_relation_size(c.oid) as size_bytes
      FROM pg_class c
      JOIN pg_namespace n ON c.relnamespace = n.oid
      WHERE n.nspname = ? AND c.relname = ?
        AND c.relkind = 'r'  -- Regular table
      """;

  /**
   * Query to get the primary time column for a hypertable.
   */
  public static final String GET_TIME_COLUMN = """
      SELECT d.column_name as time_column
      FROM _timescaledb_catalog.hypertable h
      JOIN _timescaledb_catalog.dimension d ON h.id = d.hypertable_id
      WHERE h.schema_name = ? AND h.table_name = ?
        AND d.dimension_type = 1  -- Time dimension
      LIMIT 1
      """;

  /**
   * Query to get column information for a chunk/table.
   */
  public static final String GET_COLUMN_INFO = """
      SELECT 
          column_name,
          data_type,
          is_nullable,
          column_default,
          ordinal_position
      FROM information_schema.columns
      WHERE table_schema = ? AND table_name = ?
      ORDER BY ordinal_position
      """;

  /**
   * Template for building chunk-specific SELECT queries.
   * Use String.format() with: column_list, schema, table, order_by_clause
   */
  public static final String CHUNK_SELECT_TEMPLATE = """
      SELECT %s 
      FROM %s.%s 
      ORDER BY %s
      """;

  /**
   * Query to estimate chunk processing time based on size and complexity.
   */
  public static final String ESTIMATE_CHUNK_PROCESSING = """
      SELECT 
          pg_total_relation_size(format('%I.%I', ?, ?)) as size_bytes,
          (SELECT COUNT(*) FROM information_schema.columns 
           WHERE table_schema = ? AND table_name = ?) as column_count,
          CASE 
              WHEN pg_total_relation_size(format('%I.%I', ?, ?)) < 1024*1024 THEN 'small'
              WHEN pg_total_relation_size(format('%I.%I', ?, ?)) < 100*1024*1024 THEN 'medium'
              ELSE 'large'
          END as size_category
      """;

} 