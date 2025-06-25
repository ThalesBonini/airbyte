## Objective

Provide a clear development guideline for implementing CDC-based ingestion of **TimescaleDB hypertables** in Airbyte, fully automated and transparent to the user.

---

## Current Context

1. **Manual Chunk Discovery:** New chunks (partitions) require running schema discovery and manual activation in the Airbyte connector.
2. **Explicit Internal Schema Inclusion:** On the first sync, users must manually add the `_timescaledb_internal` schema to the connector configuration.
3. **Monolithic Snapshot:** The initial ingestion performs a single `FULL TABLE SCAN` inside one transaction, causing long locks on large hypertables and potential contention issues.

---

## Feature Requirements

1. **Automated Chunk Discovery**

   * Detect new chunks in real time and enable them for ingestion automatically, without requiring manual schema discovery.
2. **Implicit Internal Schema Handling**

   * Automatically include the `_timescaledb_internal` schema in hypertable ingestion flows, eliminating user configuration.
3. **Transactional Chunk-by-Chunk Ingestion**

   * Ingest each chunk in a separate transaction rather than scanning the entire hypertable in one go, reducing lock duration and risk.

---

## Development Roadmap

### 1. Research & Architecture Design

* **TimescaleDB Catalog Exploration**

  * Analyze queries on `timescaledb_information.chunks` to understand how to list existing and new chunks.
  * Compare polling vs. event-driven approaches (`LISTEN`/`NOTIFY`) for chunk detection.
* **Component Definition**

  * **Chunk Watcher:** Module that monitors and emits events for new chunks.
  * **Dynamic Configuration Pipeline:** Orchestrator injecting internal schema and chunk metadata.
  * **Transactional Executor:** Wrapper managing per-chunk ingestion transactions.

### 2. Chunk Watcher Implementation

1. **PoC with Polling**

   * Periodic query against `timescaledb_information.chunks`.
   * Maintain local cache of processed chunks.
   * Emit `ChunkDetected` events.
2. **Event-Driven Upgrade**

   * Create a PL/pgSQL trigger on chunk creation firing a `NOTIFY`.
   * Maintain a persistent Postgres connection in the connector to `LISTEN` on that channel.

### 3. Implicit Configuration Pipeline

1. **Hypertable Detection**

   * Query `pg_class` and size metadata to detect hypertables.
2. **Schema Injection**

   * Automatically add `_timescaledb_internal` to the connector’s `schemasToDiscover` configuration.
   * Ensure this update happens transparently before ingestion begins.

### 4. Transactional Ingestion Mechanism

1. **Snapshot Logic Refactor**

   * When a source is identified as a hypertable, switch from full snapshot to chunk-level ingestion.
2. **Chunk-by-Chunk Executor**

   * For each chunk:

     * Begin transaction (`BEGIN`).
     * Scan and ingest chunk data.
     * Commit transaction (`COMMIT`).
3. **Fallback for Regular Tables**

   * Maintain current full-table snapshot behavior for non-hypertables or unpartitioned tables.

### 5. Testing & Validation

* **Unit Tests**

  * Mock `timescaledb_information.chunks` queries and `NOTIFY` events.
  * Validate dynamic configuration injection logic.
* **Integration Tests**

  * Use a real TimescaleDB instance with small and large hypertables.
  * Verify no long-duration locks and complete ingestion of all chunks.
* **Performance Benchmarks**

  * Compare duration and resource usage: full-table snapshot vs. chunk-by-chunk ingestion.

### 6. Documentation & Release

* Update README and connector docs to describe the new transparent behavior.
* Publish a detailed changelog highlighting enhancements.
* Provide fallback instructions and compatibility notes.
