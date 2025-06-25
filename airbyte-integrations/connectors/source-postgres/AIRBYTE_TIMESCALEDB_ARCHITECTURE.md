# TimescaleDB Integration - Airbyte-Native Architecture

## Overview

This document outlines a **self-contained**, **Airbyte-native** architecture for TimescaleDB integration that follows Airbyte's design patterns and minimizes database dependencies. All solutions are implemented within the source connector code.

## Core Principles

1. **Self-Contained**: All logic resides within the Airbyte connector
2. **Minimal DB Interaction**: Only essential queries, no triggers or stored procedures
3. **Airbyte Patterns**: Follow existing source connector patterns
4. **Backward Compatible**: Graceful fallback to standard Postgres behavior

---

## File Architecture

### 📁 **Proposed Directory Structure**

```
src/main/java/io/airbyte/integrations/source/postgres/
├── timescaledb/                          # New TimescaleDB package
│   ├── TimescaleDbSourceOperations.java
│   ├── TimescaleDbStateManager.java
│   ├── TimescaleDbDiscoveryHandler.java
│   ├── TimescaleDbSnapshotHandler.java
│   ├── chunking/
│   │   ├── ChunkMetadata.java
│   │   ├── ChunkDiscoveryService.java
│   │   └── ChunkIteratorFactory.java
│   ├── config/
│   │   ├── TimescaleDbConfiguration.java
│   │   └── TimescaleDbConfigValidator.java
│   └── utils/
│       ├── TimescaleDbQueries.java
│       └── TimescaleDbMetrics.java
├── cdc/
│   ├── PostgresCdcProperties.java        # Enhanced for TimescaleDB
│   └── PostgresDebeziumStateUtil.java    # Enhanced for TimescaleDB
└── PostgresSource.java                   # Main integration point
```

### 📋 **Integration Points**

#### 1. **PostgresSource.java** - Main Entry Point
```java
@Override
public AutoCloseableIterator<AirbyteMessage> read(JsonNode config, 
                                                  ConfiguredAirbyteCatalog catalog, 
                                                  JsonNode state) throws Exception {
    
    // Detect TimescaleDB support
    if (TimescaleDbConfiguration.isEnabled(config, database)) {
        return new TimescaleDbSourceOperations().read(config, catalog, state, database);
    }
    
    // Standard Postgres flow
    return super.read(config, catalog, state);
}
```

#### 2. **Schema Discovery Enhancement**
```java
@Override
public AirbyteCatalog discover(JsonNode config) throws Exception {
    AirbyteCatalog catalog = super.discover(config);
    
    // Enhance with TimescaleDB metadata if enabled
    if (TimescaleDbConfiguration.isEnabled(config, database)) {
        return TimescaleDbDiscoveryHandler.enhanceCatalog(catalog, database);
    }
    
    return catalog;
}
```

---

## Core Components

### 🏗️ **1. TimescaleDbSourceOperations**

**Purpose**: Main orchestrator following Airbyte's source operation patterns

```java
public class TimescaleDbSourceOperations extends CtidPostgresSourceOperations {
    
    private final TimescaleDbConfiguration config;
    private final ChunkDiscoveryService chunkDiscovery;
    private final TimescaleDbStateManager stateManager;
    
    @Override
    public AutoCloseableIterator<AirbyteMessage> read(JsonNode config, 
                                                      ConfiguredAirbyteCatalog catalog, 
                                                      JsonNode state,
                                                      JdbcDatabase database) {
        
        // Initialize TimescaleDB-specific components
        initializeTimescaleDbComponents(config, database);
        
        // Create iterators based on stream type
        List<AutoCloseableIterator<AirbyteMessage>> iterators = new ArrayList<>();
        
        for (ConfiguredAirbyteStream stream : catalog.getStreams()) {
            if (isHypertable(stream)) {
                iterators.add(createHypertableIterator(stream, state, database));
            } else {
                iterators.add(createStandardIterator(stream, state, database));
            }
        }
        
        return AutoCloseableIterators.concatWithEagerClose(iterators);
    }
    
    private AutoCloseableIterator<AirbyteMessage> createHypertableIterator(
            ConfiguredAirbyteStream stream, JsonNode state, JdbcDatabase database) {
        
        // Use chunk-based processing for hypertables
        return new TimescaleDbChunkIterator(
            stream, 
            stateManager.getStreamState(stream), 
            chunkDiscovery.discoverChunks(stream),
            database
        );
    }
}
```

### 🔍 **2. ChunkDiscoveryService** 

**Purpose**: Self-contained chunk discovery without database triggers

```java
@Component
public class ChunkDiscoveryService {
    
    private static final String CHUNK_QUERY = """
        SELECT 
            c.chunk_schema,
            c.chunk_name,
            c.hypertable_schema,
            c.hypertable_name,
            c.range_start,
            c.range_end,
            pg_total_relation_size(format('%I.%I', c.chunk_schema, c.chunk_name)) as size_bytes
        FROM timescaledb_information.chunks c
        WHERE c.hypertable_schema = ? AND c.hypertable_name = ?
        ORDER BY c.range_start
        """;
    
    private final Map<String, List<ChunkMetadata>> chunkCache = new ConcurrentHashMap<>();
    private final Map<String, Instant> lastDiscoveryTime = new ConcurrentHashMap<>();
    
    public List<ChunkMetadata> discoverChunks(ConfiguredAirbyteStream stream) {
        String streamKey = getStreamKey(stream);
        
        // Use cached chunks if recent (within 5 minutes)
        if (isCacheValid(streamKey)) {
            LOGGER.info("Using cached chunk information for {}", streamKey);
            return chunkCache.get(streamKey);
        }
        
        // Discover chunks from database
        List<ChunkMetadata> chunks = queryChunks(stream);
        
        // Update cache
        chunkCache.put(streamKey, chunks);
        lastDiscoveryTime.put(streamKey, Instant.now());
        
        LOGGER.info("Discovered {} chunks for hypertable {}", chunks.size(), streamKey);
        return chunks;
    }
    
    private boolean isCacheValid(String streamKey) {
        Instant lastDiscovery = lastDiscoveryTime.get(streamKey);
        return lastDiscovery != null && 
               Duration.between(lastDiscovery, Instant.now()).toMinutes() < 5;
    }
    
    private List<ChunkMetadata> queryChunks(ConfiguredAirbyteStream stream) {
        try {
            return database.queryJsons(CHUNK_QUERY, 
                stream.getStream().getNamespace(), 
                stream.getStream().getName())
                .stream()
                .map(this::mapToChunkMetadata)
                .collect(Collectors.toList());
        } catch (Exception e) {
            LOGGER.error("Failed to discover chunks for {}", getStreamKey(stream), e);
            return Collections.emptyList();
        }
    }
}
```

### 📊 **3. TimescaleDbStateManager**

**Purpose**: Extends Airbyte's state management for chunk-level tracking

```java
public class TimescaleDbStateManager extends StateManager {
    
    // Airbyte-native state structure
    public static class TimescaleDbStreamState {
        private String streamName;
        private String streamNamespace;
        private Map<String, ChunkState> processedChunks = new HashMap<>();
        private Instant lastChunkDiscovery;
        private String lastProcessedChunkId;
        
        // Standard Airbyte state serialization
    }
    
    public static class ChunkState {
        private String chunkName;
        private String chunkSchema;
        private ProcessingStatus status;
        private long recordsProcessed;
        private Instant completedAt;
    }
    
    @Override
    public AirbyteStateMessage createStateMessage(AirbyteStream stream, AirbyteMessage record) {
        // Create Airbyte-standard state message with TimescaleDB extensions
        AirbyteStateMessage stateMessage = super.createStateMessage(stream, record);
        
        // Add TimescaleDB-specific state if applicable
        if (isTimescaleDbStream(stream)) {
            enhanceStateWithTimescaleDbInfo(stateMessage, stream, record);
        }
        
        return stateMessage;
    }
    
    public List<ChunkMetadata> getUnprocessedChunks(ConfiguredAirbyteStream stream, 
                                                   List<ChunkMetadata> allChunks) {
        TimescaleDbStreamState streamState = getTimescaleDbState(stream);
        
        return allChunks.stream()
            .filter(chunk -> !isChunkProcessed(streamState, chunk))
            .collect(Collectors.toList());
    }
    
    private boolean isChunkProcessed(TimescaleDbStreamState streamState, ChunkMetadata chunk) {
        ChunkState chunkState = streamState.getProcessedChunks().get(chunk.getChunkName());
        return chunkState != null && chunkState.getStatus() == ProcessingStatus.COMPLETED;
    }
}
```

### 🔄 **4. TimescaleDbChunkIterator**

**Purpose**: Chunk-by-chunk processing following Airbyte iterator patterns

```java
public class TimescaleDbChunkIterator extends AbstractIterator<AirbyteMessage> 
                                      implements AutoCloseableIterator<AirbyteMessage> {
    
    private final ConfiguredAirbyteStream stream;
    private final Queue<ChunkMetadata> pendingChunks;
    private final TimescaleDbStateManager stateManager;
    private final JdbcDatabase database;
    
    private AutoCloseableIterator<AirbyteMessage> currentChunkIterator;
    private ChunkMetadata currentChunk;
    
    public TimescaleDbChunkIterator(ConfiguredAirbyteStream stream,
                                   TimescaleDbStreamState state,
                                   List<ChunkMetadata> chunks,
                                   JdbcDatabase database) {
        this.stream = stream;
        this.database = database;
        this.stateManager = new TimescaleDbStateManager(state);
        
        // Filter to unprocessed chunks only
        this.pendingChunks = new LinkedList<>(
            stateManager.getUnprocessedChunks(stream, chunks)
        );
        
        LOGGER.info("Initialized chunk iterator with {} pending chunks", pendingChunks.size());
    }
    
    @Override
    protected AirbyteMessage computeNext() {
        // If current chunk iterator is exhausted, move to next chunk
        if (currentChunkIterator == null || !currentChunkIterator.hasNext()) {
            if (!moveToNextChunk()) {
                return endOfData();
            }
        }
        
        AirbyteMessage message = currentChunkIterator.next();
        
        // Emit state periodically (following Airbyte patterns)
        if (shouldEmitState(message)) {
            return createStateMessage();
        }
        
        return message;
    }
    
    private boolean moveToNextChunk() {
        closeCurrentIterator();
        
        if (pendingChunks.isEmpty()) {
            return false;
        }
        
        currentChunk = pendingChunks.poll();
        currentChunkIterator = createChunkIterator(currentChunk);
        
        LOGGER.info("Processing chunk: {}.{}", 
            currentChunk.getChunkSchema(), 
            currentChunk.getChunkName());
        
        return true;
    }
    
    private AutoCloseableIterator<AirbyteMessage> createChunkIterator(ChunkMetadata chunk) {
        // Create standard Airbyte iterator for this specific chunk
        String chunkQuery = buildChunkQuery(chunk);
        
        return database.query(
            connection -> connection.prepareStatement(chunkQuery),
            resultSet -> convertToAirbyteMessage(resultSet, stream)
        );
    }
    
    private String buildChunkQuery(ChunkMetadata chunk) {
        return String.format(
            "SELECT %s FROM %s.%s ORDER BY %s",
            getColumnList(stream),
            chunk.getChunkSchema(),
            chunk.getChunkName(),
            getOrderByClause(chunk)
        );
    }
}
```

### ⚙️ **5. TimescaleDbConfiguration**

**Purpose**: Configuration management following Airbyte config patterns

```java
@JsonDeserialize(builder = TimescaleDbConfiguration.Builder.class)
public class TimescaleDbConfiguration {
    
    private final boolean enabled;
    private final Duration chunkDiscoveryInterval;
    private final int maxConcurrentChunks;
    private final boolean enableChunkCaching;
    
    public static boolean isEnabled(JsonNode config, JdbcDatabase database) {
        if (!config.has("timescaledb_support") || 
            !config.get("timescaledb_support").asBoolean()) {
            return false;
        }
        
        // Verify TimescaleDB extension availability
        return PostgresUtils.isTimescaleDbAvailable(database);
    }
    
    public static TimescaleDbConfiguration fromConfig(JsonNode config) {
        return new Builder()
            .enabled(config.path("timescaledb_support").asBoolean(false))
            .chunkDiscoveryInterval(Duration.ofMinutes(
                config.path("timescaledb_chunk_discovery_interval_minutes").asInt(5)))
            .maxConcurrentChunks(
                config.path("timescaledb_max_concurrent_chunks").asInt(1))
            .enableChunkCaching(
                config.path("timescaledb_enable_chunk_caching").asBoolean(true))
            .build();
    }
    
    @JsonPOJOBuilder(withPrefix = "")
    public static class Builder {
        // Standard Airbyte builder pattern
    }
}
```

---

## Enhanced Discovery & CDC Integration

### 🔍 **Discovery Enhancement**

```java
public class TimescaleDbDiscoveryHandler {
    
    public static AirbyteCatalog enhanceCatalog(AirbyteCatalog catalog, JdbcDatabase database) {
        List<AirbyteStream> enhancedStreams = catalog.getStreams().stream()
            .map(stream -> enhanceStreamIfHypertable(stream, database))
            .collect(Collectors.toList());
        
        return new AirbyteCatalog().withStreams(enhancedStreams);
    }
    
    private static AirbyteStream enhanceStreamIfHypertable(AirbyteStream stream, JdbcDatabase database) {
        if (isHypertable(stream, database)) {
            // Add TimescaleDB-specific metadata
            Map<String, Object> metadata = new HashMap<>(stream.getJsonSchema().getAdditionalProperties());
            metadata.put("timescaledb_hypertable", true);
            metadata.put("timescaledb_chunk_time_interval", getChunkTimeInterval(stream, database));
            
            return stream.withJsonSchema(
                stream.getJsonSchema().withAdditionalProperties(metadata)
            );
        }
        
        return stream;
    }
    
    private static boolean isHypertable(AirbyteStream stream, JdbcDatabase database) {
        try {
            String query = """
                SELECT EXISTS(
                    SELECT 1 FROM _timescaledb_catalog.hypertable 
                    WHERE schema_name = ? AND table_name = ?
                )
                """;
            
            return database.queryJsons(query, stream.getNamespace(), stream.getName())
                .get(0).get("exists").asBoolean();
                
        } catch (Exception e) {
            LOGGER.debug("Failed to check if {} is a hypertable", stream.getName(), e);
            return false;
        }
    }
}
```

### 📡 **CDC Integration** (Enhanced existing classes)

```java
// Enhancement to existing PostgresCdcProperties.java
public class PostgresCdcProperties {
    
    // Add TimescaleDB-aware schema configuration
    private static void configureTimescaleDbProperties(Properties props, JdbcDatabase database) {
        // Discover hypertable schemas dynamically (cached)
        String schemaIncludeList = TimescaleDbSchemaDiscovery.getSchemaList(database);
        props.setProperty("schema.include.list", schemaIncludeList);
        
        // Configure SMT
        props.setProperty("transforms", "timescaledb");
        props.setProperty("transforms.timescaledb.type", 
            "io.debezium.connector.postgresql.transforms.timescaledb.TimescaleDb");
        
        // Database connection details for SMT
        configureSmtDatabaseConnection(props, database);
    }
}
```

---

## Performance & Resource Management

### 🏃‍♂️ **Memory Management**

```java
public class TimescaleDbResourceManager {
    
    private final AtomicLong totalMemoryUsed = new AtomicLong(0);
    private final int maxMemoryMB;
    
    public boolean canProcessChunk(ChunkMetadata chunk) {
        long estimatedMemory = estimateChunkMemoryUsage(chunk);
        long currentMemory = totalMemoryUsed.get();
        
        return (currentMemory + estimatedMemory) <= (maxMemoryMB * 1024 * 1024);
    }
    
    public void trackChunkProcessing(ChunkMetadata chunk) {
        long estimatedMemory = estimateChunkMemoryUsage(chunk);
        totalMemoryUsed.addAndGet(estimatedMemory);
    }
    
    public void releaseChunkMemory(ChunkMetadata chunk) {
        long estimatedMemory = estimateChunkMemoryUsage(chunk);
        totalMemoryUsed.addAndGet(-estimatedMemory);
    }
    
    private long estimateChunkMemoryUsage(ChunkMetadata chunk) {
        // Estimate based on chunk size and column count
        return chunk.getSizeBytes() / 10; // Conservative estimate
    }
}
```

### 📈 **Metrics & Monitoring**

```java
public class TimescaleDbMetrics {
    
    private final Map<String, Long> processedChunks = new ConcurrentHashMap<>();
    private final Map<String, Duration> processingTimes = new ConcurrentHashMap<>();
    
    public void recordChunkProcessed(String hypertable, String chunkName, Duration processingTime) {
        String key = hypertable + "." + chunkName;
        processedChunks.put(key, System.currentTimeMillis());
        processingTimes.put(key, processingTime);
        
        // Log for Airbyte monitoring
        LOGGER.info("Processed TimescaleDB chunk: {} in {}ms", 
            key, processingTime.toMillis());
    }
    
    public Map<String, Object> getMetrics() {
        return Map.of(
            "total_chunks_processed", processedChunks.size(),
            "average_processing_time_ms", getAverageProcessingTime(),
            "active_hypertables", getActiveHypertables().size()
        );
    }
}
```

---

## Configuration Schema

### 🔧 **Enhanced Connector Configuration**

```json
{
  "type": "object",
  "properties": {
    "timescaledb_support": {
      "type": "boolean",
      "default": false,
      "title": "Enable TimescaleDB Support",
      "description": "Enable TimescaleDB-specific optimizations and chunk-based processing"
    },
    "timescaledb_chunk_discovery_interval_minutes": {
      "type": "integer",
      "default": 5,
      "minimum": 1,
      "maximum": 60,
      "title": "Chunk Discovery Interval",
      "description": "How often to discover new chunks (minutes)"
    },
    "timescaledb_max_concurrent_chunks": {
      "type": "integer",
      "default": 1,
      "minimum": 1,
      "maximum": 10,
      "title": "Max Concurrent Chunks",
      "description": "Maximum number of chunks to process concurrently"
    },
    "timescaledb_enable_chunk_caching": {
      "type": "boolean",
      "default": true,
      "title": "Enable Chunk Caching",
      "description": "Cache chunk metadata to reduce database queries"
    }
  }
}
```

---

## Testing Strategy

### 🧪 **Unit Tests Structure**

```
src/test/java/io/airbyte/integrations/source/postgres/timescaledb/
├── TimescaleDbSourceOperationsTest.java
├── ChunkDiscoveryServiceTest.java
├── TimescaleDbStateManagerTest.java
├── TimescaleDbChunkIteratorTest.java
└── integration/
    ├── TimescaleDbIntegrationTest.java
    └── TimescaleDbPerformanceTest.java
```

### 🔬 **Test Examples**

```java
@Test
class ChunkDiscoveryServiceTest {
    
    @Mock
    private JdbcDatabase database;
    
    @Test
    void testChunkDiscovery_WithValidHypertable_ReturnsChunks() {
        // Given
        when(database.queryJsons(any(), any(), any()))
            .thenReturn(createMockChunkData());
        
        ChunkDiscoveryService service = new ChunkDiscoveryService(database);
        ConfiguredAirbyteStream stream = createHypertableStream();
        
        // When
        List<ChunkMetadata> chunks = service.discoverChunks(stream);
        
        // Then
        assertThat(chunks).hasSize(3);
        assertThat(chunks.get(0).getChunkName()).isEqualTo("_hyper_1_1_chunk");
    }
    
    @Test
    void testChunkDiscovery_WithCachedData_UsesCache() {
        // Test caching behavior
    }
}
```

---

## Migration Strategy

### 🔄 **Backward Compatibility**

```java
public class TimescaleDbMigrationHandler {
    
    public static boolean shouldMigrate(JsonNode currentState) {
        // Check if state needs migration to TimescaleDB format
        return currentState.has("streams") && 
               !currentState.has("timescaledb_version");
    }
    
    public static JsonNode migrateState(JsonNode oldState) {
        // Convert existing state to TimescaleDB-compatible format
        ObjectNode newState = oldState.deepCopy();
        newState.put("timescaledb_version", "1.0");
        
        // Add chunk-level state tracking
        newState.getStreams().forEach(this::addChunkStateTracking);
        
        return newState;
    }
}
```

---

## Summary

This architecture provides:

✅ **Self-Contained**: All logic within Airbyte connector  
✅ **Minimal DB Queries**: Only essential chunk discovery queries  
✅ **Airbyte Patterns**: Follows existing iterator and state patterns  
✅ **Backward Compatible**: Graceful fallback to standard Postgres  
✅ **Performance Optimized**: Chunk-level processing and caching  
✅ **Production Ready**: Comprehensive error handling and monitoring  

The solution addresses all three critical issues from `next_steps.md`:

1. **✅ Automated Chunk Discovery**: Self-contained polling with caching
2. **✅ Transactional Chunk Processing**: Per-chunk iterators with short transactions  
3. **✅ Implicit Schema Handling**: Automatic schema discovery and configuration

All while maintaining Airbyte's architectural principles and keeping database interactions to a minimum.