# TimescaleDB Integration - Complete User Guide

## 🎯 **Overview**

This guide provides comprehensive instructions for using the **TimescaleDB integration** in the Airbyte PostgreSQL source connector. The integration enables optimized processing of TimescaleDB hypertables with chunk-based data extraction, intelligent caching, and robust error handling.

---

## 🚀 **Quick Start**

### **1. Enable TimescaleDB Support**

In your Airbyte connection configuration:

```json
{
  "host": "your-timescaledb-host.com",
  "port": 5432,
  "database": "your_database",
  "username": "your_user", 
  "password": "your_password",
  "timescaledb_support": true
}
```

### **2. Automatic Detection**

Once enabled, the connector automatically:
- ✅ Detects TimescaleDB extension
- ✅ Identifies hypertables during discovery
- ✅ Processes hypertables using chunk-based optimization
- ✅ Falls back to standard processing for regular tables

### **3. Monitor Performance**

Check the connector logs for TimescaleDB-specific metrics:
```
INFO  TimescaleDB support enabled, found 3 hypertables
INFO  Processing hypertable metrics with 12 chunks
INFO  Chunk processing completed: 1.2M records in 45 seconds
```

---

## ⚙️ **Configuration Options**

### **Basic Configuration**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `timescaledb_support` | boolean | `false` | Enable TimescaleDB chunk-based processing |

### **Advanced Configuration**

| Option | Type | Default | Range | Description |
|--------|------|---------|-------|-------------|
| `timescaledb_chunk_discovery_interval_minutes` | integer | `5` | 1-60 | How often to refresh chunk metadata |
| `timescaledb_max_concurrent_chunks` | integer | `1` | 1-10 | Maximum chunks to process simultaneously |
| `timescaledb_enable_chunk_caching` | boolean | `true` | - | Cache chunk metadata to reduce DB queries |
| `timescaledb_chunk_cache_ttl_minutes` | integer | `10` | 1-120 | How long to cache chunk metadata |
| `timescaledb_max_memory_mb` | integer | `512` | 100-4096 | Maximum memory usage for chunk processing |

### **Configuration Examples**

#### **High-Performance Production**
```json
{
  "timescaledb_support": true,
  "timescaledb_chunk_discovery_interval_minutes": 10,
  "timescaledb_max_concurrent_chunks": 3,
  "timescaledb_enable_chunk_caching": true,
  "timescaledb_chunk_cache_ttl_minutes": 30,
  "timescaledb_max_memory_mb": 2048
}
```

#### **Resource-Constrained Environment**
```json
{
  "timescaledb_support": true,
  "timescaledb_chunk_discovery_interval_minutes": 15,
  "timescaledb_max_concurrent_chunks": 1,
  "timescaledb_enable_chunk_caching": false,
  "timescaledb_chunk_cache_ttl_minutes": 5,
  "timescaledb_max_memory_mb": 256
}
```

#### **Development/Testing**
```json
{
  "timescaledb_support": true,
  "timescaledb_chunk_discovery_interval_minutes": 2,
  "timescaledb_max_concurrent_chunks": 1,
  "timescaledb_enable_chunk_caching": true,
  "timescaledb_chunk_cache_ttl_minutes": 5,
  "timescaledb_max_memory_mb": 512
}
```

---

## 📊 **How It Works**

### **Discovery Phase**
1. **Extension Detection**: Checks for TimescaleDB extension
2. **Hypertable Identification**: Queries `_timescaledb_catalog.hypertable`
3. **Chunk Discovery**: Enumerates chunks with metadata
4. **Catalog Enhancement**: Adds TimescaleDB metadata to stream definitions

### **Processing Phase**
1. **Smart Routing**: Routes hypertables to chunk-based processing
2. **Chunk Iteration**: Processes chunks sequentially with separate transactions
3. **State Management**: Tracks progress per chunk for resumability
4. **Memory Management**: Monitors and controls memory usage
5. **Fallback Handling**: Falls back to standard processing on errors

### **CDC Integration**
1. **Schema Inclusion**: Automatically includes `_timescaledb_internal` schema
2. **SMT Configuration**: Configures TimescaleDB transform for event routing
3. **Event Mapping**: Maps chunk events to logical hypertable topics

---

## 📈 **Performance Benefits**

### **Memory Efficiency**
- **Before**: Memory usage scales with table size
- **After**: Memory usage bounded by chunk size (typically 10-100MB)

### **Processing Optimization**
- **Before**: Single large transaction per table
- **After**: Multiple smaller transactions per chunk

### **Resumability**
- **Before**: Restart from beginning on failure
- **After**: Resume from last completed chunk

### **Database Load**
- **Before**: Long-running locks on large tables
- **After**: Short-lived locks on individual chunks

---

## 🔍 **Monitoring & Observability**

### **Key Metrics**

The connector provides comprehensive metrics for TimescaleDB processing:

```
INFO  TimescaleDB Metrics Summary:
  - Chunks Processed: 24
  - Records Processed: 2,450,000
  - Processing Time: 3m 45s
  - Average Chunk Time: 9.4s
  - Memory Usage: 384MB (peak: 512MB)
  - Throughput: 10,889 records/sec
  - Cache Hit Ratio: 87%
```

### **Log Levels**

Enable different log levels for troubleshooting:

```properties
# Basic TimescaleDB logging
logging.level.io.airbyte.integrations.source.postgres.timescaledb=INFO

# Detailed chunk processing
logging.level.io.airbyte.integrations.source.postgres.timescaledb.chunking=DEBUG

# Performance metrics
logging.level.io.airbyte.integrations.source.postgres.timescaledb.utils.TimescaleDbMetrics=DEBUG
```

### **Performance Indicators**

| Metric | Good | Warning | Critical |
|--------|------|---------|----------|
| Cache Hit Ratio | >80% | 60-80% | <60% |
| Memory Usage | <70% of limit | 70-90% | >90% |
| Chunk Processing Time | <30s | 30-60s | >60s |
| Error Rate | <1% | 1-5% | >5% |

---

## 🔧 **Troubleshooting**

### **Common Issues**

#### **1. TimescaleDB Extension Not Found**
```
ERROR: TimescaleDB extension not available
```

**Solution:**
```sql
-- Install TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Verify installation
SELECT * FROM pg_extension WHERE extname = 'timescaledb';
```

#### **2. Permission Denied for Internal Schema**
```
ERROR: Permission denied for schema _timescaledb_internal
```

**Solution:**
```sql
-- Grant necessary permissions
GRANT USAGE ON SCHEMA _timescaledb_internal TO your_user;
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_internal TO your_user;

-- For future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_internal 
GRANT SELECT ON TABLES TO your_user;
```

#### **3. Memory Limit Exceeded**
```
WARN: Memory usage exceeded configured limit (512MB)
```

**Solutions:**
```json
// Option 1: Increase memory limit
{
  "timescaledb_max_memory_mb": 1024
}

// Option 2: Reduce concurrent chunks
{
  "timescaledb_max_concurrent_chunks": 1
}

// Option 3: Enable more aggressive caching
{
  "timescaledb_enable_chunk_caching": true,
  "timescaledb_chunk_cache_ttl_minutes": 30
}
```

#### **4. Chunk Discovery Failures**
```
ERROR: Failed to discover chunks for hypertable metrics
```

**Solutions:**
```sql
-- Check hypertable status
SELECT * FROM timescaledb_information.hypertables 
WHERE hypertable_name = 'metrics';

-- Check chunk information
SELECT * FROM timescaledb_information.chunks 
WHERE hypertable_name = 'metrics';

-- Verify user permissions
SELECT has_table_privilege('your_user', '_timescaledb_catalog.hypertable', 'SELECT');
```

#### **5. CDC Schema Inclusion Issues**
```
ERROR: Missing catalog stream for _timescaledb_internal._hyper_1_2_chunk
```

**Solution:**
This is automatically handled by the integration. If you see this error:
1. Ensure `timescaledb_support` is enabled
2. Check that the publication includes the hypertable
3. Verify CDC configuration is correct

### **Debug Mode**

Enable comprehensive debug logging:

```properties
# Enable all TimescaleDB debug logging
logging.level.io.airbyte.integrations.source.postgres.timescaledb=DEBUG

# Enable SQL query logging
logging.level.io.airbyte.integrations.source.postgres.timescaledb.utils.TimescaleDbQueries=TRACE

# Enable chunk discovery debugging
logging.level.io.airbyte.integrations.source.postgres.timescaledb.chunking.ChunkDiscoveryService=DEBUG
```

---

## 🏗️ **Best Practices**

### **Configuration Tuning**

#### **For Large Hypertables (>1GB chunks)**
```json
{
  "timescaledb_chunk_discovery_interval_minutes": 15,
  "timescaledb_max_concurrent_chunks": 1,
  "timescaledb_max_memory_mb": 2048
}
```

#### **For Many Small Hypertables**
```json
{
  "timescaledb_chunk_discovery_interval_minutes": 5,
  "timescaledb_max_concurrent_chunks": 3,
  "timescaledb_max_memory_mb": 1024
}
```

#### **For High-Frequency Updates**
```json
{
  "timescaledb_chunk_discovery_interval_minutes": 2,
  "timescaledb_enable_chunk_caching": true,
  "timescaledb_chunk_cache_ttl_minutes": 5
}
```

### **Database Optimization**

#### **Recommended TimescaleDB Settings**
```sql
-- Optimize chunk time intervals
SELECT set_chunk_time_interval('metrics', INTERVAL '1 hour');

-- Enable compression for older chunks
ALTER TABLE metrics SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'device_id'
);

-- Create compression policy
SELECT add_compression_policy('metrics', INTERVAL '7 days');
```

#### **Index Optimization**
```sql
-- Create indexes on frequently queried columns
CREATE INDEX idx_metrics_device_time ON metrics (device_id, time DESC);

-- Create partial indexes for recent data
CREATE INDEX idx_metrics_recent ON metrics (time DESC) 
WHERE time > NOW() - INTERVAL '1 day';
```

### **Monitoring Setup**

#### **Key Metrics to Track**
1. **Chunk Processing Time**: Should be consistent across chunks
2. **Memory Usage**: Should stay within configured limits
3. **Cache Hit Ratio**: Should be >80% for optimal performance
4. **Error Rate**: Should be <1% under normal conditions

#### **Alerting Thresholds**
```yaml
alerts:
  - name: "TimescaleDB High Memory Usage"
    condition: memory_usage > 90% of limit
    action: "Reduce concurrent chunks or increase memory limit"
    
  - name: "TimescaleDB Low Cache Hit Ratio" 
    condition: cache_hit_ratio < 60%
    action: "Increase cache TTL or check query patterns"
    
  - name: "TimescaleDB Slow Chunk Processing"
    condition: avg_chunk_time > 60 seconds
    action: "Check database performance and chunk sizes"
```

---

## 🔄 **Migration Guide**

### **Enabling TimescaleDB Support on Existing Connections**

1. **Backup Current State**: Ensure you have a backup of your sync state
2. **Enable Support**: Add `"timescaledb_support": true` to configuration
3. **Test Discovery**: Run discovery to verify hypertable detection
4. **Monitor First Sync**: Watch logs for any issues during first sync
5. **Optimize Settings**: Tune configuration based on performance

### **Disabling TimescaleDB Support**

1. **Set Support to False**: Change `"timescaledb_support": false`
2. **Clear Cache**: The system will automatically fall back to standard processing
3. **Monitor Performance**: Standard processing may use more memory for large tables

---

## 🚀 **Advanced Features**

### **Custom Chunk Strategies**

The architecture supports custom chunk processing strategies:

```java
// Example: Custom chunk discovery for specific use cases
public class CustomChunkDiscoveryService extends ChunkDiscoveryService {
  @Override
  public List<ChunkMetadata> discoverChunks(ConfiguredAirbyteStream stream) {
    // Custom logic for chunk discovery
    return super.discoverChunks(stream);
  }
}
```

### **Performance Monitoring Integration**

```java
// Access performance metrics programmatically
TimescaleDbMetrics metrics = sourceOperations.getMetrics();
Map<String, Object> summary = metrics.getSummary();

// Custom metrics collection
metrics.recordCustomMetric("chunk_size_distribution", chunkSizes);
```

### **State Management Customization**

```java
// Custom state management for specific requirements
public class CustomTimescaleDbStateManager extends TimescaleDbStateManager {
  @Override
  public void updateState(String streamName, ChunkMetadata completedChunk) {
    // Custom state update logic
    super.updateState(streamName, completedChunk);
  }
}
```

---

## 📚 **Reference**

### **SQL Queries Used**

The integration uses these optimized queries:

```sql
-- Check TimescaleDB extension
SELECT EXISTS(
  SELECT 1 FROM pg_extension 
  WHERE extname = 'timescaledb'
) as timescaledb_available;

-- Discover hypertables
SELECT 
  h.schema_name,
  h.table_name,
  h.num_dimensions,
  d.column_name as time_column_name,
  d.interval_length as chunk_time_interval
FROM _timescaledb_catalog.hypertable h
LEFT JOIN _timescaledb_catalog.dimension d ON h.id = d.hypertable_id
WHERE d.dimension_type = 1;

-- Discover chunks
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
ORDER BY c.range_start;
```

### **Configuration Schema Reference**

Complete JSON schema for TimescaleDB configuration:

```json
{
  "timescaledb_support": {
    "type": "boolean",
    "default": false,
    "description": "Enable TimescaleDB support"
  },
  "timescaledb_chunk_discovery_interval_minutes": {
    "type": "integer",
    "default": 5,
    "minimum": 1,
    "maximum": 60
  },
  "timescaledb_max_concurrent_chunks": {
    "type": "integer",
    "default": 1,
    "minimum": 1,
    "maximum": 10
  },
  "timescaledb_enable_chunk_caching": {
    "type": "boolean",
    "default": true
  },
  "timescaledb_chunk_cache_ttl_minutes": {
    "type": "integer",
    "default": 10,
    "minimum": 1,
    "maximum": 120
  },
  "timescaledb_max_memory_mb": {
    "type": "integer",
    "default": 512,
    "minimum": 100,
    "maximum": 4096
  }
}
```

---

## 🆘 **Support**

### **Getting Help**

1. **Check Logs**: Enable debug logging for detailed information
2. **Review Configuration**: Verify all settings are within valid ranges
3. **Test Permissions**: Ensure database user has necessary privileges
4. **Monitor Resources**: Check memory and CPU usage during processing

### **Common Solutions**

| Issue | Quick Fix |
|-------|-----------|
| High memory usage | Reduce `timescaledb_max_concurrent_chunks` to 1 |
| Slow processing | Increase `timescaledb_max_memory_mb` |
| Permission errors | Grant SELECT on `_timescaledb_internal` schema |
| Cache misses | Increase `timescaledb_chunk_cache_ttl_minutes` |

### **Performance Optimization Checklist**

- ✅ TimescaleDB extension installed and up-to-date
- ✅ Appropriate chunk time intervals configured
- ✅ Indexes on time columns and frequently queried dimensions
- ✅ Compression policies for older data
- ✅ Database statistics up-to-date (`ANALYZE` run recently)
- ✅ Sufficient memory allocated to PostgreSQL
- ✅ Network latency between Airbyte and database minimized

---

## 🎯 **Summary**

The TimescaleDB integration provides:

✅ **Automatic Detection**: Zero-configuration hypertable detection  
✅ **Optimized Processing**: Chunk-based processing for better performance  
✅ **Robust Error Handling**: Graceful fallback to standard PostgreSQL processing  
✅ **Comprehensive Monitoring**: Detailed metrics and observability  
✅ **Production Ready**: Enterprise-grade configuration and validation  

The integration is designed to be **self-contained**, **performant**, and **reliable**, providing optimal time-series data processing while maintaining full compatibility with existing PostgreSQL workflows.

**For additional support or advanced use cases, refer to the implementation documentation and source code.** 