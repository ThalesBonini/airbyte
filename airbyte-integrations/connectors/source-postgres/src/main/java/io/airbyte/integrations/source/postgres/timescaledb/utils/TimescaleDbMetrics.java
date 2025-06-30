/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.timescaledb.utils;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics collection and monitoring for TimescaleDB operations.
 * Provides observability into chunk processing performance and resource usage.
 */
public class TimescaleDbMetrics {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDbMetrics.class);

  private final Map<String, Long> processedChunks = new ConcurrentHashMap<>();
  private final Map<String, Duration> processingTimes = new ConcurrentHashMap<>();
  private final Map<String, Long> chunkSizes = new ConcurrentHashMap<>();
  private final Map<String, Long> recordCounts = new ConcurrentHashMap<>();
  private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);
  private final AtomicLong totalChunksProcessed = new AtomicLong(0);
  private final AtomicLong totalRecordsProcessed = new AtomicLong(0);
  private final AtomicLong totalBytesProcessed = new AtomicLong(0);
  private final AtomicReference<Instant> startTime = new AtomicReference<>(Instant.now());

  /**
   * Records successful chunk processing.
   */
  public void recordChunkProcessed(final String hypertable, 
                                  final String chunkName, 
                                  final Duration processingTime,
                                  final long recordCount,
                                  final long sizeBytes) {
    final String key = createChunkKey(hypertable, chunkName);
    
    processedChunks.put(key, System.currentTimeMillis());
    processingTimes.put(key, processingTime);
    chunkSizes.put(key, sizeBytes);
    recordCounts.put(key, recordCount);
    
    // Update totals
    totalProcessingTimeMs.addAndGet(processingTime.toMillis());
    totalChunksProcessed.incrementAndGet();
    totalRecordsProcessed.addAndGet(recordCount);
    totalBytesProcessed.addAndGet(sizeBytes);
    
    // Log for Airbyte monitoring
    LOGGER.info("Processed TimescaleDB chunk: {} in {}ms, {} records, {} bytes", 
        key, processingTime.toMillis(), recordCount, sizeBytes);
    
    // Log summary every 10 chunks
    if (totalChunksProcessed.get() % 10 == 0) {
      logProgressSummary();
    }
  }

  /**
   * Records chunk processing failure.
   */
  public void recordChunkFailure(final String hypertable, 
                                final String chunkName, 
                                final Exception error) {
    final String key = createChunkKey(hypertable, chunkName);
    
    LOGGER.error("Failed to process TimescaleDB chunk: {} - Error: {}", key, error.getMessage());
    
    // Could extend to track failure metrics if needed
  }

  /**
   * Records chunk discovery operation.
   */
  public void recordChunkDiscovery(final String hypertable, 
                                  final int chunksDiscovered, 
                                  final Duration discoveryTime) {
    LOGGER.info("Discovered {} chunks for hypertable {} in {}ms", 
        chunksDiscovered, hypertable, discoveryTime.toMillis());
  }

  /**
   * Records memory usage tracking.
   */
  public void recordMemoryUsage(final long usedMemoryBytes, final long maxMemoryBytes) {
    final double usagePercentage = (double) usedMemoryBytes / maxMemoryBytes * 100;
    
    if (usagePercentage > 80) {
      LOGGER.warn("High memory usage: {}% ({} MB / {} MB)", 
          String.format("%.1f", usagePercentage),
          usedMemoryBytes / (1024 * 1024),
          maxMemoryBytes / (1024 * 1024));
    } else if (usagePercentage > 60) {
      LOGGER.info("Memory usage: {}% ({} MB / {} MB)", 
          String.format("%.1f", usagePercentage),
          usedMemoryBytes / (1024 * 1024),
          maxMemoryBytes / (1024 * 1024));
    }
  }

  /**
   * Gets comprehensive metrics map for reporting.
   */
  public Map<String, Object> getMetrics() {
    final Map<String, Object> metrics = new HashMap<>();
    final long totalChunks = totalChunksProcessed.get();
    final Duration totalRuntime = Duration.between(startTime.get(), Instant.now());
    
    metrics.put("total_chunks_processed", totalChunks);
    metrics.put("total_records_processed", totalRecordsProcessed.get());
    metrics.put("total_bytes_processed", totalBytesProcessed.get());
    metrics.put("total_processing_time_ms", totalProcessingTimeMs.get());
    metrics.put("total_runtime_ms", totalRuntime.toMillis());
    metrics.put("average_processing_time_ms", getAverageProcessingTimeMs());
    metrics.put("active_hypertables", getActiveHypertables().size());
    metrics.put("throughput_chunks_per_minute", calculateChunkThroughput(totalRuntime));
    metrics.put("throughput_records_per_second", calculateRecordThroughput(totalRuntime));
    metrics.put("throughput_mb_per_second", calculateMBThroughput(totalRuntime));
    
    return metrics;
  }

  /**
   * Gets performance summary for logging.
   */
  public String getPerformanceSummary() {
    final Map<String, Object> metrics = getMetrics();
    return String.format(
        "TimescaleDB Performance Summary: %d chunks processed, %.1f avg ms/chunk, " +
        "%.1f chunks/min, %.1f records/sec, %.2f MB/sec",
        metrics.get("total_chunks_processed"),
        metrics.get("average_processing_time_ms"),
        metrics.get("throughput_chunks_per_minute"),
        metrics.get("throughput_records_per_second"),
        metrics.get("throughput_mb_per_second")
    );
  }

  /**
   * Resets all metrics (useful for testing or restarting measurements).
   */
  public void reset() {
    processedChunks.clear();
    processingTimes.clear();
    chunkSizes.clear();
    recordCounts.clear();
    totalProcessingTimeMs.set(0);
    totalChunksProcessed.set(0);
    totalRecordsProcessed.set(0);
    totalBytesProcessed.set(0);
    startTime.set(Instant.now());
    
    LOGGER.info("TimescaleDB metrics reset");
  }

  private String createChunkKey(final String hypertable, final String chunkName) {
    return hypertable + "." + chunkName;
  }

  private double getAverageProcessingTimeMs() {
    final long totalChunks = totalChunksProcessed.get();
    if (totalChunks == 0) {
      return 0.0;
    }
    return (double) totalProcessingTimeMs.get() / totalChunks;
  }

  private Map<String, Integer> getActiveHypertables() {
    final Map<String, Integer> hypertables = new HashMap<>();
    
    for (final String chunkKey : processedChunks.keySet()) {
      final String hypertable = extractHypertableFromKey(chunkKey);
      hypertables.merge(hypertable, 1, Integer::sum);
    }
    
    return hypertables;
  }

  private String extractHypertableFromKey(final String chunkKey) {
    final int lastDotIndex = chunkKey.lastIndexOf('.');
    return lastDotIndex > 0 ? chunkKey.substring(0, lastDotIndex) : chunkKey;
  }

  private double calculateChunkThroughput(final Duration totalRuntime) {
    if (totalRuntime.toMinutes() == 0) {
      return 0.0;
    }
    return (double) totalChunksProcessed.get() / totalRuntime.toMinutes();
  }

  private double calculateRecordThroughput(final Duration totalRuntime) {
    if (totalRuntime.toSeconds() == 0) {
      return 0.0;
    }
    return (double) totalRecordsProcessed.get() / totalRuntime.toSeconds();
  }

  private double calculateMBThroughput(final Duration totalRuntime) {
    if (totalRuntime.toSeconds() == 0) {
      return 0.0;
    }
    final double mbProcessed = (double) totalBytesProcessed.get() / (1024 * 1024);
    return mbProcessed / totalRuntime.toSeconds();
  }

  private void logProgressSummary() {
    LOGGER.info("TimescaleDB Progress: {}", getPerformanceSummary());
  }

} 