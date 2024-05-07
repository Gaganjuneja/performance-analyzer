/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.performanceanalyzer.transport;

import com.sun.management.ThreadMXBean;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.performanceanalyzer.commons.collectors.StatsCollector;
import org.opensearch.performanceanalyzer.commons.metrics.MetricsProcessor;
import org.opensearch.performanceanalyzer.commons.metrics.PerformanceAnalyzerMetrics;
import org.opensearch.performanceanalyzer.commons.os.OSGlobals;
import org.opensearch.performanceanalyzer.commons.os.ThreadDiskIO;
import org.opensearch.performanceanalyzer.commons.stats.metrics.StatExceptionCode;
import org.opensearch.performanceanalyzer.commons.util.ThreadIDUtil;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.noop.NoopCounter;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.transport.TransportChannel;

public class PerformanceAnalyzerTransportChannel implements TransportChannel, MetricsProcessor {
    private static final Logger LOG =
            LogManager.getLogger(PerformanceAnalyzerTransportChannel.class);
    private static final int KEYS_PATH_LENGTH = 3;
    private static final String PID = OSGlobals.getPid();
    private static final AtomicLong UNIQUE_ID = new AtomicLong(0);

    private static Map<String, Long> startValue;
    private static final Logger LOGGER = LogManager.getLogger(ThreadDiskIO.class);

    private TransportChannel original;
    private String indexName;
    private int shardId;
    private boolean primary;
    private String id;
    private String threadID;
    private Counter throughputCounter;
    private Long startCpuTimeNanos;
    private long jvmThreadID;

    void set(
            TransportChannel original,
            long startTime,
            String indexName,
            int shardId,
            int itemCount,
            boolean bPrimary,
            Counter throughputCounter) {
        this.original = original;
        this.id = String.valueOf(UNIQUE_ID.getAndIncrement());
        this.indexName = indexName;
        this.shardId = shardId;
        this.primary = bPrimary;
        this.threadID = String.valueOf(ThreadIDUtil.INSTANCE.getNativeCurrentThreadId());
        this.startValue = getIOUsage(threadID);
        this.throughputCounter = throughputCounter;
        this.startCpuTimeNanos = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
        jvmThreadID = Thread.currentThread().getId();
        LOG.info("Updating the counter Start thread {} {}", jvmThreadID, Thread.currentThread().getName());
    }

    @Override
    public String getProfileName() {
        return "PerformanceAnalyzerTransportChannelProfile";
    }

    @Override
    public String getChannelType() {
        return "PerformanceAnalyzerTransportChannelType";
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        LOG.info("Updating the counter Finish thread {}", Thread.currentThread().getName());
        if (throughputCounter != null && !(throughputCounter instanceof NoopCounter)) {
            long timeNow = ((ThreadMXBean)ManagementFactory.getThreadMXBean()).getThreadCpuTime(jvmThreadID);
            long updatedValue = timeNow - startCpuTimeNanos;
            LOG.info("Updating the counter inside PATransportChannel {} start time {}, now {}", updatedValue, startCpuTimeNanos, timeNow);
            throughputCounter.add(
                    Math.max(updatedValue, 0),
                    Tags.create().addTag("indexName", indexName).addTag("shardId", shardId));
        }
        emitMetricsFinish(null);
        original.sendResponse(response);
    }

    private Map<String, Long> getIOUsage(String tid) {
        try (FileReader fileReader =
                        new FileReader(new File("/proc/" + PID + "/task/" + tid + "/io"));
                BufferedReader bufferedReader = new BufferedReader(fileReader); ) {
            String line = null;
            Map<String, Long> kvmap = new HashMap<>();
            while ((line = bufferedReader.readLine()) != null) {
                String[] toks = line.split("[: ]+");
                String key = toks[0];
                long val = Long.parseLong(toks[1]);
                kvmap.put(key, val);
            }
            return kvmap;
        } catch (FileNotFoundException e) {
            LOGGER.debug("FileNotFound in parse with exception: {}", () -> e.toString());
        } catch (Exception e) {
            LOGGER.debug(
                    "Error In addSample PID for: {} Tid for: {}  with error: {} with ExceptionCode: {}",
                    () -> PID,
                    () -> tid,
                    () -> e.toString(),
                    () -> StatExceptionCode.THREAD_IO_ERROR.toString());
            StatsCollector.instance().logException(StatExceptionCode.THREAD_IO_ERROR);
        }
        return null;
    }

    private double getIOUtilization(Map<String, Long> v, Map<String, Long> oldv) {
        if (v != null && oldv != null) {
            return v.get("write_bytes") - oldv.get("write_bytes");
        } else if (v != null) {
            return v.get("write_bytes");
        } else {
            return 0.0;
        }
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        emitMetricsFinish(exception);
        original.sendResponse(exception);
    }

    private void emitMetricsFinish(Exception exception) {
        if (exception != null) {
            Map<String, Long> finishValue = getIOUsage(threadID);
            double writeThroughput = getIOUtilization(finishValue, startValue);
            if (writeThroughput >= 0.0) {
                throughputCounter.add(
                        writeThroughput,
                        Tags.create()
                                .addTag("IndexName", indexName)
                                .addTag("ShardId", shardId)
                                .addTag("Operation", "shardbulk"));
            }
        }
    }

    // This function is called from the security plugin using reflection. Do not
    // remove this function without changing the security plugin.
    public TransportChannel getInnerChannel() {
        return this.original;
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keys.length is not equal to 3 (Keys should be threadID, ShardBulkId,
        // start/finish)
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }
        return PerformanceAnalyzerMetrics.generatePath(
                startTime,
                PerformanceAnalyzerMetrics.sThreadsPath,
                keysPath[0],
                PerformanceAnalyzerMetrics.sShardBulkPath,
                keysPath[1],
                keysPath[2]);
    }
}
