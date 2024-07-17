/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.performanceanalyzer.listener;

import static org.opensearch.performanceanalyzer.commons.stats.metrics.StatExceptionCode.OPENSEARCH_REQUEST_INTERCEPTOR_ERROR;

import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.NotifyOnceListener;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.performanceanalyzer.OpenSearchResources;
import org.opensearch.performanceanalyzer.commons.collectors.StatsCollector;
import org.opensearch.performanceanalyzer.commons.metrics.RTFMetrics;
import org.opensearch.performanceanalyzer.commons.os.OSGlobals;
import org.opensearch.performanceanalyzer.commons.util.Util;
import org.opensearch.performanceanalyzer.config.PerformanceAnalyzerController;
import org.opensearch.performanceanalyzer.util.Utils;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

public class RTFPerformanceAnalyzerSearchListener
        implements SearchOperationListener, SearchListener {

    private static final Logger LOG =
            LogManager.getLogger(RTFPerformanceAnalyzerSearchListener.class);
    private static final String OPERATION_SHARD_FETCH = "shard_fetch";
    private static final String OPERATION_SHARD_QUERY = "shard_query";
    public static final String QUERY_CPU_START_TIME = "query_cpu";
    public static final String QUERY_START_TIME = "query_start_time";
    public static final String FETCH_CPU_START_TIME = "fetch_cpu";
    public static final String FETCH_START_TIME = "fetch_start_time";
    private final ThreadLocal<Map<String, Long>> threadLocal = new ThreadLocal<>();
    private static final SearchListener NO_OP_SEARCH_LISTENER = new NoOpSearchListener();
    private static final ThreadMXBean threadMXBean =
            (ThreadMXBean) ManagementFactory.getThreadMXBean();
    private final long scClkTck;
    private final PerformanceAnalyzerController controller;
    private Histogram cpuUtilizationHistogram;

    public RTFPerformanceAnalyzerSearchListener(final PerformanceAnalyzerController controller) {
        this.controller = controller;
        this.scClkTck = OSGlobals.getScClkTck();
        this.cpuUtilizationHistogram = createCPUUtilizationHistogram();
    }

    private Histogram createCPUUtilizationHistogram() {
        MetricsRegistry metricsRegistry = OpenSearchResources.INSTANCE.getMetricsRegistry();
        if (metricsRegistry != null) {
            return metricsRegistry.createHistogram(
                    RTFMetrics.OSMetrics.CPU_UTILIZATION.toString(),
                    "CPU Utilization per shard for an operation",
                    "rate");
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return RTFPerformanceAnalyzerSearchListener.class.getSimpleName();
    }

    private SearchListener getSearchListener() {
        return isSearchListenerEnabled() ? this : NO_OP_SEARCH_LISTENER;
    }

    private boolean isSearchListenerEnabled() {
        return cpuUtilizationHistogram != null
                && controller.isPerformanceAnalyzerEnabled()
                && (controller.getCollectorsSettingValue() == Util.CollectorMode.DUAL.getValue()
                        || controller.getCollectorsSettingValue()
                                == Util.CollectorMode.TELEMETRY.getValue());
    }


    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        try {
            getSearchListener().preQueryPhase(searchContext);
        } catch (Exception ex) {
            LOG.error(ex);
            StatsCollector.instance().logException(OPENSEARCH_REQUEST_INTERCEPTOR_ERROR);
        }
    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        try {
            getSearchListener().queryPhase(searchContext, tookInNanos);
        } catch (Exception ex) {
            LOG.error(ex);
            StatsCollector.instance().logException(OPENSEARCH_REQUEST_INTERCEPTOR_ERROR);
        }
    }

    @Override
    public void onFailedQueryPhase(SearchContext searchContext) {
        try {
            getSearchListener().failedQueryPhase(searchContext);
        } catch (Exception ex) {
            LOG.error(ex);
            StatsCollector.instance().logException(OPENSEARCH_REQUEST_INTERCEPTOR_ERROR);
        }
    }

    @Override
    public void onPreFetchPhase(SearchContext searchContext) {
        try {
            getSearchListener().preFetchPhase(searchContext);
        } catch (Exception ex) {
            LOG.error(ex);
            StatsCollector.instance().logException(OPENSEARCH_REQUEST_INTERCEPTOR_ERROR);
        }
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        try {
            getSearchListener().fetchPhase(searchContext, tookInNanos);
        } catch (Exception ex) {
            LOG.error(ex);
            StatsCollector.instance().logException(OPENSEARCH_REQUEST_INTERCEPTOR_ERROR);
        }
    }

    @Override
    public void onFailedFetchPhase(SearchContext searchContext) {
        try {
            getSearchListener().failedFetchPhase(searchContext);
        } catch (Exception ex) {
            LOG.error(ex);
            StatsCollector.instance().logException(OPENSEARCH_REQUEST_INTERCEPTOR_ERROR);
        }
    }

    @Override
    public void preQueryPhase(SearchContext searchContext) {
        threadLocal.get().put(QUERY_START_TIME, System.nanoTime());
    }

    @Override
    public void queryPhase(SearchContext searchContext, long tookInNanos) {
        long queryStartTime = threadLocal.get().getOrDefault(QUERY_START_TIME, 0l);
        addCPUResourceTrackingCompletionListener(
                searchContext, queryStartTime, OPERATION_SHARD_FETCH, false);
    }

    @Override
    public void failedQueryPhase(SearchContext searchContext) {
        long queryStartTime = threadLocal.get().getOrDefault(QUERY_START_TIME, 0l);
        addCPUResourceTrackingCompletionListener(
                searchContext, queryStartTime, OPERATION_SHARD_FETCH, true);
    }

    @Override
    public void preFetchPhase(SearchContext searchContext) {
        threadLocal.get().put(FETCH_START_TIME, System.nanoTime());
    }

    @Override
    public void fetchPhase(SearchContext searchContext, long tookInNanos) {
        long fetchStartTime = threadLocal.get().getOrDefault(FETCH_START_TIME, 0l);
        addCPUResourceTrackingCompletionListener(
                searchContext, fetchStartTime, OPERATION_SHARD_FETCH, false);
    }

    @Override
    public void failedFetchPhase(SearchContext searchContext) {
        long fetchStartTime = threadLocal.get().getOrDefault(FETCH_START_TIME, 0l);
        addCPUResourceTrackingCompletionListener(
                searchContext, fetchStartTime, OPERATION_SHARD_FETCH, true);
    }

    private double calculateCPUUtilization(long phaseStartTime, long phaseCPUStartTime) {
        long totalCpuTime =
                Math.min(0, (threadMXBean.getCurrentThreadCpuTime() - phaseCPUStartTime));
        return Utils.calculateCPUUtilization(
                totalCpuTime, scClkTck, phaseStartTime - System.nanoTime());
    }

    private void recordCPUUtilizationMetric(
            SearchContext searchContext, double cpuUtilization, String operation) {
        cpuUtilizationHistogram.record(
                cpuUtilization,
                Tags.create()
                        .addTag(
                                RTFMetrics.CommonDimension.SHARD_ID.toString(),
                                searchContext.shardTarget().getShardId().getId())
                        .addTag(
                                RTFMetrics.CommonDimension.INDEX_NAME.toString(),
                                searchContext.shardTarget().getShardId().getIndex().getName())
                        .addTag(
                                RTFMetrics.CommonDimension.INDEX_UUID.toString(),
                                searchContext.shardTarget().getShardId().getIndex().getUUID())
                        .addTag(RTFMetrics.CommonDimension.OPERATION.toString(), operation));
    }

    private NotifyOnceListener<Task> addCPUResourceTrackingCompletionListener(
            SearchContext searchContext, long startTime, String operation, boolean isFailed) {
        return new NotifyOnceListener<Task>() {
            @Override
            protected void innerOnResponse(Task task) {
                LOG.debug("Updating the counter for task {}", task.getId());
                cpuUtilizationHistogram.record(
                        Utils.calculateCPUUtilization(
                                task.getTotalResourceStats().getCpuTimeInNanos(),
                                scClkTck,
                                (System.nanoTime() - startTime)),
                        Tags.create()
                                .addTag(
                                        RTFMetrics.CommonDimension.INDEX_NAME.toString(),
                                        searchContext.shardTarget().getShardId().getIndexName())
                                .addTag(
                                        RTFMetrics.CommonDimension.INDEX_UUID.toString(),
                                        searchContext
                                                .shardTarget()
                                                .getShardId()
                                                .getIndex()
                                                .getUUID())
                                .addTag(
                                        RTFMetrics.CommonDimension.SHARD_ID.toString(),
                                        searchContext.shardTarget().getShardId().getId())
                                .addTag(RTFMetrics.CommonDimension.OPERATION.toString(), operation)
                                .addTag(RTFMetrics.CommonDimension.FAILED.toString(), isFailed));
            }

            @Override
            protected void innerOnFailure(Exception e) {}
        };
    }
}
