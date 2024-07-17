/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.performanceanalyzer.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.core.action.NotifyOnceListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.performanceanalyzer.OpenSearchResources;
import org.opensearch.performanceanalyzer.commons.stats.metrics.StatExceptionCode;
import org.opensearch.performanceanalyzer.commons.util.Util;
import org.opensearch.performanceanalyzer.config.PerformanceAnalyzerController;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

public class RTFPerformanceAnalyzerSearchListenerTests {
    private static final long TOOK_IN_NANOS = 10;
    private static final String EXCEPTION =
            StatExceptionCode.OPENSEARCH_REQUEST_INTERCEPTOR_ERROR.toString();

    private RTFPerformanceAnalyzerSearchListener searchListener;

    @Mock private SearchContext searchContext;
    @Mock private ShardSearchRequest shardSearchRequest;
    @Mock private ShardId shardId;
    @Mock private PerformanceAnalyzerController controller;
    @Mock private SearchShardTask task;
    @Mock private MetricsRegistry metricsRegistry;
    @Mock private Histogram histogram;
    @Mock private Index index;

    @Mock private TaskResourceUsage taskResourceUsage;

    @BeforeClass
    public static void setup() {
        // this test only runs in Linux system
        // as some of the static members of the ThreadList class are specific to Linux
        org.junit.Assume.assumeTrue(SystemUtils.IS_OS_LINUX);
    }

    @Before
    public void init() {
        initMocks(this);
        Mockito.when(controller.isPerformanceAnalyzerEnabled()).thenReturn(true);
        searchListener = new RTFPerformanceAnalyzerSearchListener(controller);
        OpenSearchResources.INSTANCE.setMetricsRegistry(metricsRegistry);
        assertEquals(
                RTFPerformanceAnalyzerSearchListener.class.getSimpleName(),
                searchListener.toString());
    }

    @Test
    public void tesSearchListener() {
        Mockito.when(controller.getCollectorsSettingValue())
                .thenReturn(Util.CollectorMode.TELEMETRY.getValue());
        assertTrue(searchListener.getSearchListener() instanceof NoOpSearchListener);

        Mockito.when(controller.getCollectorsSettingValue())
                .thenReturn(Util.CollectorMode.RCA.getValue());
        assertTrue(searchListener.getSearchListener() instanceof PerformanceAnalyzerSearchListener);

        Mockito.when(controller.getCollectorsSettingValue())
                .thenReturn(Util.CollectorMode.DUAL.getValue());
        assertTrue(searchListener.getSearchListener() instanceof PerformanceAnalyzerSearchListener);
    }

    @Test
    public void testQueryPhase() {
        initializeValidSearchContext(true);
        Mockito.when(controller.getCollectorsSettingValue())
                .thenReturn(Util.CollectorMode.TELEMETRY.getValue());
        Mockito.when(
                        metricsRegistry.createHistogram(
                                Mockito.eq("CPU_Utilization"),
                                Mockito.anyString(),
                                Mockito.eq("rate")))
                .thenReturn(histogram);
        searchListener.preQueryPhase(searchContext);
        searchListener.queryPhase(searchContext, 0l);
        Mockito.verify(task).addResourceTrackingCompletionListener(Mockito.any());
    }

    @Test
    public void testQueryPhaseFailed() {
        initializeValidSearchContext(true);
        Mockito.when(controller.getCollectorsSettingValue())
                .thenReturn(Util.CollectorMode.TELEMETRY.getValue());
        Mockito.when(
                        metricsRegistry.createHistogram(
                                Mockito.eq("CPU_Utilization"),
                                Mockito.anyString(),
                                Mockito.eq("rate")))
                .thenReturn(histogram);
        searchListener.preQueryPhase(searchContext);
        searchListener.failedQueryPhase(searchContext);
        Mockito.verify(task).addResourceTrackingCompletionListener(Mockito.any());
    }

    @Test
    public void testFetchPhase() {
        initializeValidSearchContext(true);
        Mockito.when(controller.getCollectorsSettingValue())
                .thenReturn(Util.CollectorMode.TELEMETRY.getValue());
        Mockito.when(
                        metricsRegistry.createHistogram(
                                Mockito.eq("CPU_Utilization"),
                                Mockito.anyString(),
                                Mockito.eq("rate")))
                .thenReturn(histogram);
        searchListener.preFetchPhase(searchContext);
        searchListener.fetchPhase(searchContext, 0l);
        Mockito.verify(task).addResourceTrackingCompletionListener(Mockito.any());
    }

    @Test
    public void testFetchPhaseFailed() {
        initializeValidSearchContext(true);
        Mockito.when(controller.getCollectorsSettingValue())
                .thenReturn(Util.CollectorMode.TELEMETRY.getValue());
        Mockito.when(
                        metricsRegistry.createHistogram(
                                Mockito.eq("CPU_Utilization"),
                                Mockito.anyString(),
                                Mockito.eq("rate")))
                .thenReturn(histogram);
        searchListener.preFetchPhase(searchContext);
        searchListener.failedFetchPhase(searchContext);
        Mockito.verify(task).addResourceTrackingCompletionListener(Mockito.any());
    }

    @Test
    public void testTaskCompletionListener() {
        initializeValidSearchContext(true);
        Mockito.when(
                        metricsRegistry.createHistogram(
                                Mockito.eq("CPU_Utilization"),
                                Mockito.anyString(),
                                Mockito.eq("rate")))
                .thenReturn(histogram);
        RTFPerformanceAnalyzerSearchListener rtfSearchListener =
                new RTFPerformanceAnalyzerSearchListener(controller);

        Mockito.when(shardId.getIndex()).thenReturn(index);
        Mockito.when(index.getName()).thenReturn("myTestIndex");
        Mockito.when(index.getUUID()).thenReturn("abc-def");
        Mockito.when(task.getTotalResourceStats()).thenReturn(taskResourceUsage);
        Mockito.when(taskResourceUsage.getCpuTimeInNanos()).thenReturn(10l);

        NotifyOnceListener<Task> taskCompletionListener =
                rtfSearchListener.createListener(searchContext, 0l, "test", false);
        taskCompletionListener.onResponse(task);
        Mockito.verify(histogram).record(Mockito.anyDouble(), Mockito.any(Tags.class));
    }

    private void initializeValidSearchContext(boolean isValid) {
        if (isValid) {
            Mockito.when(searchContext.request()).thenReturn(shardSearchRequest);
            Mockito.when(shardSearchRequest.shardId()).thenReturn(shardId);
        } else {
            Mockito.when(searchContext.request()).thenReturn(null);
        }
    }
}
