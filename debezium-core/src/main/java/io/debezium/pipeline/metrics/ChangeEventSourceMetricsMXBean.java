/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Map;

import io.debezium.pipeline.metrics.traits.CommonEventMetricsMXBean;
import io.debezium.pipeline.metrics.traits.QueueMetricsMXBean;
import io.debezium.pipeline.metrics.traits.SchemaMetricsMXBean;

/**
 * Metrics that are common for both snapshot and streaming change event sources
 *
 * @author Jiri Pechanec
 */
public interface ChangeEventSourceMetricsMXBean extends CommonEventMetricsMXBean, QueueMetricsMXBean,
        SchemaMetricsMXBean {

    /**
     * @deprecated Superseded by the 'Captured Tables' metric. Use {@link #getCapturedTables()}.
     * Scheduled for removal in a future release.
     * As a temporary solution to be compatible with prometheus metrics format.
     */
    @Deprecated
    Map<String, Integer> getMonitoredTables();

    void reset();
}
