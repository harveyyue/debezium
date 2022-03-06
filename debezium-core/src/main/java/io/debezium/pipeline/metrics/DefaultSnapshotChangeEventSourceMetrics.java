/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.metrics;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.meters.SnapshotMeter;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

/**
 * The default implementation of metrics related to the snapshot phase of a connector.
 *
 * @author Randall Hauch, Jiri Pechanec
 */
@ThreadSafe
public class DefaultSnapshotChangeEventSourceMetrics<P extends Partition> extends PipelineMetrics<P>
        implements SnapshotChangeEventSourceMetrics<P>, SnapshotChangeEventSourceMetricsMXBean {

    private final SnapshotMeter snapshotMeter;

    public <T extends CdcSourceTaskContext> DefaultSnapshotChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                    EventMetadataProvider metadataProvider) {
        super(taskContext, "snapshot", changeEventQueueMetrics, metadataProvider);
        snapshotMeter = new SnapshotMeter(taskContext.getClock());
    }

    @Override
    public int getTotalTableCount() {
        return snapshotMeter.getTotalTableCount();
    }

    @Override
    public int getRemainingTableCount() {
        return snapshotMeter.getRemainingTableCount();
    }

    @Override
    public boolean getSnapshotRunning() {
        return snapshotMeter.getSnapshotRunning();
    }

    @Override
    public boolean getSnapshotCompleted() {
        return snapshotMeter.getSnapshotCompleted();
    }

    @Override
    public boolean getSnapshotAborted() {
        return snapshotMeter.getSnapshotAborted();
    }

    @Override
    public long getSnapshotDurationInSeconds() {
        return snapshotMeter.getSnapshotDurationInSeconds();
    }

    /**
     * @deprecated Superseded by the 'Captured Tables' metric. Use {@link #getCapturedTables()}.
     * Scheduled for removal in a future release.
     */
    @Override
    @Deprecated
    public Map<String, Integer> getMonitoredTables() {
        return Arrays.stream(snapshotMeter.getCapturedTables()).collect(Collectors.toMap(f -> f, f -> 1));
    }

    @Override
    public String[] getCapturedTables() {
        return snapshotMeter.getCapturedTables();
    }

    @Override
    public void monitoredDataCollectionsDetermined(P partition, Iterable<? extends DataCollectionId> dataCollectionIds) {
        snapshotMeter.monitoredDataCollectionsDetermined(dataCollectionIds);
    }

    @Override
    public void dataCollectionSnapshotCompleted(P partition, DataCollectionId dataCollectionId, long numRows) {
        snapshotMeter.dataCollectionSnapshotCompleted(dataCollectionId, numRows);
    }

    @Override
    public void snapshotStarted(P partition) {
        snapshotMeter.snapshotStarted();
    }

    @Override
    public void snapshotCompleted(P partition) {
        snapshotMeter.snapshotCompleted();
    }

    @Override
    public void snapshotAborted(P partition) {
        snapshotMeter.snapshotAborted();
    }

    @Override
    public void rowsScanned(P partition, TableId tableId, long numRows) {
        snapshotMeter.rowsScanned(tableId, numRows);
    }

    @Override
    public ConcurrentMap<String, Long> getRowsScanned() {
        return snapshotMeter.getRowsScanned();
    }

    @Override
    public void currentChunk(P partition, String chunkId, Object[] chunkFrom, Object[] chunkTo) {
        snapshotMeter.currentChunk(chunkId, chunkFrom, chunkTo);
    }

    @Override
    public void currentChunk(P partition, String chunkId, Object[] chunkFrom, Object[] chunkTo, Object[] tableTo) {
        snapshotMeter.currentChunk(chunkId, chunkFrom, chunkTo, tableTo);
    }

    @Override
    public String getChunkId() {
        return snapshotMeter.getChunkId();
    }

    @Override
    public String getChunkFrom() {
        return snapshotMeter.getChunkFrom();
    }

    @Override
    public String getChunkTo() {
        return snapshotMeter.getChunkTo();
    }

    @Override
    public String getTableFrom() {
        return snapshotMeter.getTableFrom();
    }

    @Override
    public String getTableTo() {
        return snapshotMeter.getTableTo();
    }

    @Override
    public void reset() {
        super.reset();
        snapshotMeter.reset();
    }
}
