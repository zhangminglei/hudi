/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.ObjectSizeCalculator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.event.BatchWriteSuccessEvent;
import org.apache.hudi.table.action.commit.FlinkWriteHelper;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Sink function to write the data to the underneath filesystem.
 *
 * <p><h2>Work Flow</h2>
 *
 * <p>The function firstly buffers the data as a batch of {@link HoodieRecord}s,
 * It flushes(write) the records batch when the batch size exceeds the configured size {@link FlinkOptions#WRITE_BATCH_SIZE}
 * or the total buffer size exceeds the configured size {@link FlinkOptions#WRITE_TASK_MAX_SIZE}
 * or a Flink checkpoint starts. After a batch has been written successfully,
 * the function notifies its operator coordinator {@link StreamWriteOperatorCoordinator} to mark a successful write.
 *
 * <p><h2>The Semantics</h2>
 *
 * <p>The task implements exactly-once semantics by buffering the data between checkpoints. The operator coordinator
 * starts a new instant on the time line when a checkpoint triggers, the coordinator checkpoints always
 * start before its operator, so when this function starts a checkpoint, a REQUESTED instant already exists.
 *
 * <p>In order to improve the throughput, The function process thread does not block data buffering
 * after the checkpoint thread starts flushing the existing data buffer. So there is possibility that the next checkpoint
 * batch was written to current checkpoint. When a checkpoint failure triggers the write rollback, there may be some duplicate records
 * (e.g. the eager write batch), the semantics is still correct using the UPSERT operation.
 *
 * <p><h2>Fault Tolerance</h2>
 *
 * <p>The operator coordinator checks and commits the last instant then starts a new one when a checkpoint finished successfully.
 * The operator rolls back the written data and throws to trigger a failover when any error occurs.
 * This means one Hoodie instant may span one or more checkpoints(some checkpoints notifications may be skipped).
 * If a checkpoint timed out, the next checkpoint would help to rewrite the left buffer data (clean the buffer in the last
 * step of the #flushBuffer method).
 *
 * <p>The operator coordinator would try several times when committing the write status.
 *
 * <p>Note: The function task requires the input stream be shuffled by the file IDs.
 *
 * @param <I> Type of the input record
 * @see StreamWriteOperatorCoordinator
 */
public class StreamWriteFunction<I, O>
    extends ProcessFunction<I, O>
    implements CheckpointedFunction, CheckpointListener {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(StreamWriteFunction.class);

  /**
   * Write buffer as buckets for a checkpoint. The key is bucket ID.
   */
  private transient Map<String, DataBucket> buckets;

  /**
   * Config options.
   */
  private final Configuration config;

  /**
   * Id of current subtask.
   */
  private int taskID;

  /**
   * Write Client.
   */
  private transient HoodieFlinkWriteClient writeClient;

  private transient BiFunction<List<HoodieRecord>, String, List<WriteStatus>> writeFunction;

  /**
   * The REQUESTED instant we write the data.
   */
  private volatile String currentInstant;

  /**
   * Gateway to send operator events to the operator coordinator.
   */
  private transient OperatorEventGateway eventGateway;

  /**
   * Commit action type.
   */
  private transient String actionType;

  /**
   * Total size tracer.
   */
  private transient TotalSizeTracer tracer;

  /**
   * Flag saying whether the write task is waiting for the checkpoint success notification
   * after it finished a checkpoint.
   *
   * <p>The flag is needed because the write task does not block during the waiting time interval,
   * some data buckets still flush out with old instant time. There are two cases that the flush may produce
   * corrupted files if the old instant is committed successfully:
   * 1) the write handle was writing data but interrupted, left a corrupted parquet file;
   * 2) the write handle finished the write but was not closed, left an empty parquet file.
   *
   * <p>To solve, when this flag was set to true, we flush the data buffer with a new instant time = old instant time + 1ms,
   * the new instant time would affect the write file name. The filesystem view does not recognize the file as committed because
   * it always filters the data files based on successful commit time.
   */
  private volatile boolean confirming = false;

  /**
   * Constructs a StreamingSinkFunction.
   *
   * @param config The config options
   */
  public StreamWriteFunction(Configuration config) {
    this.config = config;
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.writeClient = StreamerUtil.createWriteClient(this.config, getRuntimeContext());
    this.actionType = CommitUtils.getCommitActionType(
        WriteOperationType.fromValue(config.getString(FlinkOptions.OPERATION)),
        HoodieTableType.valueOf(config.getString(FlinkOptions.TABLE_TYPE)));
    this.tracer = new TotalSizeTracer(this.config);
    initBuffer();
    initWriteFunction();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    // no operation
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {
    // Based on the fact that the coordinator starts the checkpoint first,
    // it would check the validity.
    // wait for the buffer data flush out and request a new instant
    flushRemaining(false);
  }

  @Override
  public void processElement(I value, ProcessFunction<I, O>.Context ctx, Collector<O> out) {
    bufferRecord(value);
  }

  @Override
  public void close() {
    if (this.writeClient != null) {
      this.writeClient.cleanHandles();
      this.writeClient.close();
    }
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    this.confirming = false;
  }

  /**
   * End input action for batch source.
   */
  public void endInput() {
    flushRemaining(true);
    this.writeClient.cleanHandles();
  }

  // -------------------------------------------------------------------------
  //  Getter/Setter
  // -------------------------------------------------------------------------

  @VisibleForTesting
  @SuppressWarnings("rawtypes")
  public Map<String, List<HoodieRecord>> getDataBuffer() {
    Map<String, List<HoodieRecord>> ret = new HashMap<>();
    for (Map.Entry<String, DataBucket> entry : buckets.entrySet()) {
      ret.put(entry.getKey(), entry.getValue().records);
    }
    return ret;
  }

  @VisibleForTesting
  public boolean isConfirming() {
    return confirming;
  }

  @VisibleForTesting
  @SuppressWarnings("rawtypes")
  public HoodieFlinkWriteClient getWriteClient() {
    return writeClient;
  }

  public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
    this.eventGateway = operatorEventGateway;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void initBuffer() {
    this.buckets = new LinkedHashMap<>();
  }

  private void initWriteFunction() {
    final String writeOperation = this.config.get(FlinkOptions.OPERATION);
    switch (WriteOperationType.fromValue(writeOperation)) {
      case INSERT:
        this.writeFunction = (records, instantTime) -> this.writeClient.insert(records, instantTime);
        break;
      case UPSERT:
        this.writeFunction = (records, instantTime) -> this.writeClient.upsert(records, instantTime);
        break;
      case INSERT_OVERWRITE:
        this.writeFunction = (records, instantTime) -> this.writeClient.insertOverwrite(records, instantTime);
        break;
      case INSERT_OVERWRITE_TABLE:
        this.writeFunction = (records, instantTime) -> this.writeClient.insertOverwriteTable(records, instantTime);
        break;
      default:
        throw new RuntimeException("Unsupported write operation : " + writeOperation);
    }
  }

  /**
   * Data bucket.
   */
  private static class DataBucket {
    private final List<HoodieRecord> records;
    private final BufferSizeDetector detector;

    private DataBucket(Double batchSize) {
      this.records = new ArrayList<>();
      this.detector = new BufferSizeDetector(batchSize);
    }

    public void reset() {
      this.records.clear();
      this.detector.reset();
    }
  }

  /**
   * Tool to detect if to flush out the existing buffer.
   * Sampling the record to compute the size with 0.01 percentage.
   */
  private static class BufferSizeDetector {
    private final Random random = new Random(47);
    private static final int DENOMINATOR = 100;

    private final double batchSizeBytes;

    private long lastRecordSize = -1L;
    private long totalSize = 0L;

    BufferSizeDetector(double batchSizeMb) {
      this.batchSizeBytes = batchSizeMb * 1024 * 1024;
    }

    boolean detect(Object record) {
      if (lastRecordSize == -1 || sampling()) {
        lastRecordSize = ObjectSizeCalculator.getObjectSize(record);
      }
      totalSize += lastRecordSize;
      return totalSize > this.batchSizeBytes;
    }

    boolean sampling() {
      // 0.01 sampling percentage
      return random.nextInt(DENOMINATOR) == 1;
    }

    void reset() {
      this.lastRecordSize = -1L;
      this.totalSize = 0L;
    }
  }

  /**
   * Tool to trace the total buffer size. It computes the maximum buffer size,
   * if current buffer size is greater than the maximum buffer size, the data bucket
   * flush triggers.
   */
  private static class TotalSizeTracer {
    private long bufferSize = 0L;
    private final double maxBufferSize;

    TotalSizeTracer(Configuration conf) {
      long mergeReaderMem = 100; // constant 100MB
      long mergeMapMaxMem = conf.getInteger(FlinkOptions.WRITE_MERGE_MAX_MEMORY);
      this.maxBufferSize = (conf.getDouble(FlinkOptions.WRITE_TASK_MAX_SIZE) - mergeReaderMem - mergeMapMaxMem) * 1024 * 1024;
      final String errMsg = String.format("'%s' should be at least greater than '%s' plus merge reader memory(constant 100MB now)",
          FlinkOptions.WRITE_TASK_MAX_SIZE.key(), FlinkOptions.WRITE_MERGE_MAX_MEMORY.key());
      ValidationUtils.checkState(this.maxBufferSize > 0, errMsg);
    }

    /**
     * Trace the given record size {@code recordSize}.
     *
     * @param recordSize The record size
     * @return true if the buffer size exceeds the maximum buffer size
     */
    boolean trace(long recordSize) {
      this.bufferSize += recordSize;
      return this.bufferSize > this.maxBufferSize;
    }

    void countDown(long size) {
      this.bufferSize -= size;
    }

    public void reset() {
      this.bufferSize = 0;
    }
  }

  /**
   * Returns the bucket ID with the given value {@code value}.
   */
  private String getBucketID(I value) {
    HoodieRecord<?> record = (HoodieRecord<?>) value;
    final String fileId = record.getCurrentLocation().getFileId();
    return StreamerUtil.generateBucketKey(record.getPartitionPath(), fileId);
  }

  /**
   * Buffers the given record.
   *
   * <p>Flush the data bucket first if the bucket records size is greater than
   * the configured value {@link FlinkOptions#WRITE_BATCH_SIZE}.
   *
   * <p>Flush the max size data bucket if the total buffer size exceeds the configured
   * threshold {@link FlinkOptions#WRITE_TASK_MAX_SIZE}.
   *
   * @param value HoodieRecord
   */
  private void bufferRecord(I value) {
    final String bucketID = getBucketID(value);

    DataBucket bucket = this.buckets.computeIfAbsent(bucketID,
        k -> new DataBucket(this.config.getDouble(FlinkOptions.WRITE_BATCH_SIZE)));
    boolean flushBucket = bucket.detector.detect(value);
    boolean flushBuffer = this.tracer.trace(bucket.detector.lastRecordSize);
    if (flushBucket) {
      flushBucket(bucket);
      this.tracer.countDown(bucket.detector.totalSize);
      bucket.reset();
    } else if (flushBuffer) {
      // find the max size bucket and flush it out
      List<DataBucket> sortedBuckets = this.buckets.values().stream()
          .sorted((b1, b2) -> Long.compare(b2.detector.totalSize, b1.detector.totalSize))
          .collect(Collectors.toList());
      final DataBucket bucketToFlush = sortedBuckets.get(0);
      flushBucket(bucketToFlush);
      this.tracer.countDown(bucketToFlush.detector.totalSize);
      bucketToFlush.reset();
    }
    bucket.records.add((HoodieRecord<?>) value);
  }

  @SuppressWarnings("unchecked, rawtypes")
  private void flushBucket(DataBucket bucket) {
    final String instant = this.writeClient.getLastPendingInstant(this.actionType);

    if (instant == null) {
      // in case there are empty checkpoints that has no input data
      LOG.info("No inflight instant when flushing data, cancel.");
      return;
    }

    // if we are waiting for the checkpoint notification, shift the write instant time.
    boolean shift = confirming && StreamerUtil.equal(instant, this.currentInstant);
    final String flushInstant = shift ? StreamerUtil.instantTimePlus(instant, 1) : instant;

    List<HoodieRecord> records = bucket.records;
    ValidationUtils.checkState(records.size() > 0, "Data bucket to flush has no buffering records");
    if (config.getBoolean(FlinkOptions.INSERT_DROP_DUPS)) {
      records = FlinkWriteHelper.newInstance().deduplicateRecords(records, (HoodieIndex) null, -1);
    }
    final List<WriteStatus> writeStatus = new ArrayList<>(writeFunction.apply(records, flushInstant));
    final BatchWriteSuccessEvent event = BatchWriteSuccessEvent.builder()
        .taskID(taskID)
        .instantTime(instant) // the write instant may shift but the event still use the currentInstant.
        .writeStatus(writeStatus)
        .isLastBatch(false)
        .isEndInput(false)
        .build();
    this.eventGateway.sendEventToCoordinator(event);
  }

  @SuppressWarnings("unchecked, rawtypes")
  private void flushRemaining(boolean isEndInput) {
    this.currentInstant = this.writeClient.getLastPendingInstant(this.actionType);
    if (this.currentInstant == null) {
      // in case there are empty checkpoints that has no input data
      LOG.info("No inflight instant when flushing data, cancel.");
      return;
    }
    final List<WriteStatus> writeStatus;
    if (buckets.size() > 0) {
      writeStatus = new ArrayList<>();
      this.buckets.values()
          // The records are partitioned by the bucket ID and each batch sent to
          // the writer belongs to one bucket.
          .forEach(bucket -> {
            List<HoodieRecord> records = bucket.records;
            if (records.size() > 0) {
              if (config.getBoolean(FlinkOptions.INSERT_DROP_DUPS)) {
                records = FlinkWriteHelper.newInstance().deduplicateRecords(records, (HoodieIndex) null, -1);
              }
              writeStatus.addAll(writeFunction.apply(records, currentInstant));
            }
          });
    } else {
      LOG.info("No data to write in subtask [{}] for instant [{}]", taskID, currentInstant);
      writeStatus = Collections.emptyList();
    }
    final BatchWriteSuccessEvent event = BatchWriteSuccessEvent.builder()
        .taskID(taskID)
        .instantTime(currentInstant)
        .writeStatus(writeStatus)
        .isLastBatch(true)
        .isEndInput(isEndInput)
        .build();
    this.eventGateway.sendEventToCoordinator(event);
    this.buckets.clear();
    this.tracer.reset();
    this.writeClient.cleanHandles();
    this.confirming = true;
  }
}
