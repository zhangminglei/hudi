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

import org.apache.flink.core.fs.Path;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.commitpolicy.HiveTableMetaStore;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.sink.event.BatchWriteSuccessEvent;
import org.apache.hudi.sink.event.InitWriterEvent;
import org.apache.hudi.sink.event.ResponseEvent;
import org.apache.hudi.sink.utils.CoordinatorExecutor;
import org.apache.hudi.sink.utils.HiveSyncContext;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.table.utils.PartitionPathUtils.extractPartitionSpecFromPath;
import static org.apache.hudi.util.StreamerUtil.initTableIfNotExists;

/**
 * {@link OperatorCoordinator} for {@link StreamWriteFunction}.
 *
 * <p>This coordinator starts a new instant when a new checkpoint starts. It commits the instant when all the
 * operator tasks write the buffer successfully for a round of checkpoint.
 *
 * <p>If there is no data for a round of checkpointing, it resets the events buffer and returns early.
 *
 * @see StreamWriteFunction for the work flow and semantics
 */
public class StreamWriteOperatorCoordinator
    implements OperatorCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(StreamWriteOperatorCoordinator.class);

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Coordinator context.
   */
  private final Context context;

  /**
   * Write client.
   */
  private transient HoodieFlinkWriteClient writeClient;

  /**
   * Current REQUESTED instant, for validation.
   */
  private volatile String instant = "";

  /**
   * Event buffer for one round of checkpointing. When all the elements are non-null and have the same
   * write instant, then the instant succeed and we can commit it.
   */
  private transient BatchWriteSuccessEvent[] eventBuffer;

  /**
   * Event buffer for init. When all the elements are non-null then we can commit it and start new instant.
   */
  private transient InitWriterEvent[] initEventBuffer;

  /**
   * Task number of the operator.
   */
  private final int parallelism;

  /**
   * Whether needs to schedule compaction task on finished checkpoints.
   */
  private final boolean needsScheduleCompaction;

  /**
   * A single-thread executor to handle all the asynchronous jobs of the coordinator.
   */
  private CoordinatorExecutor executor;

  /**
   * A single-thread executor to handle asynchronous hive sync.
   */
  private NonThrownExecutor hiveSyncExecutor;

  /**
   * Context that holds variables for asynchronous hive sync.
   */
  private HiveSyncContext hiveSyncContext;

  /**
   * The table state.
   */
  private transient TableState tableState;

  /**
   * adapter the pattern in bilibili warehouse in hdfs path .../log_date=20210629/log_hour=00
   */
  private Pattern pattern = Pattern.compile(".*(log_date=)(\\d*)(/)?(log_hour=)?(\\d*)?");

  /**
   * Used to store each commit partition path.
   */
  private HashSet<String> partitionCollect = new HashSet<>();

  /**
   * Constructs a StreamingSinkOperatorCoordinator.
   *
   * @param conf    The config options
   * @param context The coordinator context
   */
  public StreamWriteOperatorCoordinator(
      Configuration conf,
      Context context) {
    this.conf = conf;
    this.context = context;
    this.parallelism = context.currentParallelism();
    this.needsScheduleCompaction = StreamerUtil.needsScheduleCompaction(conf);
  }

  @Override
  public void start() throws Exception {
    // initialize event buffer
    reset();
    this.writeClient = StreamerUtil.createWriteClient(conf, null);
    this.tableState = TableState.create(conf);
    // init table, create it if not exists.
    initTableIfNotExists(this.conf);
    // start the executor
    this.executor = new CoordinatorExecutor(this.context, LOG);
    // start the executor if required
    if (conf.getBoolean(FlinkOptions.HIVE_SYNC_ENABLED)) {
      initHiveSync();
    }
  }

  @Override
  public void close() throws Exception {
    // teardown the resource
    if (writeClient != null) {
      writeClient.close();
    }
    if (executor != null) {
      executor.close();
    }
    this.eventBuffer = null;
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
    executor.execute(
        () -> {
          try {
            result.complete(new byte[0]);
          } catch (Throwable throwable) {
            // when a checkpoint fails, throws directly.
            result.completeExceptionally(
                new CompletionException(
                    String.format("Failed to checkpoint Instant %s for source %s",
                        this.instant, this.getClass().getSimpleName()), throwable));
          }
        }, "taking checkpoint %d", checkpointId
    );
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    executor.execute(
        () -> {
          // for streaming mode, commits the ever received events anyway,
          // the stream write task snapshot and flush the data buffer synchronously in sequence,
          // so a successful checkpoint subsumes the old one(follows the checkpoint subsuming contract)
          final boolean committed = commitInstant();
          if (committed) {
            // if async compaction is on, schedule the compaction
            if (needsScheduleCompaction) {
              writeClient.scheduleCompaction(Option.empty());
            }
            // start new instant.
            startInstant();

            sendEventToWriter(context, true);
          }
        }, "commits the instant %s", this.instant
    );
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) {
    executor.execute(
         () -> {
           sendEventToWriter(context, false);
         }, "failed to checkpoint %s", checkpointId
    );
  }

  private void startInit() {
    executor.execute(() -> {
      String actionType = CommitUtils.getCommitActionType(
              WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION)),
              HoodieTableType.valueOf(conf.getString(FlinkOptions.TABLE_TYPE)));
      String pendingInstant = writeClient.getLastPendingInstant(actionType);

      if (pendingInstant == null) {
        startInstant();
        sendEventToWriter(context, false);
        return;
      }

      List<WriteStatus> writeResults = Arrays.stream(initEventBuffer)
          .filter(Objects::nonNull)
          .filter(initEvent -> initEvent.getInstant() != null && Objects.equals(initEvent.getInstant(), pendingInstant))
          .map(InitWriterEvent::getWriteStatuses)
          .flatMap(Collection::stream)
          .collect(Collectors.toList());

      if (writeResults.size() > 0) {
          doCommit(writeResults);
      }

      initEventBuffer = new InitWriterEvent[parallelism];

      startInstant();
      sendEventToWriter(context, true);
    }, "init success.");
  }

  private void sendEventToWriter(Context context, boolean committed) throws Exception {
    for (int i = 0; i < context.currentParallelism(); i++) {
      CompletableFuture<Acknowledge> future = context.sendEvent(new ResponseEvent(committed), i);
      future.get();
    }
  }

  private void syncHiveIfEnabled() {
    if (conf.getBoolean(FlinkOptions.HIVE_SYNC_ENABLED)) {
      LOG.info("syncHiveIfEnabled begin");
      this.hiveSyncExecutor.execute(this::syncHive, "sync hive metadata for instant %s", this.instant);
    }
  }

  /**
   * Sync hoodie table metadata to Hive metastore.
   */
  public void syncHive() {
    HiveSyncTool hiveSyncTool = hiveSyncContext.hiveSyncTool();
    LOG.info("hiveSyncTool client path " + hiveSyncTool.getHoodieHiveClient().getBasePath());
    hiveSyncTool.syncHoodieTable();
  }

  private void startInstant() {
    final String instant = HoodieActiveTimeline.createNewInstantTime();
    this.writeClient.startCommitWithTime(instant, tableState.commitAction);
    this.instant = instant;
    this.writeClient.transitionRequestedToInflight(tableState.commitAction, this.instant);
    LOG.info("Create instant [{}] for table [{}] with type [{}]", this.instant,
            this.conf.getString(FlinkOptions.TABLE_NAME), conf.getString(FlinkOptions.TABLE_TYPE));
  }

  @Override
  public void resetToCheckpoint(long checkpointID, @Nullable byte[] checkpointData) {
    // no operation
  }

  @Override
  public void handleEventFromOperator(int i, OperatorEvent operatorEvent) {
    executor.execute(
        () -> {
          if (operatorEvent instanceof BatchWriteSuccessEvent) {
            handleBatchWriteSuccessEvent((BatchWriteSuccessEvent) operatorEvent);
            return;
          }

          if (operatorEvent instanceof InitWriterEvent) {
            handleInitWriterEvent((InitWriterEvent) operatorEvent);
            return;
          }

          // no event to handle
          throw new IllegalStateException("The coordinator can only handle BatchWriteSuccessEvent and InitWriterEvent.");
        }, "handle write success event for instant %s", this.instant
    );
  }

  @Override
  public void subtaskFailed(int i, @Nullable Throwable throwable) {
    // no operation
  }

  @Override
  public void subtaskReset(int i, long l) {
    // no operation
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void initHiveSync() {
    this.hiveSyncExecutor = new NonThrownExecutor(LOG);
    this.hiveSyncContext = HiveSyncContext.create(conf);
  }

  private void reset() {
    this.eventBuffer = new BatchWriteSuccessEvent[this.parallelism];
  }

  /** Checks the buffer is ready to commit. */
  private boolean allEventsReceived() {
    return Arrays.stream(eventBuffer)
        .allMatch(event -> event != null && event.isReady(this.instant));
  }

  /** Checks the init event all received. */
  private boolean allInitEventsReceived() {
    return Arrays.stream(initEventBuffer)
            .allMatch(Objects::nonNull);
  }

  private void handleBatchWriteSuccessEvent(BatchWriteSuccessEvent event) {
    // the write task does not block after checkpointing(and before it receives a checkpoint success event),
    // if it checkpoints succeed then flushes the data buffer again before this coordinator receives a checkpoint
    // success event, the data buffer would flush with an older instant time.
    ValidationUtils.checkState(
            HoodieTimeline.compareTimestamps(instant, HoodieTimeline.GREATER_THAN_OR_EQUALS, event.getInstantTime()),
            String.format("Receive an unexpected event for instant %s from task %d",
                    event.getInstantTime(), event.getTaskID()));

    if (this.eventBuffer[event.getTaskID()] != null) {
      this.eventBuffer[event.getTaskID()].mergeWith(event);
    } else {
      this.eventBuffer[event.getTaskID()] = event;
    }
    if (event.isEndInput() && allEventsReceived()) {
      // start to commit the instant.
      commitInstant();
      // no compaction scheduling for batch mode
    }
  }

  private void handleInitWriterEvent(InitWriterEvent event) {
    if (this.initEventBuffer == null) {
      initEventBuffer = new InitWriterEvent[this.parallelism];
    }

    this.initEventBuffer[event.getTaskID()] = event;

    if (allInitEventsReceived()) {
      startInit();
    }
  }

  /**
   * Commits the instant.
   *
   * @return true if the write statuses are committed successfully.
   */
  private boolean commitInstant() {
    if (Arrays.stream(eventBuffer).allMatch(Objects::isNull)) {
      // The last checkpoint finished successfully.
      return false;
    }

    List<WriteStatus> writeResults = Arrays.stream(eventBuffer)
        .filter(Objects::nonNull)
        .map(BatchWriteSuccessEvent::getWriteStatuses)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    if (writeResults.size() == 0) {
      // No data has written, reset the buffer and returns early
      reset();
      return false;
    }

    Optional<BatchWriteSuccessEvent> min = Arrays.stream(eventBuffer).filter(Objects::nonNull).min((o1, o2) -> (int) (o1.getWatermark() - o2.getWatermark()));
    doCommit(writeResults);
    if (!min.isPresent()) {
      return true;
    }
    commitToHms(writeResults, min.get().getWatermark());
    return true;
  }

  public void commitToHms(List<WriteStatus> writeResults, long watermark) {
    List<String> partitionPath = writeResults.stream().map(WriteStatus::getPartitionPath).collect(Collectors.toList());
    partitionCollect.addAll(partitionPath);
    List<String> needCommit = new ArrayList<>();
    partitionCollect.forEach(partitionTime -> {
      Matcher matcher = pattern.matcher(partitionTime);
      if (partitionTime.contains("log_hour")) {
        if (matcher.find()) {
          String partTime = matcher.group(2) + matcher.group(5);
          if (needCommit(true, watermark, partTime)) {
            needCommit.add(matcher.group(1) + matcher.group(2) + matcher.group(3) + matcher.group(4) + matcher.group(5));
          }
        }
      }

      if (!partitionTime.contains("log_hour")) {
        if (matcher.find()) {
          String partTime = matcher.group(2);
          if (needCommit(false, watermark, partTime)) {
            needCommit.add(matcher.group(1) + matcher.group(2));
          }
        }
      }
    });

    try {
      HiveTableMetaStore metaStore = HiveTableMetaStore.HiveTableMetaStoreFactory.
              getHiveTableMetaStore(conf.getString(FlinkOptions.HIVE_SYNC_DB), conf.getString(FlinkOptions.HIVE_SYNC_TABLE));
      for (String commitPartition: needCommit) {
        LOG.info("need commits partition {}", commitPartition);
        LinkedHashMap<String, String> partSpec = extractPartitionSpecFromPath(new Path(commitPartition));
        Path path = new Path(conf.getString(FlinkOptions.PATH) + "/" + commitPartition);
        metaStore.createOrAlterPartition(partSpec, path);
        partitionCollect.remove(commitPartition + "/");
      }
    } catch (Exception e) {
      LOG.warn("Failed to commit partition {}", partitionCollect, e);
    }
  }

  private boolean needCommit(boolean hourPartition, long lowWatermark, String partitionTime) {
    try {
      if (hourPartition) {
        String watermark = transformToHour(lowWatermark);
        return Integer.parseInt(watermark) > Integer.parseInt(partitionTime);
      } else {
        String watermark = transformToDay(lowWatermark);
        return Integer.parseInt(watermark) > Integer.parseInt(partitionTime);
      }
    } catch (ParseException p) {
      return false;
    }
  }

  public static String transformToHour(long time) throws ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
    Date date = sdf.parse(sdf.format(time));
    return sdf.format(date);
  }

  public static String transformToDay(long time) throws ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    Date date = sdf.parse(sdf.format(time));
    return sdf.format(date);
  }

  /** Performs the actual commit action. */
  @SuppressWarnings("unchecked")
  private void doCommit(List<WriteStatus> writeResults) {
    // commit or rollback
    long totalErrorRecords = writeResults.stream().map(WriteStatus::getTotalErrorRecords).reduce(Long::sum).orElse(0L);
    long totalRecords = writeResults.stream().map(WriteStatus::getTotalRecords).reduce(Long::sum).orElse(0L);
    boolean hasErrors = totalErrorRecords > 0;

    if (!hasErrors || this.conf.getBoolean(FlinkOptions.IGNORE_FAILED)) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      if (hasErrors) {
        LOG.warn("Some records failed to merge but forcing commit since commitOnErrors set to true. Errors/Total="
            + totalErrorRecords + "/" + totalRecords);
      }

      final Map<String, List<String>> partitionToReplacedFileIds = tableState.isOverwrite
          ? writeClient.getPartitionToReplacedFileIds(tableState.operationType, writeResults)
          : Collections.emptyMap();
      boolean success = writeClient.commit(this.instant, writeResults, Option.of(checkpointCommitMetadata), tableState.commitAction, partitionToReplacedFileIds);
      if (success) {
        reset();
        LOG.info("Commit instant [{}] success!", this.instant);
      } else {
        throw new HoodieException(String.format("Commit instant [%s] failed!", this.instant));
      }
    } else {
      LOG.error("Error when writing. Errors/Total=" + totalErrorRecords + "/" + totalRecords);
      LOG.error("The first 100 error messages");
      writeResults.stream().filter(WriteStatus::hasErrors).limit(100).forEach(ws -> {
        LOG.error("Global error for partition path {} and fileID {}: {}",
            ws.getGlobalError(), ws.getPartitionPath(), ws.getFileId());
        if (ws.getErrors().size() > 0) {
          ws.getErrors().forEach((key, value) -> LOG.trace("Error for key:" + key + " and value " + value));
        }
      });
      // Rolls back instant
      writeClient.rollback(this.instant);
      throw new HoodieException(String.format("Commit instant [%s] failed and rolled back !", this.instant));
    }
  }

  @VisibleForTesting
  public BatchWriteSuccessEvent[] getEventBuffer() {
    return eventBuffer;
  }

  @VisibleForTesting
  public InitWriterEvent[] getInitBuffer() {
    return initEventBuffer;
  }

  @VisibleForTesting
  public String getInstant() {
    return instant;
  }

  @VisibleForTesting
  @SuppressWarnings("rawtypes")
  public HoodieFlinkWriteClient getWriteClient() {
    return writeClient;
  }

  @VisibleForTesting
  public Context getContext() {
    return context;
  }

  @VisibleForTesting
  public void setExecutor(CoordinatorExecutor executor) throws Exception {
    if (this.executor != null) {
      this.executor.close();
    }
    this.executor = executor;
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Provider for {@link StreamWriteOperatorCoordinator}.
   */
  public static class Provider implements OperatorCoordinator.Provider {
    private final OperatorID operatorId;
    private final Configuration conf;

    public Provider(OperatorID operatorId, Configuration conf) {
      this.operatorId = operatorId;
      this.conf = conf;
    }

    @Override
    public OperatorID getOperatorId() {
      return this.operatorId;
    }

    @Override
    public OperatorCoordinator create(Context context) {
      return new StreamWriteOperatorCoordinator(this.conf, context);
    }
  }

  /**
   * Remember some table state variables.
   */
  private static class TableState implements Serializable {
    private static final long serialVersionUID = 1L;

    private final WriteOperationType operationType;
    private final String commitAction;
    private final boolean isOverwrite;

    private TableState(Configuration conf) {
      this.operationType = WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION));
      this.commitAction = CommitUtils.getCommitActionType(this.operationType,
          HoodieTableType.valueOf(conf.getString(FlinkOptions.TABLE_TYPE).toUpperCase(Locale.ROOT)));
      this.isOverwrite = WriteOperationType.isOverwrite(this.operationType);
    }

    public static TableState create(Configuration conf) {
      return new TableState(conf);
    }
  }
}
