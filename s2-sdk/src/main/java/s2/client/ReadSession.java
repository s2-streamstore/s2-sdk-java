package s2.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.types.Batch;
import s2.types.ReadOutput;
import s2.types.ReadSessionRequest;
import s2.types.Start;
import s2.v1alpha.ReadSessionResponse;

public class ReadSession implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ReadSession.class.getName());

  private static final Long HEARTBEAT_THRESHOLD_NANOS = TimeUnit.SECONDS.toNanos(20);

  final ScheduledExecutorService executor;
  final StreamClient client;

  final AtomicReference<Start> nextStart;
  final AtomicLong consumedRecords = new AtomicLong();
  final AtomicLong consumedBytes = new AtomicLong(0);
  final AtomicInteger remainingAttempts;

  // Liveness timer.
  final AtomicLong lastEvent;
  final ListenableFuture<Void> livenessDaemon;

  final Consumer<ReadOutput> onResponse;
  final Consumer<Throwable> onError;

  final ReadSessionRequest request;
  final ListenableFuture<Void> daemon;

  ReadSession(
      StreamClient client,
      ReadSessionRequest request,
      Consumer<ReadOutput> onResponse,
      Consumer<Throwable> onError) {
    this.executor = client.executor;
    this.client = client;
    this.onResponse = onResponse;
    this.onError = onError;
    this.request = request;
    this.nextStart = new AtomicReference<>(request.start);
    this.remainingAttempts = new AtomicInteger(client.config.maxRetries);
    this.lastEvent = new AtomicLong(System.nanoTime());

    this.livenessDaemon = request.heartbeats ? livenessDaemon() : Futures.immediateFuture(null);
    this.daemon = this.retrying();
  }

  private ListenableFuture<Void> readSessionInner(
      ReadSessionRequest updatedRequest, Consumer<ReadOutput> innerOnResponse) {

    SettableFuture<Void> fut = SettableFuture.create();

    this.client.asyncStub.readSession(
        updatedRequest.toProto(this.client.streamName),
        new StreamObserver<ReadSessionResponse>() {

          @Override
          public void onNext(ReadSessionResponse value) {
            lastEvent.set(System.nanoTime());
            if (value.hasOutput()) {
              innerOnResponse.accept(ReadOutput.fromProto(value.getOutput()));
            } else {
              logger.trace("heartbeat");
            }
          }

          @Override
          public void onError(Throwable t) {
            logger.debug("Read session onError={}", t.toString());
            fut.setException(t);
          }

          @Override
          public void onCompleted() {
            logger.debug("Read session inner onCompleted");
            livenessDaemon.cancel(true);
            fut.set(null);
          }
        });
    return fut;
  }

  private ListenableFuture<Void> livenessDaemon() {
    SettableFuture<Void> livenessFuture = SettableFuture.create();
    scheduleLivenessCheck(livenessFuture);
    return livenessFuture;
  }

  private void scheduleLivenessCheck(SettableFuture<Void> livenessFuture) {
    final long delay = (lastEvent.get() + HEARTBEAT_THRESHOLD_NANOS) - System.nanoTime();

    logger.trace(
        "Checking liveness. Next deadline: {} seconds.",
        TimeUnit.SECONDS.convert(delay, TimeUnit.NANOSECONDS));
    if (delay <= 0) {
      this.onError.accept(
          Status.DEADLINE_EXCEEDED
              .withDescription("ReadSession hit local heartbeat deadline")
              .asRuntimeException());
      this.daemon.cancel(true);
      livenessFuture.set(null);
    } else {
      ScheduledFuture<?> scheduledCheck =
          executor.schedule(
              () -> {
                if (livenessFuture.isDone()) {
                  return;
                }
                scheduleLivenessCheck(livenessFuture);
              },
              delay,
              TimeUnit.NANOSECONDS);

      livenessFuture.addListener(() -> scheduledCheck.cancel(true), executor);
    }
  }

  private ListenableFuture<Void> retrying() {

    return Futures.catchingAsync(
        readSessionInner(
            request.update(nextStart.get(), consumedRecords.get(), consumedBytes.get()),
            resp -> {
              if (resp instanceof Batch) {
                final Batch batch = (Batch) resp;
                var lastPosition = batch.lastPosition();
                lastPosition.ifPresent(v -> nextStart.set(Start.seqNum(v.seqNum + 1)));
                consumedRecords.addAndGet(batch.sequencedRecordBatch.records.size());
                consumedBytes.addAndGet(batch.meteredBytes());
              }
              this.remainingAttempts.set(client.config.maxRetries);
              this.onResponse.accept(resp);
            }),
        Throwable.class,
        t -> {
          var status = Status.fromThrowable(t);
          var currentRemainingAttempts = remainingAttempts.getAndDecrement();
          if (currentRemainingAttempts > 0 && BaseClient.retryableStatus(status)) {
            logger.warn(
                "readSession retrying after {} delay, status={}",
                client.config.retryDelay,
                status.getCode());
            return Futures.scheduleAsync(this::retrying, client.config.retryDelay, this.executor);
          } else {
            logger.warn("readSession failed, status={}", status.getCode());
            onError.accept(t);
            this.livenessDaemon.cancel(true);
            return Futures.immediateFuture(null);
          }
        },
        executor);
  }

  public ListenableFuture<Void> awaitCompletion() {
    return this.daemon;
  }

  @Override
  public void close() {
    this.livenessDaemon.cancel(true);
    this.daemon.cancel(true);
  }
}
