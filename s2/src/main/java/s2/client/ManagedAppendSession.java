package s2.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.config.AppendRetryPolicy;
import s2.types.AppendInput;
import s2.types.AppendOutput;
import s2.v1alpha.AppendSessionRequest;
import s2.v1alpha.AppendSessionResponse;

public class ManagedAppendSession {

  record InflightRecord(
      AppendInput input,
      Long entryNanos,
      SettableFuture<AppendOutput> callback,
      Long meteredBytes) {
    static InflightRecord construct(AppendInput input, Long meteredBytes) {
      return new InflightRecord(input, System.nanoTime(), SettableFuture.create(), meteredBytes);
    }
  }

  private static final Logger logger =
      LoggerFactory.getLogger(ManagedAppendSession.class.getName());

  sealed interface Notification permits Ack, Batch, ClientClose, Error, ServerClose {}

  record Batch(InflightRecord input) implements Notification {}

  record Ack(AppendOutput output) implements Notification {}

  record Error(Throwable throwable) implements Notification {}

  record ClientClose(boolean gracefully) implements Notification {}

  record ServerClose() implements Notification {}

  final ListeningScheduledExecutorService executor;
  final StreamClient client;
  final Integer bufferCapacityBytes;
  final Semaphore inflightBytes;
  final ListenableFuture<Void> daemon;
  final AtomicInteger remainingAttempts;
  final AtomicReference<Optional<Long>> nextDeadlineSystemNanos =
      new AtomicReference<>(Optional.empty());

  static final int ACQUIRE_QUANTUM_MS = 50;

  final AtomicBoolean acceptingAppends = new AtomicBoolean(true);

  // TODO can use theoretical max for smallest possible batch sizes given inflightBytes budget to
  // bound queue sizes
  final LinkedBlockingQueue<InflightRecord> inflightQueue = new LinkedBlockingQueue<>();
  final LinkedBlockingQueue<Notification> notificationQueue = new LinkedBlockingQueue<>();

  ManagedAppendSession(StreamClient client) {
    this.executor = MoreExecutors.listeningDecorator(client.executor);
    this.client = client;
    this.daemon = retryingDaemon();
    this.bufferCapacityBytes = client.config.maxAppendInflightBytes;
    this.inflightBytes = new Semaphore(this.bufferCapacityBytes);
    this.remainingAttempts = new AtomicInteger(this.client.config.maxRetries);
  }

  private ListenableFuture<Void> retryingDaemon() {
    return Futures.catchingAsync(
        executor.submit(this::daemon),
        Throwable.class,
        err -> {
          var status = Status.fromThrowable(err);
          var currentRemainingAttempts = this.remainingAttempts.getAndDecrement();
          if (client.config.appendRetryPolicy == AppendRetryPolicy.ALL
              && BaseClient.retryableStatus(status)
              && currentRemainingAttempts > 0) {
            logger.debug(
                "Retrying error with (original err={}) status={}, after {} delay.",
                err,
                status,
                client.config.retryDelay);
            return Futures.scheduleAsync(
                this::retryingDaemon, client.config.retryDelay, this.executor);
          } else {
            logger.warn(
                "Not retrying error with status={}. Cleaning up append session.", status.getCode());
            return executor.submit(() -> cleanUp(status));
          }
        },
        this.executor);
  }

  private boolean acquirePermits(int permits, Duration maxWait) throws InterruptedException {
    var millisToWait = maxWait.toMillis();
    do {
      if (millisToWait < 0) {
        // maxWait has expired, and we haven't acquired permits.
        return false;
      }
      millisToWait -= ACQUIRE_QUANTUM_MS;

      // Make sure appends are still welcome before trying to acquire permits.
      if (!acceptingAppends.get()) {
        throw new RuntimeException("AppendSession has been shutdown.");
      }

    } while (!this.inflightBytes.tryAcquire(permits, ACQUIRE_QUANTUM_MS, TimeUnit.MILLISECONDS));

    // We've acquired permits, but should check once more to verify that appends
    // are still welcome.
    if (!acceptingAppends.get()) {
      this.inflightBytes.release(permits);
      throw new RuntimeException("AppendSession has been shutdown.");
    }
    return true;
  }

  public ListenableFuture<AppendOutput> submit(AppendInput input, Duration maxWait)
      throws InterruptedException {
    // TODO maxWait should be capped
    long meteredBytes = input.meteredBytes();
    if (!acquirePermits((int) meteredBytes, maxWait)) {
      throw new RuntimeException("Unable to acquire permits within deadline.");
    }
    var record = InflightRecord.construct(input, meteredBytes);
    this.notificationQueue.put(new Batch(record));

    return record.callback;
  }

  public ListenableFuture<Void> closeGracefully() throws InterruptedException {
    this.acceptingAppends.set(false);
    this.notificationQueue.put(new ClientClose(true));
    return daemon;
  }

  /// Note that this will NOT resolve any outstanding futures issued by this session.
  public ListenableFuture<Void> closeImmediately() throws InterruptedException {
    this.acceptingAppends.set(false);
    this.notificationQueue.put(new ClientClose(false));
    return daemon;
  }

  private void performInflightRecovery() throws InterruptedException {
    final ArrayBlockingQueue<Notification> recoveryNotificationQueue =
        new ArrayBlockingQueue<>(inflightQueue.size());
    final var recoveryObserver =
        this.client.asyncStub.appendSession(
            new StreamObserver<>() {
              @Override
              public void onNext(AppendSessionResponse value) {
                recoveryNotificationQueue.add(new Ack(AppendOutput.fromProto(value.getOutput())));
              }

              @Override
              public void onError(Throwable t) {
                recoveryNotificationQueue.add(new Error(t));
              }

              @Override
              public void onCompleted() {
                recoveryNotificationQueue.add(new Error(new Throwable("unexpected server close")));
              }
            });

    logger.debug("resending inflight recovery");
    // Retransmit all entries in the queue.
    inflightQueue.forEach(
        record -> {
          recoveryObserver.onNext(
              AppendSessionRequest.newBuilder()
                  .setInput(record.input.toProto(this.client.streamName))
                  .build());
        });

    logger.debug("inflight recovery finished");
    while (!inflightQueue.isEmpty()) {

      final long nanosToWait =
          this.nextDeadlineSystemNanos
              .get()
              .orElseThrow(
                  () ->
                      Status.INTERNAL
                          .withDescription("internal corruption; expected next deadline")
                          .asRuntimeException());

      final Notification notification =
          nanosToWait > 0
              ? recoveryNotificationQueue.poll(nanosToWait, TimeUnit.NANOSECONDS)
              : null;
      if (notification == null) {
        throw Status.CANCELLED
            .withDescription("hit deadline while retransmitting")
            .asRuntimeException();
      } else if (notification instanceof Ack ack) {
        this.remainingAttempts.set(this.client.config.maxRetries);
        var correspondingInflight = inflightQueue.poll();
        if (correspondingInflight == null) {
          throw Status.INTERNAL.withDescription("inflight queue is empty").asRuntimeException();
        } else {
          validate(correspondingInflight, ack.output);
          correspondingInflight.callback.set(ack.output);
          this.inflightBytes.release(correspondingInflight.meteredBytes.intValue());
        }
      } else if (notification instanceof Error error) {
        throw new RuntimeException(error.throwable);
      } else {
        throw Status.INTERNAL
            .withDescription(
                String.format(
                    "received unexpected %s notification during inflight recovery", notification))
            .asRuntimeException();
      }
    }
  }

  private synchronized Void cleanUp(Status fatal) throws InterruptedException {
    this.acceptingAppends.set(false);
    this.inflightBytes.drainPermits();

    // Cancel all inflight entrants using the throwable.
    while (!inflightQueue.isEmpty()) {
      var entry = inflightQueue.poll();
      entry.callback.setException(fatal.asRuntimeException());
    }

    // Take care of all the blocked threads awaiting permits.
    Thread.sleep(ACQUIRE_QUANTUM_MS * 2);

    // Now that we are satisfied no more notifications are coming, loop
    // through them and cancel any pending batches via their callback.
    while (!notificationQueue.isEmpty()) {
      var entry = notificationQueue.poll();
      if (entry instanceof Batch batch) {
        batch.input.callback.setException(fatal.asRuntimeException());
      }
    }

    // Rethrow.
    throw fatal.asRuntimeException();
  }

  private void validate(InflightRecord record, AppendOutput output) {
    var numRecordsForAcknowledgement = output.endSeqNum() - output.startSeqNum();
    if (numRecordsForAcknowledgement != record.input.records.size()) {
      throw Status.INTERNAL
          .withDescription(
              "number of acknowledged records from S2 does not equal amount from first inflight batch")
          .asRuntimeException();
    }
  }

  private synchronized Void daemon() throws InterruptedException {
    logger.debug("append session daemon started");
    final var clientObserver =
        this.client.asyncStub.appendSession(
            new StreamObserver<>() {
              @Override
              public void onNext(AppendSessionResponse value) {
                notificationQueue.add(new Ack(AppendOutput.fromProto(value.getOutput())));
              }

              @Override
              public void onError(Throwable t) {
                notificationQueue.add(new Error(t));
              }

              @Override
              public void onCompleted() {
                notificationQueue.add(new ServerClose());
              }
            });

    if (!inflightQueue.isEmpty()) {
      logger.debug("Performing retransmission of {} batches.", inflightQueue.size());
      performInflightRecovery();
    }

    if (!inflightQueue.isEmpty()) {
      throw new RuntimeException("Corrupted inflight queue.");
    }

    while (this.acceptingAppends.get()
        || (!this.notificationQueue.isEmpty() || !this.inflightQueue.isEmpty())) {

      final long nanosToWait =
          this.nextDeadlineSystemNanos
              .get()
              .map(deadline -> deadline - System.nanoTime())
              .orElse(TimeUnit.NANOSECONDS.convert(Duration.ofHours(1)));

      final Notification notification =
          nanosToWait > 0 ? this.notificationQueue.poll(nanosToWait, TimeUnit.NANOSECONDS) : null;

      if (notification == null) {
        logger.debug("notification=NONE");
        if (!inflightQueue.isEmpty()) {
          var elapsed = Duration.ofNanos(System.nanoTime() - inflightQueue.peek().entryNanos);
          logger.warn("deadline hit, waited for {} ms", elapsed.toMillis());
          clientObserver.onError(Status.CANCELLED.asRuntimeException());
          throw Status.CANCELLED.asRuntimeException();
        }
      } else if (notification instanceof Batch batch) {
        logger.debug("notification=BATCH");
        if (!inflightQueue.offer(batch.input)) {
          throw Status.INTERNAL.asRuntimeException();
        } else {
          clientObserver.onNext(
              AppendSessionRequest.newBuilder()
                  .setInput(batch.input.input.toProto(this.client.streamName))
                  .build());

          // Reset the next deadline.
          this.nextDeadlineSystemNanos.set(
              Optional.of(
                  System.nanoTime()
                      + TimeUnit.NANOSECONDS.convert(this.client.config.requestTimeout)));
        }

      } else if (notification instanceof Ack ack) {
        logger.debug("notification=ACK");
        this.remainingAttempts.set(this.client.config.maxRetries);
        var correspondingInflight = inflightQueue.poll();
        if (correspondingInflight == null) {
          throw Status.INTERNAL.asRuntimeException();
        } else {
          validate(correspondingInflight, ack.output);
          correspondingInflight.callback.set(ack.output);
          this.inflightBytes.release(correspondingInflight.meteredBytes.intValue());

          // Reset the next deadline.
          this.nextDeadlineSystemNanos.set(
              Optional.ofNullable(this.inflightQueue.peek())
                  .map(
                      entry ->
                          entry.entryNanos
                              + TimeUnit.NANOSECONDS.convert(this.client.config.requestTimeout)));
        }
      } else if (notification instanceof Error error) {
        logger.debug("notification=ERROR");
        clientObserver.onError(Status.CANCELLED.asRuntimeException());
        throw new RuntimeException(error.throwable);

      } else if (notification instanceof ClientClose close) {
        logger.debug("notification=CLIENT_CLOSE,gracefully={}", close.gracefully);
        clientObserver.onCompleted();
        if (!close.gracefully) {
          return null;
        }
      } else if (notification instanceof ServerClose close) {
        logger.debug("notification=SERVER_CLOSE");
        if (acceptingAppends.get() || !inflightQueue.isEmpty() || !notificationQueue.isEmpty()) {
          throw Status.INTERNAL
              .withDescription("server closed without error while work remains")
              .asRuntimeException();
        }
      }
    }
    return null;
  }
}
