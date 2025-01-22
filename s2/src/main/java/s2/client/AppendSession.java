package s2.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.config.AppendRetryPolicy;
import s2.types.AppendInput;
import s2.types.AppendOutput;
import s2.v1alpha.AppendSessionRequest;
import s2.v1alpha.AppendSessionResponse;

public class AppendSession {

  private static final Logger logger = LoggerFactory.getLogger(AppendSession.class.getName());
  final ListeningScheduledExecutorService executor;
  final StreamClient client;
  final ArrayBlockingQueue<WrappedInput> input = new ArrayBlockingQueue<>(10);
  final ArrayBlockingQueue<WrappedInput> inflight = new ArrayBlockingQueue<>(1000);
  final ArrayBlockingQueue<AppendOutput> output = new ArrayBlockingQueue<>(10);
  final AtomicInteger remainingAttempts = new AtomicInteger(0);
  final LinkedBlockingQueue<AppendSessionEvent> notify =
      new LinkedBlockingQueue<>(Integer.MAX_VALUE);
  final AtomicBoolean hasCompleted = new AtomicBoolean(false);
  final Consumer<AppendOutput> outputCallback;
  final Consumer<Throwable> errorCallback;
  final ListenableFuture<Void> daemon;

  AppendSession(
      StreamClient client,
      Consumer<AppendOutput> outputCallback,
      Consumer<Throwable> errorCallback) {
    this.client = client;
    this.outputCallback = outputCallback;
    this.errorCallback = errorCallback;
    this.executor = MoreExecutors.listeningDecorator(this.client.executor);
    this.remainingAttempts.set(client.config.maxRetries);
    this.daemon = retrying();
  }

  private void validate(WrappedInput input, AppendOutput output) {
    var numRecordsForAcknowledgement = output.endSeqNum() - output.startSeqNum();
    if (numRecordsForAcknowledgement != input.input.records.size()) {
      throw Status.INTERNAL
          .withDescription(
              "number of acknowledged records from S2 does not equal amount from first inflight batch")
          .asRuntimeException();
    }
  }

  private ListenableFuture<Void> retrying() {
    return Futures.catchingAsync(
        this.executor.submit(this::sessionInner),
        Throwable.class,
        t -> {
          var status = Status.fromThrowable(t);
          if (remainingAttempts.getAndDecrement() > 0
              && BaseClient.retryableStatus(status)
              && this.client.config.appendRetryPolicy == AppendRetryPolicy.ALL) {
            logger.trace("Retrying append session");
            return retrying();
          } else {
            logger.trace("No longer retrying append session");
            errorCallback.accept(status.asRuntimeException());
            hasCompleted.set(true);
            this.input.clear();
            this.notify.clear();
            this.output.clear();
            return Futures.immediateFuture(null);
          }
        },
        this.executor);
  }

  public void submit(AppendInput input) throws InterruptedException {
    if (this.hasCompleted.get()) {
      throw new RuntimeException("AppendSession has already been closed or encountered an error");
    }
    this.input.put(new WrappedInput(input, System.nanoTime()));
    this.notify.put(AppendSessionEvent.INPUT);
  }

  public ListenableFuture<Void> close() {
    if (this.hasCompleted.getAndSet(true)) {
      this.input.clear();
      this.notify.clear();
      this.output.clear();
      return Futures.immediateFailedFuture(
          new RuntimeException("AppendSession has already been closed"));
    } else {
      this.notify.add(AppendSessionEvent.COMPLETE);
      return this.daemon;
    }
  }

  private Void sessionInner() {

    AtomicReference<Throwable> error = new AtomicReference<>();

    var observer =
        this.client.asyncStub.appendSession(
            new StreamObserver<AppendSessionResponse>() {

              @Override
              public void onNext(AppendSessionResponse value) {
                var appendOutput = AppendOutput.fromProto(value.getOutput());
                try {
                  output.put(appendOutput);
                  remainingAttempts.set(client.config.maxRetries);
                  notify.put(AppendSessionEvent.ACK);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public void onError(Throwable t) {
                error.set(t);
                try {
                  notify.put(AppendSessionEvent.ERROR);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public void onCompleted() {
                var alreadyDone = hasCompleted.getAndSet(true);
                if (!alreadyDone) {
                  throw Status.INTERNAL
                      .withDescription(
                          "session closed by S2 without error before being closed by client")
                      .asRuntimeException();
                }
                try {
                  notify.put(AppendSessionEvent.COMPLETE);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            });

    // Retransmit.
    var retransmitCount = inflight.size();
    inflight.forEach(
        input -> {
          observer.onNext(
              AppendSessionRequest.newBuilder()
                  .setInput(input.input.toProto(this.client.streamName))
                  .build());
        });

    if (retransmitCount > 0) {
      logger.info("retransmitted {} batches", retransmitCount);
    }

    var skippedInputNotifs = 0;
    while (!(hasCompleted.get() && (input.isEmpty() && inflight.isEmpty()))) {
      AppendSessionEvent event;
      var nextDeadline =
          Optional.ofNullable(inflight.peek())
              .map(head -> head.untilDeadline(this.client.config.requestTimeout))
              .orElse(Duration.ofDays(1));
      try {
        logger.trace("nextDeadline: {} ms", nextDeadline.toMillis());
        event = notify.poll(nextDeadline.toNanos(), TimeUnit.NANOSECONDS);
        logger.trace("Received event: {}", event);
      } catch (InterruptedException e) {
        throw Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException();
      }
      if (event == null) {
        Optional.ofNullable(inflight.peek())
            .ifPresent(
                awaiting -> {
                  throw Status.CANCELLED
                      .withDescription(
                          "append hit the configured local timeout while awaiting acknowledgement")
                      .asRuntimeException();
                });
      } else {
        switch (event) {
          case INPUT -> {
            var input = this.input.peek();
            if (inflight.offer(input)) {
              this.input.remove();
              observer.onNext(
                  AppendSessionRequest.newBuilder()
                      .setInput(input.input.toProto(this.client.streamName))
                      .build());
            } else {
              // Inflight queue already saturated. This situation can only be unblocked by
              // elements moving out of the inflight, which happens when an ACK is handled.
              //
              // So, we increment a counter, and on the next ACK handling, perform the (deferred)
              // handling of this input.
              skippedInputNotifs++;
            }
          }
          case ACK -> {
            var output = this.output.poll();
            var correspondingInput = this.inflight.poll();
            validate(correspondingInput, output);
            if (skippedInputNotifs > 0) {
              // Deferred inputs can now be handled.
              var input = this.input.poll();
              inflight.add(input);
              observer.onNext(
                  AppendSessionRequest.newBuilder()
                      .setInput(input.input.toProto(this.client.streamName))
                      .build());
              skippedInputNotifs--;
            }
            logger.debug("finished append in {} ms", correspondingInput.sinceStart().toMillis());
            this.outputCallback.accept(output);
          }
          case ERROR -> {
            var e = error.get();
            logger.warn("remote error {}", e.toString());
            throw Status.fromThrowable(e).asRuntimeException();
          }
          case COMPLETE -> hasCompleted.set(true);
        }
      }
    }

    return null;
  }

  enum AppendSessionEvent {
    INPUT,
    ACK,
    ERROR,
    COMPLETE,
  }

  private record WrappedInput(AppendInput input, long instantNanos) {

    Duration untilDeadline(Duration timeout) {
      var now = System.nanoTime();
      var deadlineNanos = instantNanos + timeout.toNanos();
      return Duration.ofNanos(deadlineNanos - now);
    }

    Duration sinceStart() {
      return Duration.ofNanos(System.nanoTime() - instantNanos);
    }
  }
}
