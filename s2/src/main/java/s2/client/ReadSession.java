package s2.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.types.Batch;
import s2.types.ReadOutput;
import s2.types.ReadSessionRequest;
import s2.v1alpha.ReadSessionResponse;

public class ReadSession {

  private static final Logger logger = LoggerFactory.getLogger(ReadSession.class.getName());
  final ScheduledExecutorService executor;
  final StreamClient client;
  final AtomicLong nextStartSeqNum;
  final AtomicLong consumedRecords = new AtomicLong();
  final AtomicLong consumedBytes = new AtomicLong(0);
  final Consumer<ReadOutput> onResponse;
  final Consumer<Throwable> onError;
  final ReadSessionRequest request;
  final AtomicBoolean hasCompleted = new AtomicBoolean(false);
  final AtomicInteger remainingAttempts;
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
    this.nextStartSeqNum = new AtomicLong(request.startSeqNum);
    this.remainingAttempts = new AtomicInteger(client.config.maxRetries);
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
            if (hasCompleted.get()) {
              logger.info("Read session onNext but hasCompleted.");
              fut.set(null);
            } else {
              innerOnResponse.accept(ReadOutput.fromProto(value.getOutput()));
            }
          }

          @Override
          public void onError(Throwable t) {
            logger.info("Read session onError={}", t.toString());
            fut.setException(t);
          }

          @Override
          public void onCompleted() {
            logger.info("Read session inner onCompleted");
            fut.set(null);
          }
        });
    return fut;
  }

  private ListenableFuture<Void> retrying() {

    return Futures.catchingAsync(
        readSessionInner(
            request.update(nextStartSeqNum.get(), consumedRecords.get(), consumedBytes.get()),
            resp -> {
              if (resp instanceof Batch batch) {
                var lastRecordIdx = batch.lastSeqNum();
                lastRecordIdx.ifPresent(v -> nextStartSeqNum.set(v + 1));
                consumedRecords.addAndGet(batch.sequencedRecordBatch().records().size());
                consumedBytes.addAndGet(batch.meteredBytes());
              }
              this.remainingAttempts.set(client.config.maxRetries);
              this.onResponse.accept(resp);
            }),
        Throwable.class,
        t -> {
          var status = Status.fromThrowable(t);
          if (remainingAttempts.getAndDecrement() > 0
              && BaseClient.retryableStatus(status)
              && !this.hasCompleted.get()) {
            logger.debug("readSession retrying, status={}", status);
            return retrying();
          } else {
            logger.debug("readSession failed, status={}", status);
            onError.accept(t);
            return Futures.immediateFuture(null);
          }
        },
        executor);
  }

  public ListenableFuture<Void> awaitCompletion() {
    return this.daemon;
  }

  public ListenableFuture<Void> close() {
    if (this.hasCompleted.getAndSet(true)) {
      return Futures.immediateFailedFuture(new RuntimeException("ReadSession already closed."));
    } else {
      return this.daemon;
    }
  }
}
