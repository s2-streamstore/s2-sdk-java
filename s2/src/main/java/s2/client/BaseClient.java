package s2.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.config.Config;

public abstract class BaseClient implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(BaseClient.class.getName());
  final Config config;
  final ManagedChannel channel;
  final ScheduledExecutorService executor;

  BaseClient(Config config, ManagedChannel channel, ScheduledExecutorService executor) {
    this.config = config;
    this.channel = channel;
    this.executor = executor;
  }

  public void close() throws Exception {
    channel.shutdown();
    executor.shutdown();
  }

  static boolean retryableStatus(Status status) {
    return switch (status.getCode()) {
      case UNKNOWN, DEADLINE_EXCEEDED, UNAVAILABLE -> true;
      default -> false;
    };
  }

  <T> ListenableFuture<T> withTimeout(Supplier<ListenableFuture<T>> op) {
    return Futures.catchingAsync(
        Futures.withTimeout(op.get(), config.requestTimeout, executor),
        TimeoutException.class,
        t -> {
          throw Status.DEADLINE_EXCEEDED
              .withDescription("sdk hit local timeout")
              .asRuntimeException();
        },
        executor);
  }

  <T> ListenableFuture<T> withStaticRetries(
      int remainingAttempts, Supplier<ListenableFuture<T>> op) {
    return Futures.catchingAsync(
        op.get(),
        Throwable.class,
        t -> {
          var status = Status.fromThrowable(t);
          if (remainingAttempts > 0 && retryableStatus(status)) {
            logger.debug("retrying err={}, remainingAttempts={}", t, remainingAttempts);
            return withStaticRetries(remainingAttempts - 1, op);
          } else {
            return Futures.immediateFailedFuture(t);
          }
        },
        executor);
  }
}
