package s2.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.channel.AutoClosableManagedChannel;
import s2.config.Config;

public abstract class BaseClient implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(BaseClient.class.getName());
  final Config config;
  final AutoClosableManagedChannel channel;
  final ScheduledExecutorService executor;
  final boolean ownedChannel;
  final boolean ownedExecutor;

  BaseClient(
      Config config,
      AutoClosableManagedChannel channel,
      ScheduledExecutorService executor,
      boolean ownedChannel,
      boolean ownedExecutor) {
    this.config = config;
    this.channel = channel;
    this.executor = executor;
    this.ownedChannel = ownedChannel;
    this.ownedExecutor = ownedExecutor;
  }

  static ScheduledExecutorService defaultExecutor(String name) {
    return Executors.newScheduledThreadPool(
        Runtime.getRuntime().availableProcessors(),
        new ThreadFactory() {
          private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

          @Override
          public Thread newThread(Runnable r) {
            Thread thread = defaultFactory.newThread(r);
            thread.setDaemon(true);
            thread.setName(String.format("S2-%s-%s", name, thread.getId()));
            return thread;
          }
        });
  }

  static boolean retryableStatus(Status status) {
    return switch (status.getCode()) {
      case UNKNOWN, DEADLINE_EXCEEDED, UNAVAILABLE -> true;
      default -> false;
    };
  }

  public void close() {
    if (this.ownedChannel) {
      this.channel.close();
    }
    if (this.ownedExecutor) {
      this.executor.shutdown();
    }
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
            logger.debug(
                "retrying err={} after {} delay, remainingAttempts={}",
                status.getCode(),
                config.retryDelay,
                remainingAttempts);
            return Futures.scheduleAsync(
                () -> withStaticRetries(remainingAttempts - 1, op),
                config.retryDelay,
                this.executor);
          } else {
            return Futures.immediateFailedFuture(t);
          }
        },
        executor);
  }
}
