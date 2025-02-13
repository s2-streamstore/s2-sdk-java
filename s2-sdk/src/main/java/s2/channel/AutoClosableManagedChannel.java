package s2.channel;

import io.grpc.ManagedChannel;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoClosableManagedChannel implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(AutoClosableManagedChannel.class);
  public final ManagedChannel managedChannel;

  public AutoClosableManagedChannel(ManagedChannel managedChannel) {
    this.managedChannel = managedChannel;
  }

  @Override
  public void close() {
    managedChannel.shutdown();
    try {
      if (!managedChannel.awaitTermination(5, TimeUnit.SECONDS)) {
        managedChannel.shutdownNow();
        if (!managedChannel.awaitTermination(5, TimeUnit.SECONDS)) {
          logger.warn("Channel did not terminate within 10s total");
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      managedChannel.shutdownNow();
    }
  }
}
