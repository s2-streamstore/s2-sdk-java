package s2.channel;

import io.grpc.ManagedChannel;
import java.util.concurrent.TimeUnit;

public class AutoClosableManagedChannel implements AutoCloseable {

  public final ManagedChannel managedChannel;

  public AutoClosableManagedChannel(ManagedChannel managedChannel) {
    this.managedChannel = managedChannel;
  }

  @Override
  public void close() throws Exception {
    managedChannel.shutdownNow();
    managedChannel.awaitTermination(5000, TimeUnit.MILLISECONDS);
  }
}
