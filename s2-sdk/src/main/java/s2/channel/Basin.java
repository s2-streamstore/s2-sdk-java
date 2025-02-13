package s2.channel;

import io.grpc.ManagedChannel;

public final class Basin extends AutoClosableManagedChannel implements BasinChannel {

  Basin(ManagedChannel channel) {
    super(channel);
  }

  @Override
  public AutoClosableManagedChannel getChannel() {
    return this;
  }
}
