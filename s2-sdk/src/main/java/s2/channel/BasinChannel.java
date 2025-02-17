package s2.channel;

import io.grpc.ManagedChannel;

public final class BasinChannel extends AutoClosableManagedChannel
    implements BasinCompatibleChannel {

  BasinChannel(ManagedChannel channel) {
    super(channel);
  }

  @Override
  public AutoClosableManagedChannel getChannel() {
    return this;
  }
}
