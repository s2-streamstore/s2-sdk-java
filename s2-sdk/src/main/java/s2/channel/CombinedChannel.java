package s2.channel;

import io.grpc.ManagedChannel;

public final class CombinedChannel extends AutoClosableManagedChannel
    implements AccountCompatibleChannel, BasinCompatibleChannel {

  CombinedChannel(ManagedChannel managedChannel) {
    super(managedChannel);
  }

  @Override
  public AutoClosableManagedChannel getChannel() {
    return this;
  }
}
