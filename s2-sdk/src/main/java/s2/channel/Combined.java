package s2.channel;

import io.grpc.ManagedChannel;

public final class Combined extends AutoClosableManagedChannel
    implements AccountChannel, BasinChannel {

  Combined(ManagedChannel managedChannel) {
    super(managedChannel);
  }

  @Override
  public AutoClosableManagedChannel getChannel() {
    return this;
  }
}
