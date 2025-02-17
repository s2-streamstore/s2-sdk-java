package s2.channel;

import io.grpc.ManagedChannel;

public final class AccountChannel extends AutoClosableManagedChannel
    implements AccountCompatibleChannel {

  AccountChannel(ManagedChannel managedChannel) {
    super(managedChannel);
  }

  @Override
  public AutoClosableManagedChannel getChannel() {
    return this;
  }
}
