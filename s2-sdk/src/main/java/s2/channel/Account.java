package s2.channel;

import io.grpc.ManagedChannel;

public final class Account extends AutoClosableManagedChannel implements AccountChannel {

  Account(ManagedChannel managedChannel) {
    super(managedChannel);
  }

  @Override
  public AutoClosableManagedChannel getChannel() {
    return this;
  }
}
