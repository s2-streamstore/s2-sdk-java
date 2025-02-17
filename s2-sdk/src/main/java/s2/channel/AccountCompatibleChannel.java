package s2.channel;

public interface AccountCompatibleChannel {
  AutoClosableManagedChannel getChannel();
}
