package s2.channel;

public interface AccountChannel {
  AutoClosableManagedChannel getChannel();
}
