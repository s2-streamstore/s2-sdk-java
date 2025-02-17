package s2.channel;

public interface BasinCompatibleChannel {
  AutoClosableManagedChannel getChannel();
}
