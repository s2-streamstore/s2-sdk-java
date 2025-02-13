package s2.channel;

public interface BasinChannel {
  AutoClosableManagedChannel getChannel();
}
