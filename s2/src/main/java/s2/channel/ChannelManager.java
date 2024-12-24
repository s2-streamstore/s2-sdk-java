package s2.channel;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages gRPC channel lifecycle and configuration. This class is responsible for creating,
 * switching, and closing gRPC channels. It supports creating channels for both default and
 * basin-specific configurations.
 */
public class ChannelManager implements AutoCloseable {
  private static final Logger logger = Logger.getLogger(ChannelManager.class.getName());
  private final String originalHost;
  private final int port;
  private final boolean useTls;

  private Optional<ManagedChannel> currentChannel = Optional.empty();
  private Optional<ManagedChannel> previousChannel = Optional.empty();

  /**
   * Constructs a new ChannelManager with the specified configuration.
   *
   * @param host The host address for the gRPC server.
   * @param port The port number for the gRPC server.
   * @param useTls Whether to use TLS for secure communication.
   */
  public ChannelManager(String host, int port, boolean useTls) {
    this.originalHost = host;
    this.port = port;
    this.useTls = useTls;
  }

  /**
   * Creates a new channel with default configuration.
   *
   * @return A new ManagedChannel instance.
   */
  public ManagedChannel createChannel() {
    var channel = createChannelForHost(originalHost);
    currentChannel = Optional.of(channel);
    return channel;
  }

  /**
   * Creates a new channel for a specific basin.
   *
   * @param basinName The name of the basin.
   * @return A new ManagedChannel instance for the specified basin.
   */
  public ManagedChannel createBasinChannel(String basinName) {
    var basinHost = String.format("%s.b.%s", basinName, originalHost);
    return createChannelForHost(basinHost);
  }

  /**
   * Switches the active channel to a new basin-specific channel. If there's an existing channel, it
   * becomes the previous channel and is scheduled for shutdown.
   *
   * @param basinName The name of the basin to switch to.
   * @return The new ManagedChannel instance for the specified basin.
   */
  public ManagedChannel switchToBasin(String basinName) {
    currentChannel.ifPresent(channel -> {
      previousChannel.ifPresent(prev -> {
        try {
          prev.shutdown();
        } catch (Exception e) {
          logger.log(Level.WARNING, "Error shutting down previous channel", e);
        }
      });
      previousChannel = Optional.of(channel);
    });

    var newChannel = createBasinChannel(basinName);
    currentChannel = Optional.of(newChannel);
    return newChannel;
  }

  /**
   * Creates a ManagedChannel for the specified host.
   *
   * @param host The host address for the channel.
   * @return A new ManagedChannel instance.
   */
  private ManagedChannel createChannelForHost(String host) {
    var builder = ManagedChannelBuilder.forAddress(host, port);
    if (!useTls) {
      builder.usePlaintext();
    }
    return builder.build();
  }

  /**
   * Gets the current active channel.
   *
   * @return The current ManagedChannel instance.
   */
  public Optional<ManagedChannel> getCurrentChannel() {
    return currentChannel;
  }

  /**
   * Closes the ChannelManager, shutting down both the current and previous channels. This method is
   * called automatically when using try-with-resources.
   */
  @Override
  public void close() {
    try {
      previousChannel.ifPresent(channel -> {
        try {
          channel.shutdown();
          channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.log(Level.WARNING, "Interrupted while shutting down previous channel", e);
        }
      });

      currentChannel.ifPresent(channel -> {
        try {
          channel.shutdown();
          channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.log(Level.WARNING, "Interrupted while shutting down current channel", e);
        }
      });
    } catch (Exception e) {
      logger.log(Level.WARNING, "Error during channel manager shutdown", e);
    }
  }
}
