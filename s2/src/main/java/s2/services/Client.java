package s2.services;

import io.grpc.ManagedChannel;
import io.grpc.CallCredentials;
import s2.channel.ChannelManager;
import java.util.Optional;

/**
 * Main facade for S2 services. Delegates to specialized service clients. This class provides a
 * simplified interface to all S2 functionality while maintaining separation of concerns internally.
 * It manages the lifecycle of the gRPC channel and coordinates between different service clients.
 *
 * <p>
 * Example usage:
 * 
 * <pre>{@code
 * // Create a client
 * Client client = Client.newBuilder().host("aws.s2.dev").port(443).bearerToken("token").build();
 *
 * // Use account operations (these don't require basin selection)
 * List<BasinInfo> basins = client.account().listBasins("prefix");
 * 
 * // Switch to a specific basin before using basin or stream operations
 * client.useBasin("myBasin");
 * 
 * // Now use basin-specific operations
 * List<StreamInfo> streams = client.basin().listStreams("prefix");
 * 
 * // And stream operations
 * client.stream().append("mystream", records);
 * client.streamAsync().appendAsync("mystream", records);
 *
 * // Switch to a different basin
 * client.useBasin("otherBasin");
 * client.stream().append("otherstream", records);
 *
 * // Don't forget to close the client when done
 * client.close();
 * }</pre>
 */
public class Client implements AutoCloseable {
  private final AccountService accountService;
  private final BasinService basinService;
  private final StreamService streamService;
  private final AsyncStreamService asyncStreamService;
  private final Optional<ChannelManager> channelManager;

  /**
   * Package-private constructor used by ClientBuilder. Clients should use Client.newBuilder() to
   * create instances.
   *
   * @param channel The gRPC channel to use for communication
   * @param credentials The credentials to use for authentication
   * @param channelManager The channel manager for handling channel lifecycle, or null if using an
   *        injected channel
   */
  Client(ManagedChannel channel, CallCredentials credentials, ChannelManager channelManager) {
    this.channelManager = Optional.ofNullable(channelManager);
    this.accountService = new AccountService(channel, credentials);
    this.basinService = new BasinService(channel, credentials);
    this.streamService = new StreamService(channel, credentials);
    this.asyncStreamService = new AsyncStreamService(channel, credentials);
  }

  /**
   * Creates a new client builder.
   *
   * @return A new builder instance for creating S2 clients
   */
  public static ClientBuilder newBuilder() {
    return new ClientBuilder();
  }

  /**
   * Gets the account service client for managing basins.
   *
   * @return The account service client
   */
  public AccountService account() {
    return accountService;
  }

  /**
   * Gets the basin service client for managing streams within a basin.
   *
   * @return The basin service client
   */
  public BasinService basin() {
    return basinService;
  }

  /**
   * Gets the synchronous stream service client.
   *
   * @return The stream service client for synchronous operations
   */
  public StreamService stream() {
    return streamService;
  }

  /**
   * Gets the asynchronous stream service client.
   *
   * @return The stream service client for asynchronous operations
   */
  public AsyncStreamService streamAsync() {
    return asyncStreamService;
  }

  /**
   * Switches to a basin-specific channel. This operation creates a new channel for communicating
   * with a specific basin and updates all service clients to use this new channel.
   *
   * @param basinName The name of the basin to connect to
   * @throws UnsupportedOperationException if using an injected channel
   */
  public void useBasin(String basinName) {
    ManagedChannel newChannel = channelManager
        .orElseThrow(() -> new UnsupportedOperationException(
            "Basin switching is not supported when using an injected channel. "
            + "Create a new client with a basin-specific channel instead."))
        .switchToBasin(basinName);

    basinService.updateChannel(newChannel);
    streamService.updateChannel(newChannel);
    asyncStreamService.updateChannel(newChannel);
  }

  /**
   * Closes the client and releases all resources. This includes shutting down all managed channels.
   * If using an injected channel, only the service clients are cleaned up.
   */
  @Override
  public void close() {
    channelManager.ifPresent(ChannelManager::close);
  }
}
