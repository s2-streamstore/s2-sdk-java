package s2.services;

import io.grpc.ManagedChannel;
import io.grpc.CallCredentials;
import s2.channel.ChannelManager;
import s2.auth.BearerTokenCallCredentials;
import java.util.Optional;

/**
 * Builder for creating S2 clients with custom configuration. Provides a fluent API for setting
 * connection and authentication parameters.
 */
public class ClientBuilder {
  private Optional<String> host = Optional.empty();
  private int port = 443;
  private Optional<String> bearerToken = Optional.empty();
  private Optional<ManagedChannel> injectedChannel = Optional.empty();
  private Optional<CallCredentials> injectedCredentials = Optional.empty();

  /**
   * Package-private constructor. Use Client.newBuilder() to create instances.
   */
  ClientBuilder() {}

  /**
   * Sets the host to connect to. Required unless using an injected channel.
   *
   * @param host The host address (e.g. "aws.s2.dev")
   * @return this builder for chaining
   */
  public ClientBuilder host(String host) {
    this.host = Optional.ofNullable(host);
    return this;
  }

  /**
   * Sets the port to connect to. Optional, defaults to 443.
   *
   * @param port The port number
   * @return this builder for chaining
   */
  public ClientBuilder port(int port) {
    this.port = port;
    return this;
  }

  /**
   * Sets the bearer token for authentication. Required unless using injected credentials.
   *
   * @param token The bearer token
   * @return this builder for chaining
   */
  public ClientBuilder bearerToken(String token) {
    this.bearerToken = Optional.ofNullable(token);
    return this;
  }

  /**
   * Injects a pre-configured gRPC channel. Optional, if not provided a channel will be created
   * based on host/port. When using an injected channel, host/port settings are ignored.
   *
   * @param channel The channel to use
   * @return this builder for chaining
   */
  public ClientBuilder channel(ManagedChannel channel) {
    this.injectedChannel = Optional.ofNullable(channel);
    return this;
  }

  /**
   * Injects pre-configured credentials. Optional, if not provided credentials will be created from
   * the bearer token. When using injected credentials, bearer token setting is ignored.
   *
   * @param credentials The credentials to use
   * @return this builder for chaining
   */
  public ClientBuilder credentials(CallCredentials credentials) {
    this.injectedCredentials = Optional.ofNullable(credentials);
    return this;
  }

  /**
   * Builds a new S2 client with the configured parameters.
   *
   * @return A new client instance
   * @throws IllegalStateException if required parameters are missing or invalid
   */
  public Client build() {
    validate();

    ManagedChannel channel;
    ChannelManager channelManager = null;

    if (injectedChannel.isPresent()) {
      channel = injectedChannel.get();
    } else {
      channelManager = new ChannelManager(host.get(), port, true);
      channel = channelManager.createChannel();
    }

    CallCredentials credentials = createCredentials();
    return new Client(channel, credentials, channelManager);
  }

  /**
   * Validates the builder configuration.
   *
   * @throws IllegalStateException if the configuration is invalid
   */
  private void validate() {
    if (injectedChannel.isEmpty()) {
      host.filter(h -> !h.isEmpty())
          .orElseThrow(() -> new IllegalStateException("Host is required when not using an injected channel"));
      
      if (port <= 0) {
        throw new IllegalStateException("Port must be greater than 0");
      }
    }

    if (injectedCredentials.isEmpty() && bearerToken.filter(t -> !t.isEmpty()).isEmpty()) {
      throw new IllegalStateException("Bearer token is required when not using injected credentials");
    }
  }

  private CallCredentials createCredentials() {
    return injectedCredentials.orElseGet(() -> 
      new BearerTokenCallCredentials(
        bearerToken.orElseThrow(() -> new IllegalStateException("Bearer token is required"))
      )
    );
  }
}