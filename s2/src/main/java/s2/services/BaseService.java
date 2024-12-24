package s2.services;

import io.grpc.ManagedChannel;
import io.grpc.CallCredentials;

/**
 * Base class for all S2 services providing common channel and credentials management. This abstract
 * class handles the common functionality needed by all service implementations, including channel
 * management and credential handling.
 * 
 * <p>
 * Services are designed to be accessed through the {@link s2.services.Client} class, which manages
 * their lifecycle and ensures proper channel management. Direct instantiation of services is not
 * supported.
 * </p>
 */
public abstract class BaseService {
  /** The gRPC channel used for communication. */
  protected ManagedChannel channel;

  /** The credentials used for authentication. */
  protected final CallCredentials credentials;

  /**
   * Creates a new service instance with the specified channel and credentials. Package-private
   * constructor to ensure services are only created through the {@link s2.services.Client} class.
   *
   * @param channel The gRPC channel to use for communication
   * @param credentials The credentials to use for authentication, may be null for unauthenticated
   *        access
   */
  BaseService(ManagedChannel channel, CallCredentials credentials) {
    this.channel = channel;
    this.credentials = credentials;
  }

  /**
   * Updates the channel used by this service. Called when switching to a basin-specific channel.
   *
   * @param newChannel The new channel to use for communication
   */
  public void updateChannel(ManagedChannel newChannel) {
    this.channel = newChannel;
    onChannelUpdate();
  }

  /**
   * Called after channel update to reinitialize service-specific stubs. Implementations should use
   * this method to recreate their gRPC stubs with the new channel.
   */
  protected abstract void onChannelUpdate();
}
