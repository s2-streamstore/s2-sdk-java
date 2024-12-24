package s2.services;

import io.grpc.ManagedChannel;
import io.grpc.CallCredentials;
import s2.v1alpha.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Service client for account-related operations. Provides methods for managing basins, including
 * creation, deletion, listing, and configuration. All operations require appropriate authentication
 * credentials.
 */
public class AccountService extends BaseService {
  private AccountServiceGrpc.AccountServiceBlockingStub stub;

  /**
   * Creates a new AccountService instance. Package-private constructor to ensure services are only
   * created through the {@link s2.services.Client} class.
   *
   * @param channel The gRPC channel to use for communication
   * @param credentials The credentials to use for authentication
   */
  AccountService(ManagedChannel channel, CallCredentials credentials) {
    super(channel, credentials);
    this.stub = AccountServiceGrpc.newBlockingStub(channel);
    if (credentials != null) {
      this.stub = this.stub.withCallCredentials(credentials);
    }
  }

  @Override
  protected void onChannelUpdate() {
    initializeStub();
  }

  private void initializeStub() {
    var baseStub = AccountServiceGrpc.newBlockingStub(channel);
    if (credentials != null) {
      this.stub = baseStub.withCallCredentials(credentials);
    } else {
      this.stub = baseStub;
    }
  }

  /**
   * Lists all basins with the given prefix. This method handles pagination automatically and
   * aggregates all results.
   *
   * @param prefix The prefix to filter basins by. Use empty string to list all basins.
   * @return A list of all basins matching the prefix
   * @throws io.grpc.StatusRuntimeException if the request fails
   */
  public List<BasinInfo> listBasins(String prefix) {
    var request = ListBasinsRequest.newBuilder().setPrefix(prefix).build();
    var basins = new ArrayList<BasinInfo>();
    ListBasinsResponse response;
    do {
      response = stub.listBasins(request);
      basins.addAll(response.getBasinsList());
    } while (response.getHasMore());
    return basins;
  }

  /**
   * Creates a new basin with the specified configuration.
   *
   * @param name The name of the basin to create. Must be unique within the account.
   * @param config The configuration for the new basin. Must not be null.
   * @return Information about the newly created basin
   * @throws io.grpc.StatusRuntimeException if the basin already exists or the request fails
   * @throws NullPointerException if config is null
   */
  public BasinInfo createBasin(String name, BasinConfig config) {
    var request = CreateBasinRequest.newBuilder().setBasin(name).setConfig(config).build();
    return stub.createBasin(request).getInfo();
  }

  /**
   * Deletes a basin and all its contents. This operation is irreversible and will delete all
   * streams within the basin.
   *
   * @param name The name of the basin to delete
   * @throws io.grpc.StatusRuntimeException if the basin doesn't exist or the request fails
   */
  public void deleteBasin(String name) {
    var request = DeleteBasinRequest.newBuilder().setBasin(name).build();
    stub.deleteBasin(request);
  }

  /**
   * Reconfigures an existing basin with new settings. This operation may affect the behavior and
   * performance characteristics of the basin.
   *
   * @param name The name of the basin to reconfigure
   * @param config The new configuration to apply. Must not be null.
   * @throws io.grpc.StatusRuntimeException if the basin doesn't exist or the request fails
   * @throws NullPointerException if config is null
   */
  public void reconfigureBasin(String name, BasinConfig config) {
    var request = ReconfigureBasinRequest.newBuilder().setBasin(name).setConfig(config).build();
    stub.reconfigureBasin(request);
  }

  /**
   * Retrieves the current configuration of a basin.
   *
   * @param name The name of the basin to get configuration for
   * @return The current configuration of the basin
   * @throws io.grpc.StatusRuntimeException if the basin doesn't exist or the request fails
   */
  public BasinConfig getBasinConfig(String name) {
    var request = GetBasinConfigRequest.newBuilder().setBasin(name).build();
    return stub.getBasinConfig(request).getConfig();
  }
}
