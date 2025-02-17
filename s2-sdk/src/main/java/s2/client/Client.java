package s2.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.stub.MetadataUtils;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.auth.BearerTokenCallCredentials;
import s2.channel.AccountCompatibleChannel;
import s2.channel.ManagedChannelFactory;
import s2.config.Config;
import s2.types.BasinConfig;
import s2.types.BasinInfo;
import s2.types.CreateBasinRequest;
import s2.types.Paginated;
import s2.types.ReconfigureBasinRequest;
import s2.types.StreamConfig;
import s2.v1alpha.AccountServiceGrpc;
import s2.v1alpha.DeleteBasinRequest;
import s2.v1alpha.GetBasinConfigRequest;

/** Client for account-level operations. */
public class Client extends BaseClient {

  private static final Logger logger = LoggerFactory.getLogger(Client.class.getName());
  private final AccountServiceGrpc.AccountServiceFutureStub futureStub;

  private Client(
      Config config,
      AccountCompatibleChannel channel,
      ScheduledExecutorService executor,
      boolean ownedChannel,
      boolean ownedClient) {
    super(config, channel.getChannel(), executor, ownedChannel, ownedClient);
    this.futureStub =
        AccountServiceGrpc.newFutureStub(channel.getChannel().managedChannel)
            .withCallCredentials(new BearerTokenCallCredentials(config.token));
  }

  public static ClientBuilder newBuilder(Config config) {
    return new ClientBuilder(config);
  }

  /**
   * List basins.
   *
   * @param request the request
   * @return future of a paginated list of basin infos
   */
  public ListenableFuture<Paginated<BasinInfo>> listBasins(s2.types.ListBasinsRequest request) {
    return withTimeout(
        () ->
            Futures.transform(
                withStaticRetries(
                    config.maxRetries, () -> this.futureStub.listBasins(request.toProto())),
                resp ->
                    new Paginated<>(
                        resp.getHasMore(),
                        resp.getBasinsList().stream()
                            .map(BasinInfo::fromProto)
                            .collect(Collectors.toList())),
                executor));
  }

  /**
   * Create a new basin.
   *
   * @param request the creation request
   * @return future of the resulting basin's info
   */
  public ListenableFuture<BasinInfo> createBasin(CreateBasinRequest request) {
    final var meta = new Metadata();
    final var token = UUID.randomUUID().toString();
    meta.put(Key.of("s2-request-token", Metadata.ASCII_STRING_MARSHALLER), token);
    return withTimeout(
        () ->
            Futures.transform(
                withStaticRetries(
                    config.maxRetries,
                    () ->
                        this.futureStub
                            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta))
                            .createBasin(request.toProto())),
                resp -> BasinInfo.fromProto(resp.getInfo()),
                executor));
  }

  /**
   * Delete a basin.
   *
   * <p>Basin deletion is asynchronous, and may take a few minutes to complete.
   *
   * @param basin the basin
   * @return future representing the completion of the delete call
   */
  public ListenableFuture<Void> deleteBasin(String basin) {
    return withTimeout(
        () ->
            Futures.transform(
                withStaticRetries(
                    config.maxRetries,
                    () ->
                        this.futureStub.deleteBasin(
                            DeleteBasinRequest.newBuilder().setBasin(basin).build())),
                resp -> null,
                executor));
  }

  /**
   * Update configuration of an existing basin.
   *
   * @param reconfigure the reconfigure request
   * @return future of the updated configuration
   */
  public ListenableFuture<BasinConfig> reconfigureBasin(ReconfigureBasinRequest reconfigure) {
    return withTimeout(
        () ->
            Futures.transform(
                withStaticRetries(
                    config.maxRetries,
                    () -> this.futureStub.reconfigureBasin(reconfigure.toProto())),
                resp ->
                    new BasinConfig(
                        StreamConfig.fromProto(resp.getConfig().getDefaultStreamConfig())),
                executor));
  }

  /**
   * Get a basin's config.
   *
   * @param basin the basin
   * @return future of the basin configuration
   */
  public ListenableFuture<BasinConfig> getBasinConfig(String basin) {
    return withTimeout(
        () ->
            Futures.transform(
                withStaticRetries(
                    config.maxRetries,
                    () ->
                        this.futureStub.getBasinConfig(
                            GetBasinConfigRequest.newBuilder().setBasin(basin).build())),
                resp ->
                    new BasinConfig(
                        StreamConfig.fromProto(resp.getConfig().getDefaultStreamConfig())),
                executor));
  }

  public static class ClientBuilder {

    private final Config config;
    private Optional<AccountCompatibleChannel> channel = Optional.empty();
    private Optional<ScheduledExecutorService> executor = Optional.empty();

    public ClientBuilder(Config config) {
      this.config = config;
    }

    public ClientBuilder withChannel(AccountCompatibleChannel channel) {
      this.channel = Optional.of(channel);
      return this;
    }

    public ClientBuilder withExecutor(ScheduledExecutorService executor) {
      this.executor = Optional.of(executor);
      return this;
    }

    public Client build() {
      return new Client(
          this.config,
          this.channel.orElseGet(() -> ManagedChannelFactory.forAccountService(this.config)),
          this.executor.orElseGet(() -> BaseClient.defaultExecutor("client")),
          this.channel.isEmpty(),
          this.executor.isEmpty());
    }
  }
}
