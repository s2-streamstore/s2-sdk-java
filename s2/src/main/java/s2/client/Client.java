package s2.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.stub.MetadataUtils;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.auth.BearerTokenCallCredentials;
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

  /**
   * Instantiates a new Client, using default settings for creating a channel, as well as an
   * executor service.
   *
   * @param config the config
   */
  public Client(Config config) {
    this(
        config,
        ManagedChannelBuilder.forAddress(
                config.endpoints.account.host, config.endpoints.account.port)
            .build(),
        Executors.newScheduledThreadPool(
            Runtime.getRuntime().availableProcessors(),
            new ThreadFactory() {
              private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

              @Override
              public Thread newThread(Runnable r) {
                Thread thread = defaultFactory.newThread(r);
                thread.setDaemon(true);
                thread.setName("S2Client-" + thread.getId());
                return thread;
              }
            }));
  }

  /**
   * Instantiates a new Client.
   *
   * <p>Note that the executor is <b>not</b> the same as that used internally by netty for grpc
   * (which, for the moment, is not controllable by clients). This executor is used for other async
   * calls initiated by the SDK, such as application-level retries, timeouts, and transformations.
   *
   * <p>The executor used by this Client class will be shared with any BasinClient constructed from
   * it (and, similarly, will be used for any StreamClient constructed from the BasinClient).
   *
   * @param config the config
   * @param channel the channel
   * @param executor the executor
   */
  public Client(Config config, ManagedChannel channel, ScheduledExecutorService executor) {
    super(config, channel, executor);
    this.futureStub =
        AccountServiceGrpc.newFutureStub(channel)
            .withCallCredentials(new BearerTokenCallCredentials(config.token));
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
                        resp.getBasinsList().stream().map(BasinInfo::fromProto).toList()),
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

  /**
   * Create a BasinClient for interacting with basin-level RPCs.
   *
   * <p>The generated client will use the same channel if possible.
   *
   * @param basin the basin
   * @return the basin client
   */
  public BasinClient basinClient(String basin) {
    // If the basin endpoint identical to account, reuse the connection.
    if (this.config.endpoints.singleEndpoint()) {
      return new BasinClient(basin, this.config, this.channel, this.executor);
    } else {
      return new BasinClient(
          basin,
          this.config,
          ManagedChannelBuilder.forTarget(config.endpoints.basin.toTarget(basin)).build(),
          this.executor);
    }
  }
}
