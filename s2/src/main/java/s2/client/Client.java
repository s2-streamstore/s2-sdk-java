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

public class Client extends BaseClient {

  private static final Logger logger = LoggerFactory.getLogger(Client.class.getName());
  private final AccountServiceGrpc.AccountServiceFutureStub futureStub;

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

  public Client(Config config, ManagedChannel channel, ScheduledExecutorService executor) {
    super(config, channel, executor);
    this.futureStub =
        AccountServiceGrpc.newFutureStub(channel)
            .withCallCredentials(new BearerTokenCallCredentials(config.token));
  }

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
