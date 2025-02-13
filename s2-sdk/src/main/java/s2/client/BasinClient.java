package s2.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.stub.MetadataUtils;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import s2.auth.BearerTokenCallCredentials;
import s2.channel.BasinChannel;
import s2.channel.ManagedChannelFactory;
import s2.config.Config;
import s2.types.CreateStreamRequest;
import s2.types.Paginated;
import s2.types.ReconfigureStreamRequest;
import s2.types.StreamConfig;
import s2.types.StreamInfo;
import s2.v1alpha.BasinServiceGrpc;
import s2.v1alpha.DeleteStreamRequest;
import s2.v1alpha.GetStreamConfigRequest;

/** Client for basin-level operations. */
public class BasinClient extends BaseClient {
  /** Name of basin associated with this client. */
  final String basin;

  private final BasinServiceGrpc.BasinServiceFutureStub futureStub;

  BasinClient(
      Config config,
      String basin,
      BasinChannel channel,
      ScheduledExecutorService executor,
      boolean ownedChannel,
      boolean ownedExecutor) {
    super(config, channel.getChannel(), executor, ownedChannel, ownedExecutor);
    var meta = new Metadata();
    meta.put(Key.of("s2-basin", Metadata.ASCII_STRING_MARSHALLER), basin);
    this.basin = basin;
    this.futureStub =
        BasinServiceGrpc.newFutureStub(channel.getChannel().managedChannel)
            .withCallCredentials(new BearerTokenCallCredentials(config.token))
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
  }

  public static BasinClientBuilder newBuilder(Config config, String basin) {
    return new BasinClientBuilder(config, basin);
  }

  /**
   * List streams within the basin.
   *
   * @param listStreamsRequest the list streams request
   * @return future of a paginated list of stream infos
   */
  public ListenableFuture<Paginated<StreamInfo>> listStreams(
      s2.types.ListStreamsRequest listStreamsRequest) {
    return withTimeout(
        () ->
            Futures.transform(
                withStaticRetries(
                    config.maxRetries,
                    () -> this.futureStub.listStreams(listStreamsRequest.toProto())),
                resp ->
                    new Paginated<>(
                        resp.getHasMore(),
                        resp.getStreamsList().stream().map(StreamInfo::fromProto).toList()),
                executor));
  }

  /**
   * Create a new stream within this basin.
   *
   * @param createStreamRequest the create stream request
   * @return future of the resulting stream info
   */
  public ListenableFuture<StreamInfo> createStream(CreateStreamRequest createStreamRequest) {
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
                            .createStream(createStreamRequest.toProto())),
                resp -> StreamInfo.fromProto(resp.getInfo()),
                executor));
  }

  /**
   * Delete a stream.
   *
   * <p>Stream deletion is asynchronous, and may take a few minutes to complete.
   *
   * @param streamName the stream name
   * @return future representing the completion of this action
   */
  public ListenableFuture<Void> deleteStream(String streamName) {
    return withTimeout(
        () ->
            Futures.transform(
                withStaticRetries(
                    config.maxRetries,
                    () ->
                        this.futureStub.deleteStream(
                            DeleteStreamRequest.newBuilder().setStream(streamName).build())),
                resp -> null,
                executor));
  }

  /**
   * Get current config of a stream.
   *
   * @param streamName the stream name
   * @return future of the stream config
   */
  public ListenableFuture<StreamConfig> getStreamConfig(String streamName) {
    return withTimeout(
        () ->
            Futures.transform(
                withStaticRetries(
                    config.maxRetries,
                    () ->
                        this.futureStub.getStreamConfig(
                            GetStreamConfigRequest.newBuilder().setStream(streamName).build())),
                resp -> StreamConfig.fromProto(resp.getConfig()),
                executor));
  }

  /**
   * Reconfigure an existing stream.
   *
   * @param reconfigureStreamRequest the reconfigure stream request
   * @return future of the resulting stream config
   */
  public ListenableFuture<StreamConfig> reconfigureStream(
      ReconfigureStreamRequest reconfigureStreamRequest) {
    return withTimeout(
        () ->
            Futures.transform(
                withStaticRetries(
                    config.maxRetries,
                    () -> this.futureStub.reconfigureStream(reconfigureStreamRequest.toProto())),
                resp -> StreamConfig.fromProto(resp.getConfig()),
                executor));
  }

  public static class BasinClientBuilder {
    private final Config config;
    private final String basin;
    private Optional<BasinChannel> channel = Optional.empty();
    private Optional<ScheduledExecutorService> executor = Optional.empty();

    BasinClientBuilder(Config config, String basin) {
      this.config = config;
      this.basin = basin;
    }

    public BasinClientBuilder withChannel(BasinChannel channel) {
      this.channel = Optional.of(channel);
      return this;
    }

    public BasinClientBuilder withExecutor(ScheduledExecutorService executor) {
      this.executor = Optional.of(executor);
      return this;
    }

    public BasinClient build() {
      return new BasinClient(
          this.config,
          this.basin,
          this.channel.orElseGet(
              () -> ManagedChannelFactory.forBasinOrStreamService(this.config, this.basin)),
          this.executor.orElseGet(() -> BaseClient.defaultExecutor("basinClient")),
          this.channel.isEmpty(),
          this.executor.isEmpty());
    }
  }
}
