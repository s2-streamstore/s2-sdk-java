package s2.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.stub.MetadataUtils;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import s2.auth.BearerTokenCallCredentials;
import s2.config.Config;
import s2.types.CreateStreamRequest;
import s2.types.Paginated;
import s2.types.ReconfigureStreamRequest;
import s2.types.StreamConfig;
import s2.types.StreamInfo;
import s2.v1alpha.BasinServiceGrpc;
import s2.v1alpha.DeleteStreamRequest;
import s2.v1alpha.GetStreamConfigRequest;

public class BasinClient extends BaseClient {
  private final String basin;
  private final BasinServiceGrpc.BasinServiceFutureStub futureStub;

  public BasinClient(String basin, Config config, ScheduledExecutorService executor) {
    this(
        basin,
        config,
        ManagedChannelBuilder.forTarget(config.endpoints.basin.toTarget(basin)).build(),
        executor);
  }

  public BasinClient(
      String basin, Config config, ManagedChannel channel, ScheduledExecutorService executor) {
    super(config, channel, executor);
    var meta = new Metadata();
    meta.put(Key.of("s2-basin", Metadata.ASCII_STRING_MARSHALLER), basin);
    this.basin = basin;
    this.futureStub =
        BasinServiceGrpc.newFutureStub(channel)
            .withCallCredentials(new BearerTokenCallCredentials(config.token))
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
  }

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

  public StreamClient streamClient(String streamName) {
    return new StreamClient(streamName, this.basin, this.config, this.channel, this.executor);
  }
}
