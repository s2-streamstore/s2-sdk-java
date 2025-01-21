package s2.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.stub.MetadataUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import s2.auth.BearerTokenCallCredentials;
import s2.config.Config;
import s2.v1alpha.BasinServiceGrpc;
import s2.v1alpha.ListStreamsRequest;
import s2.v1alpha.StreamInfo;

public class BasinClient extends BaseClient {
  private final String basin;
  private final BasinServiceGrpc.BasinServiceFutureStub futureStub;

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

  public BasinClient(String basin, Config config, ScheduledExecutorService executor) {
    this(
        basin,
        config,
        ManagedChannelBuilder.forTarget(config.endpoints.basin.toTarget(basin)).build(),
        executor);
  }

  public ListenableFuture<List<StreamInfo>> listStreams(
      String prefix, String startAfter, List<StreamInfo> accumulator) {
    return Futures.transformAsync(
        this.futureStub.listStreams(
            ListStreamsRequest.newBuilder().setPrefix(prefix).setStartAfter(startAfter).build()),
        resp -> {
          accumulator.addAll(resp.getStreamsList());
          if (resp.getHasMore()) {
            var newStartAfter = resp.getStreams(resp.getStreamsCount() - 1).getName();
            return listStreams(prefix, newStartAfter, accumulator);
          } else {
            return Futures.immediateFuture(accumulator);
          }
        },
        executor);
  }

  public ListenableFuture<List<StreamInfo>> listStreams(String prefix) {
    return listStreams(prefix, "", new ArrayList<>());
  }

  public StreamClient streamClient(String streamName) {
    return new StreamClient(streamName, this.basin, this.config, this.channel, this.executor);
  }
}
