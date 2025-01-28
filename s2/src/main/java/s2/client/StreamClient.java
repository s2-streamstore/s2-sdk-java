package s2.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.auth.BearerTokenCallCredentials;
import s2.config.Config;
import s2.types.AppendInput;
import s2.types.AppendOutput;
import s2.types.ReadOutput;
import s2.types.ReadRequest;
import s2.types.ReadSessionRequest;
import s2.v1alpha.AppendRequest;
import s2.v1alpha.AppendSessionRequest;
import s2.v1alpha.AppendSessionResponse;
import s2.v1alpha.CheckTailRequest;
import s2.v1alpha.CheckTailResponse;
import s2.v1alpha.StreamServiceGrpc;
import s2.v1alpha.StreamServiceGrpc.StreamServiceFutureStub;
import s2.v1alpha.StreamServiceGrpc.StreamServiceStub;

public class StreamClient extends BasinClient {

  private static final Logger logger = LoggerFactory.getLogger(StreamClient.class.getName());
  final String streamName;
  final StreamServiceFutureStub futureStub;
  final StreamServiceStub asyncStub;

  StreamClient(
      String streamName,
      String basin,
      Config config,
      ManagedChannel channel,
      ScheduledExecutorService executor) {
    super(basin, config, channel, executor);
    var meta = new Metadata();
    meta.put(Key.of("s2-basin", Metadata.ASCII_STRING_MARSHALLER), basin);
    this.streamName = streamName;
    this.futureStub =
        StreamServiceGrpc.newFutureStub(channel)
            .withCallCredentials(new BearerTokenCallCredentials(config.token))
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
    this.asyncStub =
        StreamServiceGrpc.newStub(channel)
            .withCallCredentials(new BearerTokenCallCredentials(config.token))
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
  }

  public ListenableFuture<Long> checkTail() {
    return withTimeout(
        () ->
            Futures.transform(
                withStaticRetries(
                    config.maxRetries,
                    () ->
                        this.futureStub.checkTail(
                            CheckTailRequest.newBuilder().setStream(streamName).build())),
                CheckTailResponse::getNextSeqNum,
                executor));
  }

  public ListenableFuture<ReadOutput> read(ReadRequest request) {
    return withTimeout(
        () ->
            Futures.transform(
                withStaticRetries(
                    config.maxRetries, () -> this.futureStub.read(request.toProto(streamName))),
                response -> ReadOutput.fromProto(response.getOutput()),
                executor));
  }

  public ReadSession readSession(
      ReadSessionRequest request, Consumer<ReadOutput> onResponse, Consumer<Throwable> onError) {
    return new ReadSession(this, request, onResponse, onError);
  }

  public ManagedReadSession managedReadSession(
      ReadSessionRequest request, Integer maxBufferedBytes) {
    return new ManagedReadSession(this, request, maxBufferedBytes);
  }

  public ListenableFuture<AppendOutput> append(AppendInput request) {
    return withTimeout(
        () ->
            Futures.transform(
                switch (config.appendRetryPolicy) {
                  case ALL ->
                      withStaticRetries(
                          config.maxRetries,
                          () ->
                              this.futureStub.append(
                                  AppendRequest.newBuilder()
                                      .setInput(request.toProto(streamName))
                                      .build()));
                  case NO_SIDE_EFFECTS ->
                      this.futureStub.append(
                          AppendRequest.newBuilder().setInput(request.toProto(streamName)).build());
                },
                response -> AppendOutput.fromProto(response.getOutput()),
                executor));
  }

  public AppendSessionRequestStream appendSession(
      Consumer<AppendOutput> onResponse, Consumer<Throwable> onError, Runnable onComplete) {
    var observer =
        this.asyncStub.appendSession(
            new StreamObserver<AppendSessionResponse>() {
              @Override
              public void onNext(AppendSessionResponse value) {
                onResponse.accept(AppendOutput.fromProto(value.getOutput()));
              }

              @Override
              public void onError(Throwable t) {
                onError.accept(t);
              }

              @Override
              public void onCompleted() {
                onComplete.run();
              }
            });
    return new AppendSessionRequestStream(
        appendInput ->
            observer.onNext(
                AppendSessionRequest.newBuilder()
                    .setInput(appendInput.toProto(this.streamName))
                    .build()),
        observer::onError,
        observer::onCompleted);
  }

  public ManagedAppendSession managedAppendSession() {
    return new ManagedAppendSession(this);
  }

  public record AppendSessionRequestStream(
      Consumer<AppendInput> onNext, Consumer<Throwable> onError, Runnable onComplete) {}
}
