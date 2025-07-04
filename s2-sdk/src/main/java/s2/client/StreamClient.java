package s2.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.auth.BearerTokenCallCredentials;
import s2.channel.BasinCompatibleChannel;
import s2.channel.ManagedChannelFactory;
import s2.config.AppendRetryPolicy;
import s2.config.Config;
import s2.types.AppendInput;
import s2.types.AppendOutput;
import s2.types.ReadOutput;
import s2.types.ReadRequest;
import s2.types.ReadSessionRequest;
import s2.types.StreamPosition;
import s2.v1alpha.AppendRequest;
import s2.v1alpha.AppendResponse;
import s2.v1alpha.AppendSessionRequest;
import s2.v1alpha.AppendSessionResponse;
import s2.v1alpha.CheckTailRequest;
import s2.v1alpha.StreamServiceGrpc;
import s2.v1alpha.StreamServiceGrpc.StreamServiceFutureStub;
import s2.v1alpha.StreamServiceGrpc.StreamServiceStub;

/** Client for stream-level operations. */
public class StreamClient extends BasinClient {

  private static final Logger logger = LoggerFactory.getLogger(StreamClient.class.getName());
  private static final String compressionCodec = "gzip";

  /** Name of stream associated with this client. */
  final String streamName;

  final StreamServiceStub asyncStub;
  private final StreamServiceFutureStub futureStub;

  private StreamClient(
      Config config,
      String basin,
      String streamName,
      BasinCompatibleChannel channel,
      ScheduledExecutorService executor,
      boolean ownedChannel,
      boolean ownedExecutor) {
    super(config, basin, channel, executor, ownedChannel, ownedExecutor);
    var meta = new Metadata();
    meta.put(Key.of("s2-basin", Metadata.ASCII_STRING_MARSHALLER), basin);
    this.streamName = streamName;

    StreamServiceFutureStub futureStub =
        StreamServiceGrpc.newFutureStub(channel.getChannel().managedChannel)
            .withCallCredentials(new BearerTokenCallCredentials(config.token))
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
    StreamServiceStub asyncStub =
        StreamServiceGrpc.newStub(channel.getChannel().managedChannel)
            .withCallCredentials(new BearerTokenCallCredentials(config.token))
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));

    if (config.compression) {
      futureStub = futureStub.withCompression(compressionCodec);
      asyncStub = asyncStub.withCompression(compressionCodec);
    }

    this.futureStub = futureStub;
    this.asyncStub = asyncStub;
  }

  public static StreamClientBuilder newBuilder(Config config, String basinName, String streamName) {
    return new StreamClientBuilder(config, basinName, streamName);
  }

  /**
   * Check the sequence number that will be assigned to the next record on a stream.
   *
   * @return future of the tail's position
   */
  public ListenableFuture<StreamPosition> checkTail() {
    return withTimeout(
        () ->
            Futures.transform(
                withStaticRetries(
                    config.maxRetries,
                    () ->
                        this.futureStub.checkTail(
                            CheckTailRequest.newBuilder().setStream(streamName).build())),
                (resp) -> new StreamPosition(resp.getNextSeqNum(), resp.getLastTimestamp()),
                executor));
  }

  /**
   * Retrieve a batch of records from a stream, using the unary read RPC.
   *
   * @see StreamClient#readSession
   * @param request the request
   * @return future of the read result
   */
  public ListenableFuture<ReadOutput> read(ReadRequest request) {
    return withTimeout(
        () ->
            Futures.transform(
                withStaticRetries(
                    config.maxRetries, () -> this.futureStub.read(request.toProto(streamName))),
                response -> ReadOutput.fromProto(response.getOutput()),
                executor));
  }

  /**
   * Retrieve batches of records from a stream continuously.
   *
   * <p>This entryway into a read session does internally perform retries (if configured via {@link
   * Config#maxRetries}). It does not handle any form of backpressure or flow control directly.
   *
   * <p>The stream is interacted with via callbacks, which delegate to an underlying GRPC <a
   * href="https://grpc.github.io/grpc-java/javadoc/io/grpc/stub/StreamObserver.html">StreamObserver</a>
   * class.
   *
   * @see StreamClient#managedReadSession
   * @param request the request
   * @param onResponse function to run, sequentially, on each successful message
   * @param onError function to run on an error
   * @return a ReadSession instance
   */
  public ReadSession readSession(
      ReadSessionRequest request, Consumer<ReadOutput> onResponse, Consumer<Throwable> onError) {
    return new ReadSession(this, request, onResponse, onError);
  }

  /**
   * Retrieve batches of records from a stream continuously, using a buffered queue-backed iterator.
   *
   * <p>This entryway into a read session, similar to {@link StreamClient#readSession}, will retry
   * internally if configured.
   *
   * <p>The GRPC streaming response will be buffered, based on the `maxBufferedBytes` param,
   * preventing situations where the result of a read accumulate faster than a user can handle.
   *
   * <p>Results are interacted with via an interator-like API, rather than via callbacks.
   *
   * @see StreamClient#readSession
   * @param request the request
   * @param maxBufferedBytes the max allowed amount of read response metered bytes to keep in the
   *     buffer
   * @return a ManagedReadSession instance
   */
  public ManagedReadSession managedReadSession(
      ReadSessionRequest request, Integer maxBufferedBytes) {
    return new ManagedReadSession(this, request, maxBufferedBytes);
  }

  /**
   * Append a batch of records to a stream, using the unary append RPC.
   *
   * <p>Note that the choice of {@link Config#appendRetryPolicy} is important. Since appends are not
   * idempotent by default, retries <i>could</i> cause duplicates in a stream. If your use-case
   * cannot tolerate the potential of duplicate records, make sure to select {@link
   * AppendRetryPolicy#NO_SIDE_EFFECTS}.
   *
   * @see Config#appendRetryPolicy
   * @see AppendRetryPolicy
   * @param request the request
   * @return future of the append response
   */
  public ListenableFuture<AppendOutput> append(AppendInput request) {
    ListenableFuture<AppendResponse> future;
    switch (config.appendRetryPolicy) {
      case ALL:
        future =
            withStaticRetries(
                config.maxRetries,
                () ->
                    this.futureStub.append(
                        AppendRequest.newBuilder().setInput(request.toProto(streamName)).build()));
        break;
      case NO_SIDE_EFFECTS:
        future =
            this.futureStub.append(
                AppendRequest.newBuilder().setInput(request.toProto(streamName)).build());
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported append retry policy: " + config.appendRetryPolicy);
    }
    return withTimeout(
        () ->
            Futures.transform(
                future, response -> AppendOutput.fromProto(response.getOutput()), executor));
  }

  /**
   * Start an unmanaged streaming append session.
   *
   * <p>Append batches of records to a stream continuously, while guaranteeing pipelined requests
   * are processed in order.
   *
   * <p>Most users will prefer to use {@link StreamClient#managedAppendSession()} instead.
   *
   * <p>Retries are not attempted, and no flow control is performed.
   *
   * @param onResponse function to run, sequentially, on each successful message
   * @param onError function to run on an error, which will be a terminal message
   * @param onComplete function to run on successful server-side completion of the session
   * @return the append session request stream
   */
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

  /**
   * Start a managed append session.
   *
   * <p>Append batches of records to a stream continuously, while guaranteeing pipelined requests
   * are processed in order.
   *
   * <p>Unlike with {@link StreamClient#appendSession}, this session will attempt to retry
   * intermittent failures if so elected.
   *
   * <p>Note that the choice of {@link Config#appendRetryPolicy} is important. Since appends are not
   * idempotent by default, retries <i>could</i> cause duplicates in a stream. If you use-case
   * cannot tolerate the potential of duplicate records, make sure to select {@link
   * AppendRetryPolicy#NO_SIDE_EFFECTS}.
   *
   * @see Config#appendRetryPolicy
   * @see AppendRetryPolicy
   * @return the managed append session
   */
  public ManagedAppendSession managedAppendSession() {
    return new ManagedAppendSession(this);
  }

  public static class StreamClientBuilder {

    private final Config config;
    private final String basinName;
    private final String streamName;
    private Optional<BasinCompatibleChannel> channel = Optional.empty();
    private Optional<ScheduledExecutorService> executor = Optional.empty();

    public StreamClientBuilder(Config config, String basinName, String streamName) {
      this.config = config;
      this.basinName = basinName;
      this.streamName = streamName;
    }

    public StreamClientBuilder withChannel(BasinCompatibleChannel channel) {
      this.channel = Optional.of(channel);
      return this;
    }

    public StreamClientBuilder withExecutor(ScheduledExecutorService executor) {
      this.executor = Optional.of(executor);
      return this;
    }

    public StreamClient build() {
      return new StreamClient(
          this.config,
          this.basinName,
          this.streamName,
          this.channel.orElseGet(
              () -> ManagedChannelFactory.forBasinOrStreamService(this.config, this.basinName)),
          this.executor.orElseGet(() -> BaseClient.defaultExecutor("streamClient")),
          this.channel.isEmpty(),
          this.executor.isEmpty());
    }
  }

  public static class AppendSessionRequestStream {
    final Consumer<AppendInput> onNext;
    final Consumer<Throwable> onError;
    final Runnable onComplete;

    AppendSessionRequestStream(
        Consumer<AppendInput> onNext, Consumer<Throwable> onError, Runnable onComplete) {
      this.onNext = onNext;
      this.onError = onError;
      this.onComplete = onComplete;
    }
  }
}
