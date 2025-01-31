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

/** Client for stream-level operations. */
public class StreamClient extends BasinClient {

  private static final Logger logger = LoggerFactory.getLogger(StreamClient.class.getName());

  /** Name of stream associated with this client. */
  final String streamName;

  private final StreamServiceFutureStub futureStub;
  final StreamServiceStub asyncStub;

  /**
   * Instantiates a new Stream client.
   *
   * <p>Most users will prefer to use the {@link s2.client.BasinClient#streamClient(String)} method
   * for construction.
   *
   * @see s2.client.BasinClient#streamClient
   * @param streamName the stream name
   * @param basin the basin
   * @param config the config
   * @param channel the channel
   * @param executor the executor
   */
  public StreamClient(
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

  /**
   * Check the sequence number that will be assigned to the next record on a stream.
   *
   * @return future of the next sequence number
   */
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

  /**
   * Retrieve a batch of records from a stream, using the unary read RPC.
   *
   * @see s2.client.StreamClient#readSession
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
   * s2.config.Config#maxRetries}). It does not handle any form of backpressure or flow control
   * directly.
   *
   * <p>The stream is interacted with via callbacks, which delegate to an underlying GRPC <a
   * href="https://grpc.github.io/grpc-java/javadoc/io/grpc/stub/StreamObserver.html">StreamObserver</a>
   * class.
   *
   * @see s2.client.StreamClient#managedReadSession
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
   * <p>This entryway into a read session, similar to {@link s2.client.StreamClient#readSession},
   * will retry internally if configured.
   *
   * <p>The GRPC streaming response will be buffered, based on the `maxBufferedBytes` param,
   * preventing situations where the result of a read accumulate faster than a user can handle.
   *
   * <p>Results are interacted with via an interator-like API, rather than via callbacks.
   *
   * @see s2.client.StreamClient#readSession
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
   * <p>Note that the choice of {@link s2.config.Config#appendRetryPolicy} is important. Since
   * appends are not idempotent by default, retries <i>could</i> cause duplicates in a stream. If
   * you use-case cannot tolerate the potential of duplicate records, make sure to select {@link
   * s2.config.AppendRetryPolicy#NO_SIDE_EFFECTS}.
   *
   * @see s2.config.Config#appendRetryPolicy
   * @see s2.config.AppendRetryPolicy
   * @param request the request
   * @return future of the append response
   */
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
   * <p>Note that the choice of {@link s2.config.Config#appendRetryPolicy} is important. Since
   * appends are not idempotent by default, retries <i>could</i> cause duplicates in a stream. If
   * you use-case cannot tolerate the potential of duplicate records, make sure to select {@link
   * s2.config.AppendRetryPolicy#NO_SIDE_EFFECTS}.
   *
   * @see s2.config.Config#appendRetryPolicy
   * @see s2.config.AppendRetryPolicy
   * @return the managed append session
   */
  public ManagedAppendSession managedAppendSession() {
    return new ManagedAppendSession(this);
  }

  public record AppendSessionRequestStream(
      Consumer<AppendInput> onNext, Consumer<Throwable> onError, Runnable onComplete) {}
}
