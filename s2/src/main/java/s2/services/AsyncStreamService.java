package s2.services;

import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import s2.v1alpha.S2.*;
import s2.v1alpha.StreamServiceGrpc;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Provides asynchronous operations for streams. Supports both future-based operations and streaming
 * operations with observers.
 */
public class AsyncStreamService extends BaseService {
  private StreamServiceGrpc.StreamServiceFutureStub futureStub;
  private StreamServiceGrpc.StreamServiceStub asyncStub;

  /**
   * Creates a new AsyncStreamService instance. Package-private constructor to ensure services are
   * only created through the {@link s2.services.Client} class.
   *
   * @param channel The gRPC channel to use for communication
   * @param credentials The credentials to use for authentication
   */
  AsyncStreamService(ManagedChannel channel, CallCredentials credentials) {
    super(channel, credentials);
    initializeStubs(channel, credentials);
  }

  @Override
  protected void onChannelUpdate() {
    initializeStubs(channel, credentials);
  }

  private void initializeStubs(ManagedChannel channel, CallCredentials credentials) {
    this.futureStub = StreamServiceGrpc.newFutureStub(channel);
    this.asyncStub = StreamServiceGrpc.newStub(channel);
    if (credentials != null) {
      this.futureStub = this.futureStub.withCallCredentials(credentials);
      this.asyncStub = this.asyncStub.withCallCredentials(credentials);
    }
  }

  /**
   * Opens an append session for streaming records to a stream.
   *
   * @param stream The stream to append to
   * @param onResponse Called when a response is received
   * @param onError Called when an error occurs
   * @return A StreamObserver for sending requests
   */
  public StreamObserver<AppendSessionRequest> openAppendSession(String stream,
      Consumer<AppendSessionResponse> onResponse, Consumer<Throwable> onError) {
    if (onResponse == null || onError == null) {
      throw new NullPointerException("Callbacks cannot be null");
    }
    return asyncStub.appendSession(new StreamObserver<AppendSessionResponse>() {
      @Override
      public void onNext(AppendSessionResponse response) {
        onResponse.accept(response);
      }

      @Override
      public void onError(Throwable t) {
        onError.accept(t);
      }

      @Override
      public void onCompleted() {
        // Session completed normally
      }
    });
  }

  /**
   * Opens a read session for streaming records from a stream.
   *
   * @param stream The stream to read from
   * @param startSeqNum The sequence number to start reading from
   * @param limit Optional limit on the number of records to read
   * @param onResponse Called when a response is received
   * @param onError Called when an error occurs
   */
  public void openReadSession(String stream, long startSeqNum, ReadLimit limit,
      Consumer<ReadSessionResponse> onResponse, Consumer<Throwable> onError) {
    if (onResponse == null || onError == null) {
      throw new NullPointerException("Callbacks cannot be null");
    }
    var requestBuilder =
        ReadSessionRequest.newBuilder().setStream(stream).setStartSeqNum(startSeqNum);
    if (limit != null) {
      requestBuilder.setLimit(limit);
    }

    asyncStub.readSession(requestBuilder.build(), new StreamObserver<ReadSessionResponse>() {
      @Override
      public void onNext(ReadSessionResponse response) {
        onResponse.accept(response);
      }

      @Override
      public void onError(Throwable t) {
        onError.accept(t);
      }

      @Override
      public void onCompleted() {
        // Session completed normally
      }
    });
  }

  /**
   * Asynchronously appends records to a stream.
   *
   * @param stream The stream to append to
   * @param records The records to append
   * @return A future that will complete with the append result
   */
  public CompletableFuture<AppendOutput> appendAsync(String stream, List<AppendRecord> records) {
    var future = new CompletableFuture<AppendOutput>();
    var request = AppendRequest.newBuilder()
        .setInput(AppendInput.newBuilder().setStream(stream).addAllRecords(records).build())
        .build();

    asyncStub.append(request, new StreamObserver<>() {
      @Override
      public void onNext(AppendResponse response) {
        future.complete(response.getOutput());
      }

      @Override
      public void onError(Throwable t) {
        future.completeExceptionally(t);
      }

      @Override
      public void onCompleted() {
        // Nothing to do here since we already completed the future
      }
    });

    return future;
  }

  /**
   * Asynchronously reads records from a stream.
   *
   * @param stream The stream to read from
   * @param startSeqNum The sequence number to start reading from
   * @param limit Optional limit on the number of records to read
   * @return A future that will complete with the read result
   */
  public CompletableFuture<ReadOutput> readAsync(String stream, long startSeqNum, ReadLimit limit) {
    var future = new CompletableFuture<ReadOutput>();
    var requestBuilder = ReadRequest.newBuilder().setStream(stream).setStartSeqNum(startSeqNum);
    if (limit != null) {
      requestBuilder.setLimit(limit);
    }

    asyncStub.read(requestBuilder.build(), new StreamObserver<ReadResponse>() {
      @Override
      public void onNext(ReadResponse response) {
        future.complete(response.getOutput());
      }

      @Override
      public void onError(Throwable t) {
        future.completeExceptionally(t);
      }

      @Override
      public void onCompleted() {
        // Nothing to do here since we already completed the future
      }
    });

    return future;
  }

  /**
   * Asynchronously checks the tail sequence number of a stream.
   *
   * @param stream The stream to check
   * @return A future that will complete with the next sequence number
   */
  public CompletableFuture<Long> checkTailAsync(String stream) {
    var future = new CompletableFuture<Long>();
    var request = CheckTailRequest.newBuilder().setStream(stream).build();

    asyncStub.checkTail(request, new StreamObserver<CheckTailResponse>() {
      @Override
      public void onNext(CheckTailResponse response) {
        future.complete(response.getNextSeqNum());
      }

      @Override
      public void onError(Throwable t) {
        future.completeExceptionally(t);
      }

      @Override
      public void onCompleted() {
        // Nothing to do here since we already completed the future
      }
    });

    return future;
  }
}
