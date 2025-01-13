package s2.services;

import io.grpc.ManagedChannel;
import io.grpc.CallCredentials;
import s2.v1alpha.*;

import java.util.List;

/**
 * Service client for synchronous stream operations.
 */
public class StreamService extends BaseService {
  private StreamServiceGrpc.StreamServiceBlockingStub stub;

  /**
   * Creates a new StreamService instance. Package-private constructor to ensure services are only
   * created through the {@link s2.services.Client} class.
   *
   * @param channel The gRPC channel to use for communication
   * @param credentials The credentials to use for authentication
   */
  StreamService(ManagedChannel channel, CallCredentials credentials) {
    super(channel, credentials);
    this.stub = StreamServiceGrpc.newBlockingStub(channel);
    if (credentials != null) {
      this.stub = this.stub.withCallCredentials(credentials);
    }
  }

  @Override
  protected void onChannelUpdate() {
    this.stub = StreamServiceGrpc.newBlockingStub(channel);
    if (credentials != null) {
      this.stub = this.stub.withCallCredentials(credentials);
    }
  }

  /**
   * Appends records to a stream.
   */
  public AppendOutput append(String stream, List<AppendRecord> records) {
    var request = AppendRequest.newBuilder()
        .setInput(AppendInput.newBuilder().setStream(stream).addAllRecords(records).build())
        .build();
    return stub.append(request).getOutput();
  }

  /**
   * Reads records from a stream.
   */
  public ReadOutput read(String stream, long startSeqNum, ReadLimit limit) {
    var requestBuilder = ReadRequest.newBuilder().setStream(stream).setStartSeqNum(startSeqNum);
    if (limit != null) {
      requestBuilder.setLimit(limit);
    }
    return stub.read(requestBuilder.build()).getOutput();
  }

  /**
   * Checks the tail sequence number of a stream.
   */
  public long checkTail(String stream) {
    var request = CheckTailRequest.newBuilder().setStream(stream).build();
    return stub.checkTail(request).getNextSeqNum();
  }
}
