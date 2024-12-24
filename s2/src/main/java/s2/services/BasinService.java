package s2.services;

import io.grpc.ManagedChannel;
import io.grpc.CallCredentials;
import s2.v1alpha.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Service client for basin-level operations.
 */
public class BasinService extends BaseService {
  private BasinServiceGrpc.BasinServiceBlockingStub stub;

  /**
   * Creates a new BasinService instance. Package-private constructor to ensure services are only
   * created through the {@link s2.services.Client} class.
   *
   * @param channel The gRPC channel to use for communication
   * @param credentials The credentials to use for authentication
   */
  BasinService(ManagedChannel channel, CallCredentials credentials) {
    super(channel, credentials);
    this.stub = BasinServiceGrpc.newBlockingStub(channel);
    if (credentials != null) {
      this.stub = this.stub.withCallCredentials(credentials);
    }
  }

  @Override
  protected void onChannelUpdate() {
    this.stub = BasinServiceGrpc.newBlockingStub(channel);
    if (credentials != null) {
      this.stub = this.stub.withCallCredentials(credentials);
    }
  }

  /**
   * Lists all streams in a basin with the given prefix.
   */
  public List<StreamInfo> listStreams(String prefix) {
    var request = ListStreamsRequest.newBuilder().setPrefix(prefix).build();
    var streams = new ArrayList<StreamInfo>();
    ListStreamsResponse response;
    do {
      response = stub.listStreams(request);
      streams.addAll(response.getStreamsList());
    } while (response.getHasMore());
    return streams;
  }

  /**
   * Creates a new stream.
   */
  public StreamInfo createStream(String name, StreamConfig config) {
    var request = CreateStreamRequest.newBuilder().setStream(name).setConfig(config).build();
    return stub.createStream(request).getInfo();
  }

  /**
   * Deletes a stream.
   */
  public void deleteStream(String name) {
    var request = DeleteStreamRequest.newBuilder().setStream(name).build();
    stub.deleteStream(request);
  }
}
