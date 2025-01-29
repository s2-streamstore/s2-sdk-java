package s2.types;

import java.util.Optional;
import s2.utils.StreamUtils;

public class CreateStreamRequest {

  public final String streamName;
  public final StreamConfig streamConfig;

  CreateStreamRequest(String streamName, StreamConfig streamConfig) {
    this.streamName = streamName;
    this.streamConfig = streamConfig;
  }

  public static CreateStreamRequestBuilder newBuilder() {
    return new CreateStreamRequestBuilder();
  }

  public s2.v1alpha.CreateStreamRequest toProto() {
    var builder = s2.v1alpha.CreateStreamRequest.newBuilder();
    builder.setStream(streamName);
    builder.setConfig(streamConfig.toProto());
    return builder.build();
  }

  public static class CreateStreamRequestBuilder {
    private Optional<String> streamName;
    private Optional<StreamConfig> streamConfig;

    public CreateStreamRequestBuilder withStreamName(String streamName) {
      this.streamName = Optional.ofNullable(streamName);
      return this;
    }

    public CreateStreamRequestBuilder withStreamConfig(StreamConfig streamConfig) {
      this.streamConfig = Optional.ofNullable(streamConfig);
      return this;
    }

    public CreateStreamRequest build() {
      this.streamName.ifPresent(StreamUtils::validateStreamName);
      return new CreateStreamRequest(
          streamName.orElseThrow(() -> new IllegalArgumentException("stream is required")),
          streamConfig.orElseThrow(() -> new IllegalArgumentException("streamConfig is required")));
    }
  }
}
