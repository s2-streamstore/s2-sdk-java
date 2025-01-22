package s2.types;

import com.google.protobuf.FieldMask;
import java.util.List;
import java.util.Optional;
import s2.utils.StreamUtils;

public class ReconfigureStreamRequest {
  final String stream;
  final StreamConfig streamConfig;
  final List<String> fieldMask;

  private ReconfigureStreamRequest(
      String stream, StreamConfig streamConfig, List<String> fieldMask) {
    this.stream = stream;
    this.streamConfig = streamConfig;
    this.fieldMask = fieldMask;
  }

  public final s2.v1alpha.ReconfigureStreamRequest toProto() {
    var builder =
        s2.v1alpha.ReconfigureStreamRequest.newBuilder()
            .setStream(stream)
            .setConfig(streamConfig.toProto());
    var fieldMask = FieldMask.newBuilder();
    this.fieldMask.forEach(fieldMask::addPaths);
    builder.setMask(fieldMask.build());
    return builder.build();
  }

  public ReconfigureStreamRequestBuilder newBuilder() {
    return new ReconfigureStreamRequestBuilder();
  }

  public static class ReconfigureStreamRequestBuilder {
    private Optional<String> stream = Optional.empty();
    private Optional<StreamConfig> streamConfig = Optional.empty();
    private Optional<List<String>> fieldMask = Optional.empty();

    public ReconfigureStreamRequestBuilder withStream(String stream) {
      this.stream = Optional.of(stream);
      return this;
    }

    public ReconfigureStreamRequestBuilder withStreamConfig(StreamConfig streamConfig) {
      this.streamConfig = Optional.of(streamConfig);
      return this;
    }

    public ReconfigureStreamRequestBuilder withFieldMask(List<String> fieldMask) {
      this.fieldMask = Optional.of(fieldMask);
      return this;
    }

    public ReconfigureStreamRequest build() {
      this.stream.ifPresent(StreamUtils::validateStreamName);
      var stream =
          this.stream.orElseThrow(() -> new IllegalArgumentException("stream is required"));
      var streamConfig =
          this.streamConfig.orElseThrow(
              () -> new IllegalArgumentException("streamConfig is required"));
      return new ReconfigureStreamRequest(stream, streamConfig, fieldMask.orElse(List.of()));
    }
  }
}
