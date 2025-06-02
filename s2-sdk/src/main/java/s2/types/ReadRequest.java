package s2.types;

import java.util.Optional;

public class ReadRequest {

  public final Start start;
  public final ReadLimit readLimit;

  protected ReadRequest(Start start, ReadLimit readLimit) {
    this.start = start;
    this.readLimit = readLimit;
  }

  public static ReadRequestBuilder newBuilder() {
    return new ReadRequestBuilder();
  }

  public s2.v1alpha.ReadRequest toProto(String streamName) {
    s2.v1alpha.ReadRequest.Builder builder =
        s2.v1alpha.ReadRequest.newBuilder().setStream(streamName).setLimit(readLimit.toProto());

    if (start instanceof Start.SeqNum) {
      builder.setSeqNum(((Start.SeqNum) start).value);
    } else if (start instanceof Start.Timestamp) {
      builder.setTimestamp(((Start.Timestamp) start).value);
    } else if (start instanceof Start.TailOffset) {
      builder.setTailOffset(((Start.TailOffset) start).value);
    } else {
      throw new IllegalStateException("Unknown start type: " + start.getClass().getName());
    }

    return builder.build();
  }

  public static class ReadRequestBuilder {
    private Optional<Start> start = Optional.empty();
    private Optional<ReadLimit> readLimit = Optional.empty();

    public ReadRequestBuilder withStart(Start start) {
      this.start = Optional.of(start);
      return this;
    }

    public ReadRequestBuilder withReadLimit(ReadLimit readLimit) {
      this.readLimit = Optional.of(readLimit);
      return this;
    }

    public ReadRequest build() {
      this.readLimit.ifPresent(ReadLimit::validateUnary);
      return new ReadRequest(
          this.start.orElseGet(() -> Start.seqNum(0L)), this.readLimit.orElse(ReadLimit.NONE));
    }
  }
}
