package s2.types;

import java.util.Optional;

public class ReadSessionRequest {

  public final Start start;
  public final ReadLimit readLimit;
  public final boolean heartbeats;

  protected ReadSessionRequest(Start start, ReadLimit readLimit, boolean heartbeats) {
    this.start = start;
    this.readLimit = readLimit;
    this.heartbeats = heartbeats;
  }

  public static ReadSessionRequestBuilder newBuilder() {
    return new ReadSessionRequestBuilder();
  }

  public ReadSessionRequest update(Start start, long consumedRecords, long consumedBytes) {
    return new ReadSessionRequest(
        start, readLimit.remaining(consumedRecords, consumedBytes), heartbeats);
  }

  public s2.v1alpha.ReadSessionRequest toProto(String streamName) {
    s2.v1alpha.ReadSessionRequest.Builder builder =
        s2.v1alpha.ReadSessionRequest.newBuilder()
            .setStream(streamName)
            .setLimit(readLimit.toProto())
            .setHeartbeats(heartbeats);

    if (start instanceof Start.SeqNum) {
      builder.setSeqNum(((Start.SeqNum) start).value);
    } else if (start instanceof Start.Timestamp) {
      builder.setTimestamp(((Start.Timestamp) start).value);
    } else if (start instanceof Start.TailOffset) {
      builder.setTailOffset(((Start.TailOffset) start).value);
    } else {
      throw new IllegalStateException("Unknown Start type: " + start.getClass().getName());
    }

    return builder.build();
  }

  public static class ReadSessionRequestBuilder {
    private Optional<Start> start = Optional.empty();
    private Optional<ReadLimit> readLimit = Optional.empty();
    private boolean heartbeats = false;

    public ReadSessionRequestBuilder withStart(Start start) {
      this.start = Optional.of(start);
      return this;
    }

    public ReadSessionRequestBuilder withReadLimit(ReadLimit readLimit) {
      this.readLimit = Optional.of(readLimit);
      return this;
    }

    public ReadSessionRequestBuilder withHeartbeats(boolean heartbeats) {
      this.heartbeats = heartbeats;
      return this;
    }

    public ReadSessionRequest build() {
      return new ReadSessionRequest(
          this.start.orElseGet(() -> Start.seqNum(0L)),
          this.readLimit.orElse(ReadLimit.NONE),
          this.heartbeats);
    }
  }
}
