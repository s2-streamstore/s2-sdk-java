package s2.types;

import java.util.Optional;

public class ReadSessionRequest {

  public final long startSeqNum;
  public final ReadLimit readLimit;
  public final boolean heartbeats;

  protected ReadSessionRequest(long startSeqNum, ReadLimit readLimit, boolean heartbeats) {
    this.startSeqNum = startSeqNum;
    this.readLimit = readLimit;
    this.heartbeats = heartbeats;
  }

  public static ReadSessionRequestBuilder newBuilder() {
    return new ReadSessionRequestBuilder();
  }

  public ReadSessionRequest update(long newStartSeqNum, long consumedRecords, long consumedBytes) {
    return new ReadSessionRequest(
        newStartSeqNum, readLimit.remaining(consumedRecords, consumedBytes), heartbeats);
  }

  public s2.v1alpha.ReadSessionRequest toProto(String streamName) {
    return s2.v1alpha.ReadSessionRequest.newBuilder()
        .setStream(streamName)
        .setStartSeqNum(startSeqNum)
        .setLimit(readLimit.toProto())
        .setHeartbeats(heartbeats)
        .build();
  }

  public static class ReadSessionRequestBuilder {
    private Optional<Long> startSeqNum = Optional.empty();
    private Optional<ReadLimit> readLimit = Optional.empty();
    private boolean heartbeats = false;

    public ReadSessionRequestBuilder withStartSeqNum(long startSeqNum) {
      this.startSeqNum = Optional.of(startSeqNum);
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
          this.startSeqNum.orElse(0L), this.readLimit.orElse(ReadLimit.NONE), this.heartbeats);
    }
  }
}
