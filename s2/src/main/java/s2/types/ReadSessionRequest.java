package s2.types;

import java.util.Optional;

public class ReadSessionRequest {

  public final long startSeqNum;
  public final ReadLimit readLimit;

  protected ReadSessionRequest(long startSeqNum, ReadLimit readLimit) {
    this.startSeqNum = startSeqNum;
    this.readLimit = readLimit;
  }

  public static ReadSessionRequestBuilder newBuilder() {
    return new ReadSessionRequestBuilder();
  }

  public ReadSessionRequest update(long newStartSeqNum, long consumedRecords, long consumedBytes) {
    return new ReadSessionRequest(
        newStartSeqNum, readLimit.remaining(consumedRecords, consumedBytes));
  }

  public s2.v1alpha.ReadSessionRequest toProto() {
    return s2.v1alpha.ReadSessionRequest.newBuilder()
        .setStartSeqNum(startSeqNum)
        .setLimit(readLimit.toProto())
        .build();
  }

  public s2.v1alpha.ReadSessionRequest toProto(String streamName) {
    return s2.v1alpha.ReadSessionRequest.newBuilder()
        .setStream(streamName)
        .setStartSeqNum(startSeqNum)
        .setLimit(readLimit.toProto())
        .build();
  }

  public static class ReadSessionRequestBuilder {
    Optional<Long> startSeqNum = Optional.empty();
    Optional<ReadLimit> readLimit = Optional.empty();

    public ReadSessionRequestBuilder withStartSeqNum(long startSeqNum) {
      this.startSeqNum = Optional.of(startSeqNum);
      return this;
    }

    public ReadSessionRequestBuilder withReadLimit(ReadLimit readLimit) {
      this.readLimit = Optional.of(readLimit);
      return this;
    }

    public ReadSessionRequest build() {
      return new ReadSessionRequest(
          this.startSeqNum.orElse(0L), this.readLimit.orElse(ReadLimit.NONE));
    }
  }
}
