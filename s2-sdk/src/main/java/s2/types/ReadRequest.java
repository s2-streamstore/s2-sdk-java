package s2.types;

import java.util.Optional;

public class ReadRequest {

  public final long startSeqNum;
  public final ReadLimit readLimit;

  protected ReadRequest(long startSeqNum, ReadLimit readLimit) {
    this.startSeqNum = startSeqNum;
    this.readLimit = readLimit;
  }

  public static ReadRequestBuilder newBuilder() {
    return new ReadRequestBuilder();
  }

  public s2.v1alpha.ReadRequest toProto(String streamName) {
    return s2.v1alpha.ReadRequest.newBuilder()
        .setStream(streamName)
        .setStartSeqNum(startSeqNum)
        .setLimit(readLimit.toProto())
        .build();
  }

  public static class ReadRequestBuilder {
    private Optional<Long> startSeqNum = Optional.empty();
    private Optional<ReadLimit> readLimit = Optional.empty();

    public ReadRequestBuilder withStartSeqNum(long startSeqNum) {
      this.startSeqNum = Optional.of(startSeqNum);
      return this;
    }

    public ReadRequestBuilder withReadLimit(ReadLimit readLimit) {
      this.readLimit = Optional.of(readLimit);
      return this;
    }

    public ReadRequest build() {
      this.readLimit.ifPresent(ReadLimit::validateUnary);
      return new ReadRequest(this.startSeqNum.orElse(0L), this.readLimit.orElse(ReadLimit.NONE));
    }
  }
}
