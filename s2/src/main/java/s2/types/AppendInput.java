package s2.types;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;

public class AppendInput implements MeteredBytes {
  public final List<AppendRecord> records;
  public final Optional<Long> matchSeqNum;
  public final Optional<ByteString> fencingToken;

  private AppendInput(
      List<AppendRecord> records, Optional<Long> matchSeqNum, Optional<ByteString> fencingToken) {
    this.records = records;
    this.matchSeqNum = matchSeqNum;
    this.fencingToken = fencingToken;
  }

  public static AppendInputBuilder newBuilder() {
    return new AppendInputBuilder();
  }

  @Override
  public long meteredBytes() {
    return this.records.stream().map(AppendRecord::meteredBytes).reduce(0L, Long::sum);
  }

  public s2.v1alpha.AppendInput toProto(String streamName) {
    var builder = s2.v1alpha.AppendInput.newBuilder();
    builder.setStream(streamName);
    builder.addAllRecords(() -> this.records.stream().map(AppendRecord::toProto).iterator());
    this.matchSeqNum.ifPresent(builder::setMatchSeqNum);
    this.fencingToken.ifPresent(builder::setFencingToken);
    return builder.build();
  }

  public static class AppendInputBuilder {
    private List<AppendRecord> records = List.of();
    private Optional<Long> matchSeqNum = Optional.empty();
    private Optional<ByteString> fencingToken = Optional.empty();

    public AppendInputBuilder withRecords(List<AppendRecord> records) {
      this.records = records;
      return this;
    }

    public AppendInputBuilder withMatchSeqNum(Long matchSeqNum) {
      this.matchSeqNum = Optional.of(matchSeqNum);
      return this;
    }

    public AppendInputBuilder withFencingToken(ByteString fencingToken) {
      this.fencingToken = Optional.of(fencingToken);
      return this;
    }

    public AppendInput build() {
      matchSeqNum.ifPresent(
          num -> {
            if (num < 0) {
              throw new IllegalArgumentException("matchSeqNum must be positive");
            }
          });
      fencingToken.ifPresent(
          token -> {
            if (token.size() > 16) {
              throw new IllegalArgumentException("fencingToken must be less than 16 bytes");
            }
          });
      var provisional = new AppendInput(records, matchSeqNum, fencingToken);
      var meteredBytes = provisional.meteredBytes();
      if (meteredBytes > 1024 * 1024) {
        throw new IllegalStateException(
            String.format(
                "AppendInput would exceed the maximum allowed metered size of 1MiB, currently %s bytes.",
                meteredBytes));
      }
      return provisional;
    }
  }
}
