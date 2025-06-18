package s2.types;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.stream.Collectors;

public class SequencedRecord implements MeteredBytes {
  public final long seqNum;
  public final List<Header> headers;
  public final ByteString body;
  public final long timestamp;

  SequencedRecord(long seqNum, List<Header> headers, ByteString body, long timestamp) {
    this.seqNum = seqNum;
    this.headers = headers;
    this.body = body;
    this.timestamp = timestamp;
  }

  public static SequencedRecord fromProto(s2.v1alpha.SequencedRecord sequencedRecord) {
    return new SequencedRecord(
        sequencedRecord.getSeqNum(),
        sequencedRecord.getHeadersList().stream()
            .map(Header::fromProto)
            .collect(Collectors.toList()),
        sequencedRecord.getBody(),
        sequencedRecord.getTimestamp());
  }

  @Override
  public long meteredBytes() {
    return 8
        + (2L * this.headers.size())
        + this.headers.stream().map(h -> h.name.size() + h.value.size()).reduce(0, Integer::sum)
        + this.body.size();
  }
}
