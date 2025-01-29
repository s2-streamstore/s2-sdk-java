package s2.types;

import com.google.protobuf.ByteString;
import java.util.List;

public record SequencedRecord(Long seqNum, List<Header> headers, ByteString body)
    implements MeteredBytes {
  public static SequencedRecord fromProto(s2.v1alpha.SequencedRecord sequencedRecord) {
    return new SequencedRecord(
        sequencedRecord.getSeqNum(),
        sequencedRecord.getHeadersList().stream().map(Header::fromProto).toList(),
        sequencedRecord.getBody());
  }

  @Override
  public long meteredBytes() {
    return 8
        + (2L * this.headers.size())
        + this.headers.stream().map(h -> h.name().size() + h.value().size()).reduce(0, Integer::sum)
        + this.body.size();
  }
}
