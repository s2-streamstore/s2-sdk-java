package s2.types;

import java.util.List;

public record SequencedRecordBatch(List<SequencedRecord> records) implements MeteredBytes {

  public static SequencedRecordBatch fromProto(s2.v1alpha.SequencedRecordBatch batch) {
    return new SequencedRecordBatch(
        batch.getRecordsList().stream().map(SequencedRecord::fromProto).toList());
  }

  @Override
  public long meteredBytes() {
    return this.records.stream().map(SequencedRecord::meteredBytes).reduce(0L, Long::sum);
  }
}
