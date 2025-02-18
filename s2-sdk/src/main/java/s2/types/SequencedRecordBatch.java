package s2.types;

import java.util.List;
import java.util.stream.Collectors;

public class SequencedRecordBatch implements MeteredBytes {
  public final List<SequencedRecord> records;

  SequencedRecordBatch(List<SequencedRecord> records) {
    this.records = records;
  }

  public static SequencedRecordBatch fromProto(s2.v1alpha.SequencedRecordBatch batch) {
    return new SequencedRecordBatch(
        batch.getRecordsList().stream()
            .map(SequencedRecord::fromProto)
            .collect(Collectors.toList()));
  }

  @Override
  public long meteredBytes() {
    return this.records.stream().map(SequencedRecord::meteredBytes).reduce(0L, Long::sum);
  }
}
