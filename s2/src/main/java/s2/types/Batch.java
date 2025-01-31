package s2.types;

import java.util.Optional;

public record Batch(SequencedRecordBatch sequencedRecordBatch) implements ReadOutput, MeteredBytes {

  public Optional<Long> firstSeqNum() {
    return this.sequencedRecordBatch.records().stream().findFirst().map(SequencedRecord::seqNum);
  }

  public Optional<Long> lastSeqNum() {
    if (!this.sequencedRecordBatch.records().isEmpty()) {
      return Optional.of(
          this.sequencedRecordBatch
              .records()
              .get(sequencedRecordBatch.records().size() - 1)
              .seqNum());
    } else {
      return Optional.empty();
    }
  }

  @Override
  public long meteredBytes() {
    return this.sequencedRecordBatch.meteredBytes();
  }
}
