package s2.types;

import java.time.Instant;
import java.util.Optional;

public final class Batch implements ReadOutput, MeteredBytes {

  public final SequencedRecordBatch sequencedRecordBatch;

  Batch(SequencedRecordBatch sequencedRecordBatch) {
    this.sequencedRecordBatch = sequencedRecordBatch;
  }

  public Optional<Long> firstSeqNum() {
    return this.sequencedRecordBatch.records.stream().findFirst().map(sr -> sr.seqNum);
  }

  public Optional<Long> lastSeqNum() {
    if (!this.sequencedRecordBatch.records.isEmpty()) {
      return Optional.of(
          this.sequencedRecordBatch.records.get(sequencedRecordBatch.records.size() - 1).seqNum);
    } else {
      return Optional.empty();
    }
  }

  public Optional<Instant> firstTimestamp() {
    return this.sequencedRecordBatch.records.stream().findFirst().map(sr -> sr.timestamp);
  }

  public Optional<Instant> lastTimestamp() {
    if (!this.sequencedRecordBatch.records.isEmpty()) {
      return Optional.of(
          this.sequencedRecordBatch.records.get(sequencedRecordBatch.records.size() - 1).timestamp);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public long meteredBytes() {
    return this.sequencedRecordBatch.meteredBytes();
  }
}
