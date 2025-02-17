package s2.types;

import java.time.Instant;
import java.util.Optional;

public class StreamInfo {
  public final String name;
  public final Instant createdAt;
  public final Optional<Instant> deletedAt;

  private StreamInfo(String name, Instant createdAt, Optional<Instant> deletedAt) {
    this.name = name;
    this.createdAt = createdAt;
    this.deletedAt = deletedAt;
  }

  public static StreamInfo fromProto(s2.v1alpha.StreamInfo streamInfo) {
    var createdAt = Instant.ofEpochSecond(streamInfo.getCreatedAt());
    Optional<Instant> deletedAt =
        streamInfo.hasDeletedAt()
            ? Optional.of(Instant.ofEpochSecond(streamInfo.getDeletedAt()))
            : Optional.empty();
    return new StreamInfo(streamInfo.getName(), createdAt, deletedAt);
  }
}
