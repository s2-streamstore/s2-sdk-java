package s2.types;

import java.time.Duration;
import java.util.Optional;

public class StreamConfig {

  public final StorageClass storageClass;
  public final Optional<RetentionPolicy> retentionPolicy;

  public s2.v1alpha.StreamConfig toProto() {
    return s2.v1alpha.StreamConfig.newBuilder().build();
  }

  public static StreamConfig fromProto(s2.v1alpha.StreamConfig proto) {
    var storageClass =
        switch (proto.getStorageClass()) {
          case STORAGE_CLASS_UNSPECIFIED -> StorageClass.UNSPECIFIED;
          case STORAGE_CLASS_STANDARD -> StorageClass.STANDARD;
          case STORAGE_CLASS_EXPRESS -> StorageClass.EXPRESS;
          default -> StorageClass.UNKNOWN;
        };
    Optional<RetentionPolicy> retentionPolicy =
        switch (proto.getRetentionPolicyCase()) {
          case AGE -> Optional.of(new Age(Duration.ofSeconds(proto.getAge())));
          case RETENTIONPOLICY_NOT_SET -> Optional.empty();
        };
    return new StreamConfig(storageClass, retentionPolicy);
  }

  StreamConfig(StorageClass storageClass, Optional<RetentionPolicy> retentionPolicy) {
    this.storageClass = storageClass;
    this.retentionPolicy = retentionPolicy;
  }

  public static StreamConfigBuilder newBuilder() {
    return new StreamConfigBuilder();
  }

  public static class StreamConfigBuilder {
    Optional<StorageClass> storageClass = Optional.empty();
    Optional<RetentionPolicy> retentionPolicy = Optional.empty();

    public StreamConfigBuilder withStorageClass(StorageClass storageClass) {
      this.storageClass = Optional.of(storageClass);
      return this;
    }

    public StreamConfigBuilder withRetentionPolicy(RetentionPolicy retentionPolicy) {
      this.retentionPolicy = Optional.of(retentionPolicy);
      return this;
    }

    public StreamConfig build() {
      return new StreamConfig(this.storageClass.orElse(StorageClass.EXPRESS), this.retentionPolicy);
    }
  }
}
