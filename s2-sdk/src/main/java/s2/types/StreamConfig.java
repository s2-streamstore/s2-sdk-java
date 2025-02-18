package s2.types;

import java.time.Duration;
import java.util.Optional;
import s2.v1alpha.StreamConfig.RetentionPolicyCase;

public class StreamConfig {

  public final StorageClass storageClass;
  public final Optional<RetentionPolicy> retentionPolicy;

  StreamConfig(StorageClass storageClass, Optional<RetentionPolicy> retentionPolicy) {
    this.storageClass = storageClass;
    this.retentionPolicy = retentionPolicy;
  }

  public static StreamConfig fromProto(s2.v1alpha.StreamConfig proto) {
    StorageClass storageClass;
    switch (proto.getStorageClass()) {
      case STORAGE_CLASS_UNSPECIFIED:
        storageClass = StorageClass.UNSPECIFIED;
        break;
      case STORAGE_CLASS_STANDARD:
        storageClass = StorageClass.STANDARD;
        break;
      case STORAGE_CLASS_EXPRESS:
        storageClass = StorageClass.EXPRESS;
        break;
      default:
        storageClass = StorageClass.UNKNOWN;
        break;
    }

    Optional<RetentionPolicy> retentionPolicy = Optional.empty();

    if (proto.getRetentionPolicyCase() == RetentionPolicyCase.AGE) {
      retentionPolicy = Optional.of(new Age(Duration.ofSeconds(proto.getAge())));
    }
    return new StreamConfig(storageClass, retentionPolicy);
  }

  public static StreamConfigBuilder newBuilder() {
    return new StreamConfigBuilder();
  }

  public s2.v1alpha.StreamConfig toProto() {
    return s2.v1alpha.StreamConfig.newBuilder().build();
  }

  public static class StreamConfigBuilder {
    private Optional<StorageClass> storageClass = Optional.empty();
    private Optional<RetentionPolicy> retentionPolicy = Optional.empty();

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
