package s2.types;

import java.time.Duration;
import java.util.Optional;
import s2.v1alpha.StreamConfig.RetentionPolicyCase;

public class StreamConfig {

  public final StorageClass storageClass;
  public final Optional<RetentionPolicy> retentionPolicy;
  public final Optional<Timestamping> timestamping;

  StreamConfig(
      StorageClass storageClass,
      Optional<RetentionPolicy> retentionPolicy,
      Optional<Timestamping> timestamping) {
    this.storageClass = storageClass;
    this.retentionPolicy = retentionPolicy;
    this.timestamping = timestamping;
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

    Optional<Timestamping> timestamping = Optional.empty();
    if (proto.hasTimestamping()) {
      timestamping = Optional.of(Timestamping.fromProto(proto.getTimestamping()));
    }

    return new StreamConfig(storageClass, retentionPolicy, timestamping);
  }

  public static StreamConfigBuilder newBuilder() {
    return new StreamConfigBuilder();
  }

  public s2.v1alpha.StreamConfig toProto() {
    var builder = s2.v1alpha.StreamConfig.newBuilder();
    final s2.v1alpha.StorageClass storageClass;
    switch (this.storageClass) {
      case UNKNOWN:
        throw new IllegalArgumentException("Unknown storage class: " + this.storageClass);
      case STANDARD:
        storageClass = s2.v1alpha.StorageClass.STORAGE_CLASS_STANDARD;
        break;
      case EXPRESS:
        storageClass = s2.v1alpha.StorageClass.STORAGE_CLASS_EXPRESS;
        break;
      case UNSPECIFIED:
      default:
        storageClass = s2.v1alpha.StorageClass.STORAGE_CLASS_UNSPECIFIED;
        break;
    }

    builder.setStorageClass(storageClass);

    if (retentionPolicy.isPresent()) {
      RetentionPolicy retentionPolicy = this.retentionPolicy.get();
      if (retentionPolicy instanceof Age) {
        builder.setAge(((Age) retentionPolicy).age.getSeconds());
      } else {
        throw new IllegalArgumentException("Invalid retention policy: " + retentionPolicy);
      }
    }

    this.timestamping.ifPresent(ts -> builder.setTimestamping(ts.toProto()));

    return builder.build();
  }

  public static class StreamConfigBuilder {
    private Optional<StorageClass> storageClass = Optional.empty();
    private Optional<RetentionPolicy> retentionPolicy = Optional.empty();
    private Optional<Timestamping> timestamping = Optional.empty();

    public StreamConfigBuilder withStorageClass(StorageClass storageClass) {
      this.storageClass = Optional.of(storageClass);
      return this;
    }

    public StreamConfigBuilder withRetentionPolicy(RetentionPolicy retentionPolicy) {
      this.retentionPolicy = Optional.of(retentionPolicy);
      return this;
    }

    public StreamConfigBuilder withTimestamping(Timestamping timestamping) {
      this.timestamping = Optional.of(timestamping);
      return this;
    }

    public StreamConfig build() {
      return new StreamConfig(
          this.storageClass.orElse(StorageClass.EXPRESS), this.retentionPolicy, this.timestamping);
    }
  }
}
