package s2.types;

public class BasinConfig {
  public final StreamConfig defaultStreamConfig;
  public final boolean createStreamOnAppend;
  public final boolean createStreamOnRead;

  public BasinConfig(
      StreamConfig defaultStreamConfig, boolean createStreamOnAppend, boolean createStreamOnRead) {
    this.defaultStreamConfig = defaultStreamConfig;
    this.createStreamOnAppend = createStreamOnAppend;
    this.createStreamOnRead = createStreamOnRead;
  }

  public static BasinConfig fromProto(s2.v1alpha.BasinConfig basinConfig) {
    return new BasinConfig(
        StreamConfig.fromProto(basinConfig.getDefaultStreamConfig()),
        basinConfig.getCreateStreamOnAppend(),
        basinConfig.getCreateStreamOnRead());
  }

  public s2.v1alpha.BasinConfig toProto() {
    return s2.v1alpha.BasinConfig.newBuilder()
        .setDefaultStreamConfig(defaultStreamConfig.toProto())
        .setCreateStreamOnAppend(createStreamOnAppend)
        .setCreateStreamOnRead(createStreamOnRead)
        .build();
  }
}
