package s2.types;

public class BasinConfig {
  public final StreamConfig defaultStreamConfig;

  public BasinConfig(StreamConfig defaultStreamConfig) {
    this.defaultStreamConfig = defaultStreamConfig;
  }

  public s2.v1alpha.BasinConfig toProto() {
    return s2.v1alpha.BasinConfig.newBuilder()
        .setDefaultStreamConfig(defaultStreamConfig.toProto())
        .build();
  }
}
