package s2.types;

public record BasinConfig(StreamConfig defaultStreamConfig) {
  public s2.v1alpha.BasinConfig toProto() {
    return s2.v1alpha.BasinConfig.newBuilder()
        .setDefaultStreamConfig(defaultStreamConfig.toProto())
        .build();
  }
}
