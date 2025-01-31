package s2.types;

public record BasinInfo(String name, String scope, String cell, BasinState state) {
  public static BasinInfo fromProto(s2.v1alpha.BasinInfo basinInfo) {
    var state =
        switch (basinInfo.getState()) {
          case BASIN_STATE_UNSPECIFIED -> BasinState.UNSPECIFIED;
          case BASIN_STATE_ACTIVE -> BasinState.ACTIVE;
          case BASIN_STATE_CREATING -> BasinState.CREATING;
          case BASIN_STATE_DELETING -> BasinState.DELETING;
          default -> BasinState.UNKNOWN;
        };
    return new BasinInfo(basinInfo.getName(), basinInfo.getScope(), basinInfo.getCell(), state);
  }
}
