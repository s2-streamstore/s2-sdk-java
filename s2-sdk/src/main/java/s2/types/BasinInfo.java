package s2.types;

public class BasinInfo {
  public final String name;
  public final String scope;
  public final String cell;
  public final BasinState basinState;

  BasinInfo(String name, String scope, String cell, BasinState basinState) {
    this.name = name;
    this.scope = scope;
    this.cell = cell;
    this.basinState = basinState;
  }

  public static BasinInfo fromProto(s2.v1alpha.BasinInfo basinInfo) {
    BasinState state;
    switch (basinInfo.getState()) {
      case BASIN_STATE_UNSPECIFIED:
        state = BasinState.UNSPECIFIED;
        break;
      case BASIN_STATE_ACTIVE:
        state = BasinState.ACTIVE;
        break;
      case BASIN_STATE_CREATING:
        state = BasinState.CREATING;
        break;
      case BASIN_STATE_DELETING:
        state = BasinState.DELETING;
        break;
      default:
        state = BasinState.UNKNOWN;
        break;
    }

    return new BasinInfo(basinInfo.getName(), basinInfo.getScope(), basinInfo.getCell(), state);
  }
}
