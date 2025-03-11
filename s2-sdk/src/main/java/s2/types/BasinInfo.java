package s2.types;

public class BasinInfo {
  public final String name;
  public final BasinScope basinScope;
  public final BasinState basinState;

  BasinInfo(String name, BasinScope basinScope, BasinState basinState) {
    this.name = name;
    this.basinScope = basinScope;
    this.basinState = basinState;
  }

  public static BasinInfo fromProto(s2.v1alpha.BasinInfo basinInfo) {
    BasinScope scope;
    switch (basinInfo.getScope()) {
      case BASIN_SCOPE_UNSPECIFIED:
        scope = BasinScope.UNSPECIFIED;
        break;
      case BASIN_SCOPE_AWS_US_EAST_1:
        scope = BasinScope.AWS_US_EAST_1;
        break;
      default:
        scope = BasinScope.UNKNOWN;
        break;
    }

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

    return new BasinInfo(basinInfo.getName(), scope, state);
  }
}
