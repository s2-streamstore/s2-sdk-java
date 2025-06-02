package s2.types;

import s2.v1alpha.StreamConfig;

public class Timestamping {
  public final TimestampingMode mode;
  public final boolean uncapped;

  /**
   * Instantiates a new Timestamping.
   *
   * @param mode selected timestamping behavior
   * @param uncapped if client-specified timestamps should be allowed to exceed the arrival time
   */
  public Timestamping(TimestampingMode mode, boolean uncapped) {
    this.mode = mode;
    this.uncapped = uncapped;
  }

  public static Timestamping fromProto(StreamConfig.Timestamping proto) {
    final TimestampingMode mode;
    switch (proto.getMode()) {
      case TIMESTAMPING_MODE_UNSPECIFIED:
        mode = TimestampingMode.UNSPECIFIED;
        break;
      case TIMESTAMPING_MODE_CLIENT_PREFER:
        mode = TimestampingMode.CLIENT_PREFER;
        break;
      case TIMESTAMPING_MODE_CLIENT_REQUIRE:
        mode = TimestampingMode.CLIENT_REQUIRE;
        break;
      case TIMESTAMPING_MODE_ARRIVAL:
        mode = TimestampingMode.ARRIVAL;
        break;
      case UNRECOGNIZED:
      default:
        mode = TimestampingMode.UNKNOWN;
        break;
    }

    return new Timestamping(mode, proto.getUncapped());
  }

  public StreamConfig.Timestamping toProto() {
    final s2.v1alpha.TimestampingMode timestampingMode;
    switch (mode) {
      case UNSPECIFIED:
        timestampingMode = s2.v1alpha.TimestampingMode.TIMESTAMPING_MODE_UNSPECIFIED;
        break;
      case CLIENT_PREFER:
        timestampingMode = s2.v1alpha.TimestampingMode.TIMESTAMPING_MODE_CLIENT_PREFER;
        break;
      case CLIENT_REQUIRE:
        timestampingMode = s2.v1alpha.TimestampingMode.TIMESTAMPING_MODE_CLIENT_REQUIRE;
        break;
      case ARRIVAL:
        timestampingMode = s2.v1alpha.TimestampingMode.TIMESTAMPING_MODE_ARRIVAL;
        break;
      default:
        throw new IllegalArgumentException("Unexpected value for timestamping mode: " + mode);
    }

    return StreamConfig.Timestamping.newBuilder()
        .setMode(timestampingMode)
        .setUncapped(uncapped)
        .build();
  }
}
