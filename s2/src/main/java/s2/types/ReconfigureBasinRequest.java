package s2.types;

import com.google.protobuf.FieldMask;
import java.util.List;
import java.util.Optional;
import s2.utils.BasinUtils;

public class ReconfigureBasinRequest {
  public final String basin;
  public final BasinConfig basinConfig;
  public final List<String> fieldMask;

  private ReconfigureBasinRequest(String basin, BasinConfig basinConfig, List<String> fieldMask) {
    this.basin = basin;
    this.basinConfig = basinConfig;
    this.fieldMask = fieldMask;
  }

  public final s2.v1alpha.ReconfigureBasinRequest toProto() {
    var builder =
        s2.v1alpha.ReconfigureBasinRequest.newBuilder()
            .setBasin(basin)
            .setConfig(basinConfig.toProto());
    var fieldMask = FieldMask.newBuilder();
    this.fieldMask.forEach(fieldMask::addPaths);
    builder.setMask(fieldMask.build());
    return builder.build();
  }

  public ReconfigureBasinRequestBuilder newBuilder() {
    return new ReconfigureBasinRequestBuilder();
  }

  public static class ReconfigureBasinRequestBuilder {
    private Optional<String> basin = Optional.empty();
    private Optional<BasinConfig> basinConfig = Optional.empty();
    private Optional<List<String>> fieldMask = Optional.empty();

    public ReconfigureBasinRequestBuilder withBasin(String basin) {
      this.basin = Optional.of(basin);
      return this;
    }

    public ReconfigureBasinRequestBuilder withBasinConfig(BasinConfig basinConfig) {
      this.basinConfig = Optional.of(basinConfig);
      return this;
    }

    public ReconfigureBasinRequestBuilder withFieldMask(List<String> fieldMask) {
      this.fieldMask = Optional.of(fieldMask);
      return this;
    }

    public ReconfigureBasinRequest build() {
      this.basin.ifPresent(BasinUtils::validateBasinName);
      var basin = this.basin.orElseThrow(() -> new IllegalArgumentException("basin is required"));
      var basinConfig =
          this.basinConfig.orElseThrow(
              () -> new IllegalArgumentException("basinConfig is required"));
      return new ReconfigureBasinRequest(basin, basinConfig, fieldMask.orElse(List.of()));
    }
  }
}
