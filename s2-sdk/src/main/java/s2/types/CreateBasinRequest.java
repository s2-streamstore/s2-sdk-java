package s2.types;

import java.util.Optional;
import s2.utils.BasinUtils;

public class CreateBasinRequest {

  public final Optional<String> basin;
  public final BasinConfig config;
  public final Optional<BasinAssignment> assignment;

  private CreateBasinRequest(
      Optional<String> basin, BasinConfig config, Optional<BasinAssignment> assignment) {
    this.basin = basin;
    this.config = config;
    this.assignment = assignment;
  }

  public static CreateBasinRequestBuilder newBuilder() {
    return new CreateBasinRequestBuilder();
  }

  public s2.v1alpha.CreateBasinRequest toProto() {
    var builder = s2.v1alpha.CreateBasinRequest.newBuilder().setConfig(config.toProto());
    basin.ifPresent(builder::setBasin);
    return builder.build();
  }

  abstract static class BasinAssignment {
    public final String value;

    BasinAssignment(String value) {
      this.value = value;
    }
  }

  static final class Scope extends BasinAssignment {
    Scope(String value) {
      super(value);
    }
  }

  static final class Cell extends BasinAssignment {
    Cell(String value) {
      super(value);
    }
  }

  public static class CreateBasinRequestBuilder {
    private Optional<String> basin = Optional.empty();
    private Optional<BasinConfig> config = Optional.empty();
    private Optional<BasinAssignment> assignment = Optional.empty();

    public CreateBasinRequestBuilder withBasin(String basin) {
      this.basin = Optional.of(basin);
      return this;
    }

    public CreateBasinRequestBuilder withDefaultStreamConfig(StreamConfig config) {
      this.config = Optional.of(new BasinConfig(config));
      return this;
    }

    public CreateBasinRequestBuilder withAssignmentScope(String scope) {
      this.assignment = Optional.of(new Scope(scope));
      return this;
    }

    public CreateBasinRequestBuilder withAssignmentCell(String cell) {
      this.assignment = Optional.of(new Cell(cell));
      return this;
    }

    public CreateBasinRequest build() {
      this.basin.ifPresent(BasinUtils::validateBasinName);
      return new CreateBasinRequest(
          this.basin,
          this.config.orElse(new BasinConfig(StreamConfig.newBuilder().build())),
          this.assignment);
    }
  }
}
