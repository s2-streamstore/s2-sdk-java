package s2.types;

import java.util.Optional;
import java.util.regex.Pattern;

public class CreateBasinRequest {
  private static final Pattern BASIN_NAME_REGEX = Pattern.compile("[0-9a-z][0-9a-z-]*");

  final Optional<String> basin;
  final BasinConfig config;
  final Optional<BasinAssignment> assignment;

  private static void validateBasinName(String basinName) {
    if (basinName.length() < 8) {
      throw new IllegalArgumentException("Basin name must be at least 8 characters");
    }
    if (basinName.length() > 48) {
      throw new IllegalArgumentException("Basin name must be at most 48 characters");
    }
    if (!BASIN_NAME_REGEX.matcher(basinName).matches()) {
      throw new IllegalArgumentException(
          "Basin name must comprise lowercase letters, numbers, and hyphens. It cannot begin or end with a hyphen");
    }
  }

  abstract static sealed class BasinAssignment permits Scope, Cell {
    final String value;

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

  public s2.v1alpha.CreateBasinRequest toProto() {
    var builder = s2.v1alpha.CreateBasinRequest.newBuilder().setConfig(config.toProto());
    basin.ifPresent(builder::setBasin);
    return builder.build();
  }

  private CreateBasinRequest(
      Optional<String> basin, BasinConfig config, Optional<BasinAssignment> assignment) {
    this.basin = basin;
    this.config = config;
    this.assignment = assignment;
  }

  public static CreateBasinRequestBuilder newBuilder() {
    return new CreateBasinRequestBuilder();
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
      this.basin.ifPresent(CreateBasinRequest::validateBasinName);
      return new CreateBasinRequest(
          this.basin,
          this.config.orElse(new BasinConfig(StreamConfig.newBuilder().build())),
          this.assignment);
    }
  }
}
