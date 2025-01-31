package s2.types;

import java.util.Optional;

public class ListStreamsRequest {
  public final String prefix;
  public final String startAfter;
  public final Optional<Integer> limit;

  ListStreamsRequest(String prefix, String startAfter, Optional<Integer> limit) {
    this.prefix = prefix;
    this.startAfter = startAfter;
    this.limit = limit;
  }

  public static ListStreamsRequestBuilder newBuilder() {
    return new ListStreamsRequestBuilder();
  }

  public s2.v1alpha.ListStreamsRequest toProto() {
    var builder =
        s2.v1alpha.ListStreamsRequest.newBuilder().setPrefix(prefix).setStartAfter(startAfter);
    this.limit.ifPresent(builder::setLimit);
    return builder.build();
  }

  public static class ListStreamsRequestBuilder {
    private String prefix = "";
    private String startAfter = "";
    private Optional<Integer> limit = Optional.empty();

    public ListStreamsRequestBuilder withPrefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public ListStreamsRequestBuilder withStartAfter(String startAfter) {
      this.startAfter = startAfter;
      return this;
    }

    public ListStreamsRequestBuilder withLimit(Integer limit) {
      this.limit = Optional.of(limit);
      return this;
    }

    public ListStreamsRequest build() {
      this.limit.ifPresent(
          lim -> {
            if (lim < 0) {
              throw new IllegalArgumentException("Limit must be a positive integer");
            }
            if (lim > 1000) {
              throw new IllegalArgumentException("Limit must be less than 1000");
            }
          });
      return new ListStreamsRequest(prefix, startAfter, limit);
    }
  }
}
