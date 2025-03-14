package s2.types;

import java.util.Optional;

public class ListBasinsRequest {
  public final String prefix;
  public final String startAfter;
  public final Optional<Integer> limit;

  ListBasinsRequest(String prefix, String startAfter, Optional<Integer> limit) {
    this.prefix = prefix;
    this.startAfter = startAfter;
    this.limit = limit;
  }

  public static ListBasinsRequestBuilder newBuilder() {
    return new ListBasinsRequestBuilder();
  }

  public s2.v1alpha.ListBasinsRequest toProto() {
    var builder =
        s2.v1alpha.ListBasinsRequest.newBuilder().setPrefix(prefix).setStartAfter(startAfter);
    this.limit.ifPresent(builder::setLimit);
    return builder.build();
  }

  public static class ListBasinsRequestBuilder {
    private String prefix = "";
    private String startAfter = "";
    private Optional<Integer> limit = Optional.empty();

    public ListBasinsRequestBuilder withPrefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public ListBasinsRequestBuilder withStartAfter(String startAfter) {
      this.startAfter = startAfter;
      return this;
    }

    public ListBasinsRequestBuilder withLimit(Integer limit) {
      this.limit = Optional.of(limit);
      return this;
    }

    public ListBasinsRequest build() {
      this.limit.ifPresent(
          lim -> {
            if (lim < 0) {
              throw new IllegalArgumentException("Limit must be a positive integer");
            }
            if (lim > 1000) {
              throw new IllegalArgumentException("Limit must be less than 1000");
            }
          });
      return new ListBasinsRequest(prefix, startAfter, limit);
    }
  }
}
